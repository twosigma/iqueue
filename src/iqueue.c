/*	$TwoSigma: iqueue.c,v 1.30 2012/01/05 21:45:21 thudson Exp $	*/

/*
 *	Copyright (c) 2010 Two Sigma Investments, LLC
 *	All Rights Reserved
 *
 *	THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
 *      Two Sigma Investments, LLC.
 *
 *	The copyright notice above does not evidence any
 *	actual or intended publication of such source code.
 */

#include "twosigma.h"
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <fcntl.h>
#include <err.h>
#include <byteswap.h>
#include <xmmintrin.h>
#include <semaphore.h>
#include <pthread.h>
#include "tsutil.h"
#include "tsclock.h"
#include "iqueue.h"
#include "iqsync.h"
#include "atomic.h"
#include "tslock.h"
#include "shash.h"

__RCSID("$TwoSigma: iqueue.c,v 1.30 2012/01/05 21:45:21 thudson Exp $");


#define IQUEUE_INDEX_MAGIC	((uint32_t) 0xDADA1234)
#define IQUEUE_VERSION		((uint32_t) 0x00003000)
#define IQUEUE_BLOCK_MAGIC	((uint64_t) 0x6971626c6f636b00)
#define IQUEUE_MAX_HDR_LEN	((size_t) 4096)

#define IQUEUE_TABLE_SHIFT	20
#define IQUEUE_TABLE_MASK	((1 << IQUEUE_TABLE_SHIFT) - 1)
#define IQUEUE_TABLE_SIZE	(1 << 20)

#define IQUEUE_WRITER_TABLES	4

#define IQUEUE_BLOCK_COUNT	(1024)


typedef struct
{
    const uint32_t magic;
    const uint32_t version;
    const uint64_t creation_time;
    volatile uint64_t flock_time; // for growing the file

    // pointers to/size of the writer tables.  Must be at least
    // 8-byte aligned to ensure that it does not cross a cache line.
    const iqueue_msg_t writer_tables[IQUEUE_WRITER_TABLES]
	__attribute__((aligned(8)));

    volatile iqueue_id_t index_tail __attribute__((aligned(64)));
    volatile uint64_t data_tail __attribute__((aligned(64)));

    const uint64_t hdr_len;
    uint8_t hdr[IQUEUE_MAX_HDR_LEN];

    volatile uint64_t tables[] __attribute__((aligned(4096)));
} __attribute__((packed))
iqueue_index_t;

// Ensure that the writer table has not pushed the index to the wrong
// location.
static_assert(offsetof(iqueue_index_t, index_tail) == 64, "offset error");


typedef struct
{
    const uint64_t magic;
    const uint64_t offset;
    const uint64_t creation_time;
    uint64_t reserved;
} __attribute__((packed))
iqueue_block_t;


struct _iqueue
{
    iqueue_index_t * idx; // Typically data[0]
    int fd;
    const char * const name;

    // Cache the last index used so that the tables do not need to
    // be consulted each time.
    // Value is 20 bits of table ID and 44 bits of offset
    volatile uint64_t table_cache;

    // Flags used for mapping new blocks
    int open_flags;
    int open_mode;
    int mmap_prot;
    int mmap_flags;
    int mlock_flag;

    // Last size that we grew the file to
    uint64_t last_grow_size;

    // Set once we have warned on an attempt to allocate
    int warned_readonly_allocate;

    // iqueue writer tables
    shash_t * writer_tables[IQUEUE_WRITER_TABLES];

    // Prefetch thread
    pthread_t prefetch_thread;
    pthread_t syncbehind_thread;
    sem_t prefetch_sem;

    uint8_t * volatile blocks[IQUEUE_BLOCK_COUNT];
};



/** Attempt to lock the iqx file.
 * This needs to deal with several problems in the flock(2) interface:
 * 1. The locks are recursive
 * 2. The locks are per-fd
 * 3. The locks are shared across forks
 *
 * \return -1 on error, 0 on no lock (but progress) and the lock time on a successful lock.
 */
static uint64_t
_iqueue_flock(
    iqueue_t * const iq
)
{
    if (iq->last_grow_size == 0)
	return 1;

    iqueue_index_t * const idx = iq->idx;

    // flock the file, which might succeed if we share the fd with
    // the process that has it locked, in which case we can then check
    // the flock time field.
    if (flock(iq->fd, LOCK_EX) < 0)
	return -1;

    // Check to see the file lock time
    uint64_t now = tsclock_getnanos(0);
    uint64_t old_flock_time = atomic_cas_64(&idx->flock_time, 0, now);

    // If there was no file lock time set, and we wrote our time to it,
    // then we have the locks and are ready to proceed.
    if (old_flock_time == 0)
	return now;

    TSLOGX(TSINFO, "%s: lock held since %"PRIu64" (now=%"PRIu64")",
	iq->name,
	old_flock_time,
	now
    );

    // Spin for up to 1 msec or until the flock_time changes.
    const uint64_t flock_timeout = 1000000;
    while (now < old_flock_time + flock_timeout)
    {
	// If it changes, that means that another thread in our process
	// has finished its business and the lock conditions should be
	// rechecked to see if it even matters any more.
	if (idx->flock_time != old_flock_time)
	{
	    flock(iq->fd, LOCK_UN);
	    TSLOGX(TSDEBUG, "%s: Lock is available again", iq->name);
	    return 0;
	}

	usleep(10);
	now = tsclock_getnanos(0);
    }

    // Someone else has held the lock for more than our timeout,
    // which means they are likely dead.  Steal the lock from
    // them if we can.
    if (atomic_cas_bool_64(&idx->flock_time, old_flock_time, now))
    {
	TSLOGX(TSWARN, "%s: Stole lock after timeout", iq->name);
	return now;
    }

    flock(iq->fd, LOCK_UN);
    TSLOGX(TSDEBUG, "%s: Lock is available again", iq->name);
    return 0;
}


static void
_iqueue_funlock(
    iqueue_t * const iq,
    const uint64_t lock_time
)
{
    if (iq->last_grow_size == 0)
	return;

    if (!atomic_cas_bool_64(&iq->idx->flock_time, lock_time, 0))
	TSLOGX(TSWARN, "%s: Lock was stolen from us!  Danger!", iq->name);

    flock(iq->fd, LOCK_UN);
}



/** Grow the data file to be at least as large as the new_size.
 *
 * We can't just do an ftruncate() to the new size since there may
 * be multiple processes writing to the file and that could lead
 * to some nasty races.
 *
 * \return -1 on error, 0 on success.
 */
static int
iqueue_grow_file(
    iqueue_t * const iq,
    const uint64_t new_size
)
{
    struct stat sb;
retry:
    if (fstat(iq->fd, &sb) < 0)
	goto fail;

    // Once we (or someone) have been successful in growing the file,
    // we're done and can return.
    if (sb.st_size >= (off_t) new_size)
    {
	TSLOGX(TSDEBUG, "%s: File is already %"PRIu64" bytes >= %"PRIu64,
	    iq->name,
	    (uint64_t) sb.st_size,
	    new_size
	);
	return 0;
    }

    // For the first block, we can't lock since the file doesn't exist.
    const uint64_t flock_time = _iqueue_flock(iq);
    if (flock_time == (uint64_t) -1)
	goto fail;
    if (flock_time == 0)
	goto retry;

    // Double check the file size, just in case
    // in between us checking the size and then getting the lock,
    // someone else has grown the file.
    if (fstat(iq->fd, &sb) < 0)
    {
	_iqueue_funlock(iq, flock_time);
	goto fail;
    }

    if (sb.st_size >= (off_t) new_size)
    {
	_iqueue_funlock(iq, flock_time);
	TSLOGX(TSDEBUG, "%s: Someone else grew the file to %"PRIu64,
	    iq->name,
	    new_size
	);
	return 0;
    }


    TSLOGX(TSINFO, "%s: Growing from 0x%"PRIx64" to 0x%"PRIx64" bytes",
	iq->name,
	(uint64_t) sb.st_size,
	new_size
    );

    if (ftruncate(iq->fd, new_size) < 0)
    {
	_iqueue_funlock(iq, flock_time);
	goto fail;
    }

    _iqueue_funlock(iq, flock_time);
    iq->last_grow_size = new_size; // possible race, but doesn't matter
    return 0;

fail:
    TSLOG(TSERROR, "%s: Failed to grow to %"PRIu64" bytes",
	iq->name,
	new_size
    );

    return -1;
}


/** Call mlock() on a single block */
static int
iqueue_mlock_block(
    iqueue_t * const iq,
    const uint64_t block_id
)
{
    if (block_id >= IQUEUE_BLOCK_COUNT)
    {
	TSLOGX(TSWARN, "%s: Block %"PRIu64" out of range", iq->name, block_id);
	return -1;
    }

    void * const block = iq->blocks[block_id];
    if (!block)
	return 0;

    if (mlock(block, IQUEUE_BLOCK_SIZE) == 0)
	return 0;

    TSLOG(TSWARN, "%s: Unable to mlock(block[%"PRIu64"]=%p,0x%"PRIx64")",
	iq->name,
	block_id,
	block,
	IQUEUE_BLOCK_SIZE
    );

    iq->mlock_flag = 0;

    return -1;
}



/** Sync a single block */
static int
iqueue_fsync_block(
    iqueue_t * const iq,
    const uint64_t block_id
)
{
    if (block_id >= IQUEUE_BLOCK_COUNT)
    {
	TSLOGX(TSWARN, "%s: Block %"PRIu64" out of range", iq->name, block_id);
	return -1;
    }

    void * const block = iq->blocks[block_id];
    if (!block)
	return -1;

    const uint64_t block_offset = block_id << IQUEUE_BLOCK_SHIFT;

    // First sync contents of block to ensure any dirty pages in our mapping
    // are saved back to the file
    if (sync_file_range(
	    iq->fd,
	    block_offset,
	    IQUEUE_BLOCK_SIZE,
	    SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE) != 0) {
	TSLOG(TSWARN, "%s: Unable to fsync(block[%"PRIu64"]=%p,0x%"PRIx64")",
	    iq->name,
	    block_id,
	    block,
	    IQUEUE_BLOCK_SIZE
	);
	return -1;
    }

    // Now free the VMAs so Linux will reclaim teh pages (unless we access
    // it again)
    if (madvise(block, IQUEUE_BLOCK_SIZE, MADV_DONTNEED) != 0) {
	TSLOG(TSWARN, "%s: Unable to madvise(block[%"PRIu64"]=%p,0x%"PRIx64
		", MADV_DONTNEED)",
	    iq->name,
	    block_id,
	    block,
	    IQUEUE_BLOCK_SIZE
	);
	return -1;
    }

    return 0;
}




/** Map a block into memory if it is not already mapped.
 *
 * \note Will grow the file if necessary.
 * \return NULL if there was a failure to grow the file or map the block.
 */
static void *
_iqueue_map_block(
    iqueue_t * const iq,
    const uint64_t block_id
)
{
    // If we do not have a file, we can not do any mappings
    if (iq->fd < 0)
	return NULL;

    // Make sure the file is at least as large as this block size
    const uint64_t min_size = (block_id + 2) << IQUEUE_BLOCK_SHIFT;
    if (iqueue_grow_file(iq, min_size) < 0)
	return NULL;

    // Quick check to see if any one else has already done so
    uint8_t * block = iq->blocks[block_id];
    if (block)
    {
	TSLOGX(TSDEBUG, "%s: block %"PRIu64" already mapped to %p", iq->name, block_id, block);
	return block;
    }

    // Attempt to map it
    const uint64_t block_offset = block_id << IQUEUE_BLOCK_SHIFT;
    uint64_t map_time = -tsclock_getnanos(0);

    block = mmap(
	NULL,
	IQUEUE_BLOCK_SIZE,
	iq->mmap_prot,
	iq->mmap_flags,
	iq->fd,
	block_offset
    );
    if (block == MAP_FAILED)
    {
	TSLOGX(TSERROR, "%s: Failed to map offset %"PRIu64, iq->name, block_offset);
	return NULL;
    }

    map_time += tsclock_getnanos(0);

    // Attempt to write the mapping into the local block table
    if (!atomic_cas_bool_ptr((volatile void*) &iq->blocks[block_id], NULL, block))
    {
	// We lost!  Someone else beat us to it.  Deallocate our block
	// and use theirs instead.  Sucks to be us.
	TSLOGX(TSDEBUG,
	    "%s: Lost race.  Unmapping %"PRIx64" from %p",
	    iq->name,
	    block_offset,
	    block
	);

	munmap(block, IQUEUE_BLOCK_SIZE);
	return iq->blocks[block_id];
    }

    TSLOGX(TSINFO, "%s: Mapped 0x%"PRIx64" to %p in %"PRIu64" ns",
	iq->name,
	block_offset,
	block,
	map_time
    );

    if (iq->mlock_flag)
	iqueue_mlock_block(iq, block_id);

    posix_madvise(block, IQUEUE_BLOCK_SIZE, POSIX_MADV_SEQUENTIAL);

    return block;
}


/** Unmap any mapped blocks, close the file descriptors and generally
 * shutdown the whole kit-n-kaboodle.
 *
 * This does not free the iqueue structure since it might be reused
 * by iqueue_reopen().
 *
 * Any prefetch thread will be shutdown; should it be paused and
 * restarted instead?
 */
static void
_iqueue_unmap(
    iqueue_t * const iq
)
{
    if (iq->prefetch_thread)
    {
	TSLOGX(TSINFO, "%s: Shutdown prefetch thread", iq->name);
	pthread_cancel(iq->prefetch_thread);
	pthread_join(iq->prefetch_thread, NULL);
	iq->prefetch_thread = 0;
    }

    if (iq->syncbehind_thread)
    {
	TSLOGX(TSINFO, "%s: Shutdown syncbehind thread", iq->name);
	pthread_cancel(iq->syncbehind_thread);
	pthread_join(iq->syncbehind_thread, NULL);
	iq->syncbehind_thread = 0;
    }

    if (iq->fd >= 0)
    {
	close(iq->fd);
	iq->fd = -1;
    }

    // Don't unmap idx if it was shared with iq->blocks[0]
    if ((void*) iq->idx != iq->blocks[0] && iq->idx)
    {
	TSLOGX(TSDEBUG, "%s: Unmapping idx %p", iq->name, iq->idx);
	munmap(iq->idx, IQUEUE_BLOCK_SIZE);
    }

    iq->idx = NULL;

    for (unsigned i = 0 ; i < IQUEUE_BLOCK_COUNT ; i++)
    {
	if (!iq->blocks[i])
	    continue;

	TSLOGX(TSDEBUG, "%s: Unmapping block %u: %p", iq->name, i, iq->blocks[i]);
	munmap(iq->blocks[i], IQUEUE_BLOCK_SIZE);
	iq->blocks[i] = NULL;
    }

    // Trash the cached table lookup so that we won't use it
    iq->table_cache = -1;
}



/** Attempt to re-open an iqueue after the on-disk file has changed.
 */
static int
_iqueue_reopen(
    iqueue_t * const iq,
    const int create_flag
)
{
    int serrno;

    if (iq->fd >= 0)
	_iqueue_unmap(iq);

    const int open_flags = iq->open_flags | (create_flag ? O_CREAT : 0);
    const char * const open_str =
	open_flags == O_RDONLY ? "readonly" :
	open_flags == O_RDWR ? "readwrite" :
	"create";

    iq->fd = open(
	iq->name,
	open_flags,
	iq->open_mode
    );
    if (iq->fd < 0)
    {
	if (errno == ENOENT)
	{
	    TSLOGX(TSWARN, "%s: No such file or directory", iq->name);
	    sleep(1); // force a short wait
	    errno = ENOENT;
	    return -1;
	}

	TSLOGX(TSERROR, "%s: Unable to open %s", iq->name, open_str);
	goto fail;
    }

    // If we are creating the iqueue for the first time we are allowed
    // to map using _iqueue_map_block() since will be overwriting our
    // temporary file.
    if (create_flag)
    {
	iq->idx = _iqueue_map_block(iq, 0);
	if (iq->idx == NULL)
	    goto fail;
	return 0;
    }

    // We can't use _iqueue_map_block() since that might modify an existing
    // file.  Instead we have to just map a minimal segment at first
    void * block = mmap(
	NULL,
	IQUEUE_BLOCK_SIZE,
	iq->mmap_prot,
	iq->mmap_flags,
	iq->fd,
	0
    );
    if (block == MAP_FAILED)
    {
	TSLOG(TSERROR, "%s: Failed to map index header", iq->name);
	goto fail;
    }

    iq->idx = block;

    // Make sure that the file is at least large enough for our header
    struct stat sb;
    if (fstat(iq->fd, &sb) < 0)
    {
	TSLOG(TSERROR, "%s: Failed to stat", iq->name);
	goto fail;
    }

    const uint64_t file_size = sb.st_size;

    if ((size_t) file_size < sizeof(*iq->idx))
    {
	TSLOGX(TSWARN, "%s: File is much too small.  Not an iqx?", iq->name);
	goto fail;
    }


    // Verify the version and magic of the existing iqueue file
    if (iq->idx->magic != IQUEUE_INDEX_MAGIC
    ||  iq->idx->version != IQUEUE_VERSION)
    {
	TSLOGX(TSERROR,
	    "%s: Magic %"PRIx32".%"PRIx32" != expected %"PRIx32".%"PRIx32,
	    iq->name,
	    iq->idx->magic,
	    iq->idx->version,
	    IQUEUE_INDEX_MAGIC,
	    IQUEUE_VERSION
	);
	goto fail;
    }

    // Everything looks ok so far.
    TSLOGX(TSDEBUG,
	"%s: %s: creation %"PRIu64", entries %"PRIu64", data %"PRIu64", size %"PRIu64" %s",
	iq->name,
	open_str,
	iq->idx->creation_time,
	iq->idx->index_tail,
	iq->idx->data_tail,
	file_size,
	iqueue_is_sealed(iq) ? ", sealed" : ""
    );

    iq->last_grow_size = file_size;

    return 0;

fail:
    /* Save the errno so that the caller knows why we failed */
    serrno = errno;
    _iqueue_unmap(iq);
    errno = serrno;
    return -1;
}




/** Build the minimal memory representation of an iqueue and map
 * the first block.
 *
 * \note index_filename will be cached in the object and freed
 * when iqueue_close() is called.
 */
static iqueue_t *
_iqueue_init(
    const char * const filename,
    int create_flag // -1 == read-only, 0 == read-write, 1 == create
)
{
    iqueue_t * const iq = calloc(1, sizeof(*iq));
    if (!iq)
	goto fail_iqueue_malloc;

    int open_flags = O_RDONLY;
    int open_mode = 0444;
    int mmap_prot = PROT_READ;
    int mmap_flags = MAP_SHARED;

    if (create_flag >= 0)
    {
	open_flags = O_RDWR;
	open_mode |= 0222;
	mmap_prot |= PROT_WRITE;
    }

    memcpy(iq, &(iqueue_t) {
	.fd		= -1,
	.table_cache	= -1,
	.name		= filename,
	.open_flags	= open_flags,
	.open_mode	= open_mode,
	.mmap_prot	= mmap_prot,
	.mmap_flags	= mmap_flags,
    }, sizeof(*iq));

    sem_init(&iq->prefetch_sem, 0, 0);

    if (_iqueue_reopen(iq, create_flag == 1) < 0)
	goto fail_reopen;

    return iq;

fail_reopen:
    free(iq);
fail_iqueue_malloc:
    return NULL;
}



iqueue_t *
iqueue_open(
    const char * index_filename,
    bool writeable
)
{
    char * const filename = strdup(index_filename);
    if (!filename)
	return NULL;

    iqueue_t * const iq = _iqueue_init(filename, writeable ? 0 : -1);
    if (!iq)
	return NULL;

    if (writeable)
	iqueue_prefetch(iq, 0, 16 << 20);

    return iq;
}


iqueue_t *
iqueue_create(
    const char * index_filename,
    uint64_t creation,
    const void * const hdr,
    size_t hdr_len
)
{
    if (creation == 0)
	creation = tsclock_getnanos(0);

    if (hdr_len > IQUEUE_MAX_HDR_LEN)
    {
	TSLOGX(TSERROR, "%s: Header len %zu > max %zu",
	    index_filename,
	    hdr_len,
	    IQUEUE_MAX_HDR_LEN
	);

	return NULL;
    }

    const int namelen = strlen(index_filename);
    char * filename = calloc(1, namelen + 32);
    if (!filename)
	goto fail_filename_alloc;
    snprintf(filename, namelen+32, "%s.%"PRIx64, index_filename, creation);

    iqueue_t * const iq = _iqueue_init(filename, 1);
    if (!iq)
	goto fail_iq_alloc;

    // Fill in the required fields and user header
    memcpy(iq->idx, &(iqueue_index_t) {
	.magic		= IQUEUE_INDEX_MAGIC,
	.version	= IQUEUE_VERSION,
	.creation_time  = creation,
	.hdr_len	= hdr_len,
	.index_tail	= 0,
	.data_tail	= sizeof(*iq->idx)
			+ IQUEUE_TABLE_SIZE * sizeof(*iq->idx->tables),
    }, sizeof(*iq->idx));

    memcpy(iq->idx->hdr, hdr, hdr_len);

    // The file is fully built on disk.  Attempt to atomically swap it for
    // the real one.
    if (link(filename, index_filename) == -1)
    {
	if (errno != EEXIST)
	{
	    TSLOG(TSERROR, "%s: Unable to link from %s", index_filename, filename);
	    goto fail_link;
	}

	// Remove our temp file, and trailing creation time
	unlink(filename);
	filename[namelen] = '\0';

	// Clean up and try to open it as a normal
	// iqueue.  The caller will know that they lost the race since
	// the creation time will not be the same as the one they specified
	// \note: Do not goto the failure path since we do not want to unlink
	// the actual iqueue file.
	TSLOGX(TSINFO, "%s: Lost creation race.  Retrying", index_filename);
	if (_iqueue_reopen(iq, 0) < 0)
	{
	    iqueue_close(iq);
	    return NULL;
	}

	return iq;
    }

    // We won the race.  Unlink our temp file, update our name and keep going
    unlink(filename);
    filename[namelen] = '\0';

    // We know that we will be writing into it, so prefetch the index block and
    // some of the first data.
    iqueue_prefetch(iq, 0, 16 << 20);

    return iq;

fail_link:
    unlink(filename);
    _iqueue_unmap(iq);
    free(iq);
fail_iq_alloc:
    free(filename);
fail_filename_alloc:
    return NULL;
}



int
iqueue_reopen(
    iqueue_t * const iq
)
{
    return _iqueue_reopen(iq, false);
}


void
iqueue_close(
    iqueue_t * const iq
)
{
    _iqueue_unmap(iq);
    free((void*)(uintptr_t) iq->name);
    free(iq);
}


int
iqueue_archive(
    iqueue_t * const iq,
    iqueue_id_t seal_id
)
{

    // Attempt to seal the iqueue at this id if one is provided
    if (seal_id != IQUEUE_MSG_BAD_ID)
    {
	int rc = iqueue_try_seal(iq, seal_id);
	if (rc != 0)
	{
	    TSLOGX(TSDEBUG, "%s: Failed to seal at id %"PRIu64": rc=%d",
		iq->name,
		seal_id,
		rc
	    );
	    return rc;
	}
    }

    // We have successfully sealed the iqueue (or are not doing so)
    const char * const old_name = iqueue_name(iq);
    const size_t namelen = strlen(old_name) + 32;
    char * new_name = calloc(1, namelen);
    if (!new_name)
	return -1;

    snprintf(new_name, namelen,
	"%s.%"PRIu64,
	old_name,
	iqueue_creation(iq)
    );

    if (!iqueue_is_sealed(iq))
	TSLOGX(TSWARN, "%s: Archiving an unsealed iqueue", old_name);

    if (link(old_name, new_name) == -1)
    {
	TSLOG(TSERROR, "%s: Unable to create link to archive %s",
	    old_name,
	    new_name
	);
	return -1;
    }

    if (unlink(old_name) == -1)
    {
	TSLOG(TSERROR, "%s: Unable to unlink", old_name);
	unlink(new_name);
	return -1;
    }

    TSLOGX(TSDEBUG, "%s: Archived to %s", iq->name, new_name);

    return 0;
}



uint64_t
iqueue_entries(
    const iqueue_t * const iq
)
{
    uint64_t tail = iq->idx->index_tail;

    while (1)
    {
	switch (iqueue_status(iq, tail))
	{
	case IQUEUE_STATUS_HAS_DATA:
	    // There are still entries out there.  Try the next one
	    tail++;
	    continue;

	case IQUEUE_STATUS_NO_DATA:
	    // We have found the end.
	    return tail;

	case IQUEUE_STATUS_SEALED:
	    // We are one past the actual end of data
	    return tail - 1;

	default:
	    // Something is very wrong
	    return (uint64_t) -1;
	}
    }
}


uint64_t
iqueue_data_len(
    const iqueue_t * const iq
)
{
    return iq->idx->data_tail;
}


uint64_t
iqueue_creation(
    const iqueue_t * const iq
)
{
    return iq->idx->creation_time;
}


const char *
iqueue_name(
    const iqueue_t * const iq
)
{
    return iq->name;
}


void *
iqueue_header(
    iqueue_t * const iq,
    size_t * const len_out
)
{
    *len_out = iq->idx->hdr_len;
    return iq->idx->hdr;
}



int
iqueue_mlock(
    iqueue_t * const iq
)
{
    iq->mlock_flag = 1;

    for (unsigned block_id = 0 ; block_id < IQUEUE_BLOCK_COUNT ; block_id++)
    {
	if (iqueue_mlock_block(iq, block_id) == -1)
	    return -1;
    }

    return 0;
}




/** Retrieve the host memory pointer to an offset.
 * Optionally do the mmap() if the block is not already mapped.
 */
const void *
iqueue_get_data(
    iqueue_t * const iq,
    uint64_t offset,
    const int do_map
)
{
    const uint64_t block_id = offset >> IQUEUE_BLOCK_SHIFT;
    offset &= IQUEUE_BLOCK_MASK;

    if (unlikely(block_id >= IQUEUE_BLOCK_COUNT))
	return NULL;

    uint8_t * block = iq->blocks[block_id];
    if (unlikely(block == NULL))
    {
	if (!do_map)
	    return NULL;

	// They want it mapped.
	block = _iqueue_map_block(iq, block_id);
	if (!block)
	    return NULL;
    }

    return block + offset;
}


void *
iqueue_allocate_raw(
    iqueue_t * const iq,
    size_t len,
    iqueue_msg_t * const offset_out
)
{
    if (unlikely(!offset_out || len >= IQUEUE_BLOCK_SIZE))
	return NULL;
    if (unlikely((iq->mmap_prot & PROT_WRITE) == 0))
    {
	if (!iq->warned_readonly_allocate)
	    TSLOGX(TSWARN, "%s: Attempt to allocate from read-only iqueue", iq->name);
	iq->warned_readonly_allocate = 1;
	return NULL;
    }

    iqueue_index_t * const idx = iq->idx;
    uint64_t offset;

    while (1)
    {
	uint64_t tail = offset = idx->data_tail;
	uint64_t new_tail = tail + len;

	// Check to see if this would cross a 1 GB boundary and adjust
	// the allocation upwards to avoid the boundary.  We also
	// must avoid the first 64 bytes of the new block to avoid
	// the markers.
	if ((tail >> IQUEUE_BLOCK_SHIFT) != (new_tail >> IQUEUE_BLOCK_SHIFT))
	{
	    offset = ((tail >> IQUEUE_BLOCK_SHIFT) + 1) << IQUEUE_BLOCK_SHIFT;
	    offset += sizeof(iqueue_block_t);
	    new_tail = offset + len;
	}

	if (atomic_cas_bool_64(&idx->data_tail, tail, new_tail))
	    break;
    }

    // We have updated the idx->data_tail and can start to fill
    // in the data into the buffer.  It will not be valid data
    // until iqueue_update() is called on the data pointer.
    void * const data = (void*)(uintptr_t) iqueue_get_data(iq, offset, 1);
    if (!data)
	return NULL;

    *offset_out = iqueue_msg(offset, len);
    return data;
}


/** Perform the atomic update of the index slot to point to the
 * new message.
 *
 * If the write is successful, the tail pointer will also be updated
 * if the value being written is not a sealing tag.
 *
 * \return 0 if unsuccessful, 1 if succesfully written.
 */
static inline int
iqueue_cas(
    iqueue_index_t * const idx,
    iqueue_msg_t * const slot,
    const iqueue_id_t id,
    const iqueue_msg_t new_msg
)
{
    // If the value is still 0, write our msg offset into it
    // If we fail, return immediately.
    if (unlikely(!atomic_cas_bool_64(
	(uint64_t*)(uintptr_t)&slot->v,
	0,
	new_msg.v
    )))
	return 0;

    // Do not advance the tail for seal messages, so reader can see them
    if (unlikely(new_msg.v == IQUEUE_MSG_SEALED))
	return 1;

    // We have written our offset into slot id, try to advance
    // the tail to one past where we are, but do not care if
    // we fail.  That means that someone else has already advanced
    // the idx for us.
    const uint64_t current_tail = idx->index_tail;
    if (current_tail >= id + 1)
	return 1;

    atomic_cas_bool_64(&idx->index_tail, current_tail, id + 1);

    return 1;
}



/** Create a new table and install it in the desired location */
static uint64_t
iqueue_new_table(
    iqueue_t * const iq,
    uint64_t table_num
)
{
    iqueue_index_t * const idx = iq->idx;
    iqueue_msg_t msg;
    const void * const table_buf = iqueue_allocate_raw(
	iq,
	IQUEUE_TABLE_SIZE * sizeof(*idx->tables) + 32,
	&msg
    );
    if (!table_buf)
	return 0;

    uint64_t offset = iqueue_msg_offset(msg);

    // force alignment up to a cache line
    offset = (offset + 31) & ~31;

    // Attempt to store the new table into the correct slot
    if (atomic_cas_bool_64(&idx->tables[table_num], 0, offset))
    {
	TSLOGX(TSDIAG, "%s: New tables[%"PRIu64"] = %"PRIx64,
	    iq->name,
	    table_num,
	    offset
	);
	return offset;
    }

    // We lost the race, but now have a huge allocation.  Try
    // to make things better by storing it in the next slot
    const uint64_t correct_offset = idx->tables[table_num];

    while (++table_num < IQUEUE_TABLE_SIZE)
    {
	if (atomic_cas_bool_64(&idx->tables[table_num], 0, offset))
	    return correct_offset;
    }

    // We've fully populated the tables?  This really shouldn't happen,
    // but we'll just leak the allocation that we did.
    return correct_offset;
}



/** Given an index, lookup the pointer to the slot in the correct table.
 * Optionally create the intermediate table if necessary.
 */
static iqueue_msg_t *
iqueue_get_slot(
    iqueue_t * const iq,
    const uint64_t id,
    const int create
)
{
    const uint64_t last_table = iq->table_cache;
    const uint64_t table_num = id >> IQUEUE_TABLE_SHIFT;
    const uint64_t offset = id & IQUEUE_TABLE_MASK;
    uint64_t table_offset;

    if (likely((last_table & IQUEUE_TABLE_MASK) == table_num))
    {
	// Hurrah! We hit our cached value
	table_offset = last_table >> IQUEUE_TABLE_SHIFT;
    } else {
	// Not the cached value; do a full lookup
	if (unlikely(table_num > IQUEUE_TABLE_SIZE))
	    return NULL;

	table_offset = iq->idx->tables[table_num];
	if (unlikely(!table_offset))
	{
	    // There is no table for this id yet, create it if requested
	    if (!create)
		return NULL;

	    table_offset = iqueue_new_table(iq, table_num);
	    if (!table_offset)
		return NULL;
	}

	// We have a pointer to the table; cache the value and return the table
	iq->table_cache = (table_offset << IQUEUE_TABLE_SHIFT) | table_num;
    }

    // We have a table offset now; find the actual block that goes with it
    iqueue_msg_t * const table = (void*)(uintptr_t) iqueue_get_data(iq, table_offset, 1);
    if (!table)
	return NULL;
    return &table[offset];
}



static inline int
iqueue_try_update_internal(
    iqueue_t * iq,
    iqueue_id_t id,
    iqueue_msg_t new_msg
)
{
    iqueue_index_t * const idx = iq->idx;
    const iqueue_id_t tail = iq->idx->index_tail;

    if (unlikely(tail != id))
    {
	// If the id they are trying to write to is less than the
	// current tail, it is guaranteed to fail since there is already
	// something written there.
	if (id < tail)
	    return IQUEUE_STATUS_HAS_DATA;

	// If the id they are trying to write to is not at the tail
	// position, then it would leave a hole in the index.  This is
	// not allowed, so the index might be invalid.  To confirm,
	// check to see if the actual entries value is wrong.
	if (id > iqueue_entries(iq))
	    return IQUEUE_STATUS_INDEX_INVALID;

	// It was a spurious case of the tail being wrong; allow the
	// iqueue_try_update() to proceed.
    }

    iqueue_msg_t * const slot = iqueue_get_slot(iq, id, 1);
    if (unlikely(!slot))
	return IQUEUE_STATUS_INDEX_INVALID;

    if (likely(iqueue_cas(idx, slot, id, new_msg)))
	return 0;

    // We lost.  Check for the possibility that the iqueue has been sealed
    if (unlikely(slot->v == IQUEUE_MSG_SEALED))
	return IQUEUE_STATUS_SEALED;

    return IQUEUE_STATUS_HAS_DATA;
}


static inline int
iqueue_update_internal(
    iqueue_t * iq,
    iqueue_msg_t new_msg,
    iqueue_id_t * id_out,
    int is_id_be
)
{
    iqueue_index_t * const idx = iq->idx;

    // Find the next available slot
    while (1)
    {
	const iqueue_id_t id = idx->index_tail;
	iqueue_msg_t * const slot = iqueue_get_slot(iq, id, 1);
	if (unlikely(!slot))
	    return IQUEUE_STATUS_INDEX_INVALID;

	if (unlikely(slot->v == IQUEUE_MSG_SEALED))
	    return IQUEUE_STATUS_SEALED;

	if (unlikely(slot->v))
	{
	    // The list is in an inconsistent state; try to advance
	    // the tail pointer.
	    atomic_cas_bool_64(&idx->index_tail, id, id+1);
	    continue;
	}

	// Write to user slot before attempting the CAS to preserve
	// all lockless guarantees.
	if (id_out != NULL)
	    *id_out = (is_id_be ? htobe64(id) : id);

	if (likely(iqueue_cas(idx, slot, id, new_msg)))
	    return 0;
    }
}


int
iqueue_update(
    iqueue_t * const iq,
    iqueue_msg_t new_msg,
    iqueue_id_t * const id_out
)
{
    return iqueue_update_internal(iq, new_msg, id_out, 0);
}


int
iqueue_update_be(
    iqueue_t * const iq,
    iqueue_msg_t new_msg,
    iqueue_id_t * const id_be_out
)
{
    return iqueue_update_internal(iq, new_msg, id_be_out, 1);
}


/** Attempt to store a message in the log at the desired slot.
 * entry should point to the buffer returned from iqueue_allocate()
 * once all of the log data has been copied into it.
 *
 * \return same as iqueue_update with additional error:
 *         EAGAIN: Specified slot has been filled
 */
int
iqueue_try_update(
    iqueue_t * const iq,
    iqueue_id_t id,
    iqueue_msg_t new_msg
)
{
    return iqueue_try_update_internal(iq, id, new_msg);
}


/** Seals the iqueue, blocking any further write attemts
 *
 * \return same as iqueue_update
 * */
int
iqueue_seal(
    iqueue_t * iq
)
{
    const iqueue_msg_t new_msg = {
	.v = IQUEUE_MSG_SEALED
    };

    return iqueue_update_internal(iq, new_msg, NULL, 0);
}


int
iqueue_try_seal(
    iqueue_t * iq,
    iqueue_id_t id
)
{
    const iqueue_msg_t new_msg = {
	.v = IQUEUE_MSG_SEALED
    };

    return iqueue_try_update_internal(iq, id, new_msg);
}


iqueue_id_t
iqueue_begin(
    const iqueue_t * const iq
)
{
    __USE(iq);
    return 0;
}


iqueue_id_t
iqueue_end(
    const iqueue_t * iq
)
{
    return iq->idx->index_tail;
}


int
iqueue_status(
    const iqueue_t * const iq,
    iqueue_id_t id
)
{
    iqueue_msg_t * const slot
	= iqueue_get_slot(__UNCONST_T(iqueue_t*, iq), id, 0);

    if (!slot || !slot->v)
	return IQUEUE_STATUS_NO_DATA;

    if (slot->v == IQUEUE_MSG_SEALED)
	return IQUEUE_STATUS_SEALED;

    return IQUEUE_STATUS_HAS_DATA;
}


int
iqueue_status_wait(
    iqueue_t * iq,
    iqueue_id_t id,
    int64_t timeout_ns
)
{
    // Retrieve the map-space pointer
    volatile iqueue_msg_t * msg = NULL;
    tsclock_nanos_t start_time = 0;

    while (1)
    {
	if (!msg)
	{
	    // Try to get the slot, but do not modify the tables.
	    // If we are blocking forever, keep trying
	    msg = iqueue_get_slot(iq, id, 0);
	    if (!msg && timeout_ns >= 0 && timeout_ns < 10)
		    return IQUEUE_STATUS_NO_DATA;
	}

	if (msg)
	{
	    // We have the slot; try
	    // Try a few times before checking the clock
	    for (int i = 0 ; i < 1000 ; i++)
	    {
		if (unlikely(!msg->v)) {
		    if (timeout_ns == 0)
			return IQUEUE_STATUS_NO_DATA;

		    _mm_pause();
		    continue;
		}

		// Check if the iqueue is sealed at this index
		if (unlikely(msg->v == IQUEUE_MSG_SEALED))
		    return IQUEUE_STATUS_SEALED;

		// Build a user-space pointer from the pointer
		return IQUEUE_STATUS_HAS_DATA;
	    }
	}

	// timeout == -1 means loop forever
	if (timeout_ns == -1)
	    continue;

	// timeout < 10 means just check the queue a few times
	if (timeout_ns < 10)
	    break;

	if (!start_time)
	    start_time = tsclock_getnanos(0);

	if (start_time + timeout_ns < tsclock_getnanos(0))
	    break;
    }

    return IQUEUE_STATUS_NO_DATA;
}



uint64_t
iqueue_offset(
    iqueue_t * const iq,
    iqueue_id_t id,
    size_t * const size_out
)
{
    // Retrieve the map-space pointer
    iqueue_msg_t * const msg_ptr = iqueue_get_slot(iq, id, 0);
    if (unlikely(!msg_ptr))
	return -1;

    iqueue_msg_t msg = *msg_ptr;
    if (unlikely(!msg.v))
	return -1;

    // Check if the iqueue is sealed at this index
    if (unlikely(msg.v == IQUEUE_MSG_SEALED))
	return -1;

    if (likely(size_out))
	*size_out = iqueue_msg_len(msg);

    TSLOGX(TSDIAG, "%s: %"PRIx64": %p = %"PRIx64, iq->name, id, msg_ptr, msg.v);
    return iqueue_msg_offset(msg);
}


int
iqueue_is_sealed(
    iqueue_t * const iqueue
)
{
    iqueue_id_t id = iqueue_end(iqueue);

    while (1)
    {
	int status = iqueue_status(iqueue, id++);
	if (status == IQUEUE_STATUS_HAS_DATA)
	    continue;

	return status == IQUEUE_STATUS_SEALED;
    }
}


int
iqueue_allocator_init(
    iqueue_t * const iq,
    iqueue_allocator_t * allocator,
    const size_t bulk_len,
    const int auto_refill
)
{
    memcpy(allocator, &(iqueue_allocator_t) {
	.iq		= iq,
	.bulk_len	= bulk_len,
	.auto_refill	= auto_refill,
    }, sizeof(*allocator));

    if (iqueue_allocator_refill(allocator) < 0)
	return -1;

    return 0;
}



int
iqueue_allocator_refill(
    iqueue_allocator_t * const allocator
)
{
    iqueue_msg_t msg;
    allocator->base = iqueue_allocate_raw(
	allocator->iq,
	allocator->bulk_len,
	&msg
    );
    if (!allocator->base)
	return -1;

    allocator->base_offset = iqueue_msg_offset(msg);
    allocator->offset = 0;

    TSLOGX(TSDEBUG, "%s: Refill base=%p offset=%"PRIx64" len=%"PRIx64,
	allocator->iq->name,
	allocator->base,
	allocator->base_offset,
	allocator->bulk_len
    );

    return 0;
}



int
iqueue_realloc(
    iqueue_allocator_t * const allocator,
    iqueue_msg_t * const msg,
    const size_t new_len
)
{
    const uint64_t msg_len = iqueue_msg_len(*msg);

    return iqueue_realloc_bulk(
	allocator,
	msg,
	msg_len,
	new_len
    );
}


int
iqueue_realloc_bulk(
    iqueue_allocator_t * const allocator,
    iqueue_msg_t * const msg,
    const size_t msg_len,
    const size_t new_len
)
{
    const uint64_t msg_offset = iqueue_msg_offset(*msg);
    if (new_len > IQUEUE_MSG_MAX || new_len > msg_len)
	return -1;

    // Where was the offset after this message was allocated
    const uint64_t cur_offset = msg_offset + msg_len - allocator->base_offset;

    // Where should the new offset be with the new length
    const uint64_t new_offset = msg_offset + new_len - allocator->base_offset;

    // If the offset in the allocator is not the same as cur_offset,
    // then further allocations have been done and we can't
    // resize this one.
    // \todo: Can we do this with atomics to save on locking?
    //return atomic_cas_bool_64(&allocator->offset, cur_offset, new_offset);
    if (allocator->offset != cur_offset)
	return 0;
    allocator->offset = new_offset;
    *msg = iqueue_msg(msg_offset, new_len);
    return 1;
}



int
iqueue_prefetch(
    iqueue_t * const iq,
    const uint64_t base,
    const uint64_t extent
)
{
    for (uint64_t offset = 0 ; offset < extent ; offset += 4096)
    {
	volatile uint64_t * data = (void*)(uintptr_t) iqueue_get_data(iq, base + offset, 1);
	if (!data)
	{
	    TSLOGX(TSERROR, "%s: Unable to get data at offset %"PRIx64, iq->name, base + offset);
	    return -1;
	}

	if (iq->mmap_prot & PROT_WRITE)
	    atomic_cas_bool_64(data, 0, 0);
	else
	    data[0];
    }

    return 0;
}



static void *
prefetch_thread(
    void * iq_ptr
)
{
    iqueue_t * const iq = iq_ptr;

    // Check to see where we last prefetched and start to request pages if
    // we have fallen behind
    const uint64_t prefetch_size = 16 << 20;
    const unsigned prefetch_delay = 100;

    uint64_t offset = iq->idx->data_tail;

    while (1)
    {
	if (prefetch_delay)
	    usleep(prefetch_delay);

	if (iq->idx->data_tail + prefetch_size / 2 < offset)
	    continue;

	// They have used up more than half our last block.
	// Start prefetching the next block
	uint64_t prefetch_time = -tsclock_getnanos(0);
	if (iqueue_prefetch(iq, offset, prefetch_size) < 0)
	    break;

	prefetch_time += tsclock_getnanos(0);
	TSLOGX(TSDEBUG, "%s: Prefetched %"PRIx64" to %"PRIx64" in %"PRIu64" ns",
	    iq->name,
	    offset,
	    offset + prefetch_size,
	    prefetch_time
	);

	offset += prefetch_size;
    }

    return NULL;
}


int
iqueue_prefetch_thread(
    iqueue_t * const iq,
    pthread_t * const thread_out
)
{
    if (!iq->prefetch_thread
    && pthread_create(&iq->prefetch_thread, NULL, prefetch_thread, iq) < 0)
    {
	TSLOG(TSERROR, "%s: Unable to create prefetch thread", iq->name);
	return -1;
    }

    if (thread_out)
	*thread_out = iq->prefetch_thread;
    return 0;
}


static void *
syncbehind_thread(
    void * iq_ptr
)
{
    iqueue_t * const iq = iq_ptr;

    const uint64_t active_block_count = 4;
    const unsigned syncbehind_delay = 1e6;

    uint64_t synced_to_block_id = 0;
    uint64_t mapped_to_block_id = 0;

    while (1)
    {
	if (syncbehind_delay)
	    usleep(syncbehind_delay);

	while (mapped_to_block_id < IQUEUE_BLOCK_COUNT &&
		iq->blocks[mapped_to_block_id] != NULL)
	    mapped_to_block_id++;

	// They have used up more than half our last block.
	// Start prefetching the next block
	for (; mapped_to_block_id - synced_to_block_id > active_block_count;
		synced_to_block_id++) {
	    uint64_t syncbehind_time = -tsclock_getnanos(0);
	    iqueue_fsync_block(iq, synced_to_block_id);

	    syncbehind_time += tsclock_getnanos(0);
	    TSLOGX(TSINFO, "%s: Synced block %"PRIu64" in %"PRIu64" ns",
		iq->name,
		synced_to_block_id,
		syncbehind_time
	    );
	}
    }

    return NULL;
}


int
iqueue_syncbehind_thread(
    iqueue_t * const iq,
    pthread_t * const thread_out
)
{
    if (!iq->syncbehind_thread
    && pthread_create(&iq->syncbehind_thread, NULL, syncbehind_thread, iq) < 0)
    {
	TSLOG(TSERROR, "%s: Unable to create syncbehind thread", iq->name);
	return -1;
    }

    if (thread_out)
	*thread_out = iq->syncbehind_thread;
    return 0;
}


static void
iqueue_table_debug(
    iqueue_t * const iq
)
{
    int skipped = 0;
    printf("table,slot,orig,route,offset,len\n");

    for (uint64_t i = 0 ; i < IQUEUE_TABLE_SIZE ; i++)
    {
	const uint64_t offset = iq->idx->tables[i];
	if (!offset)
	    continue;

	TSLOGX(TSINFO, "%s: table[0x%"PRIx64"] offset 0x%"PRIx64"%s",
	    iq->name,
	    i,
	    offset,
	    (offset & 0x7) ? " UNALIGNED" : ""
	);

	const uint64_t * const table = iqueue_get_data(iq, offset, 1);
	if (!table)
	    TSABORTX("%s: Unable to get table %"PRIu64"?", iq->name, i);

	for (uint64_t j = 0 ; j <= IQUEUE_TABLE_MASK ; j++)
	{
	    iqueue_msg_t msg = { .v = table[j] };
	    const uint64_t off = iqueue_msg_offset(msg);
	    const uint64_t len = iqueue_msg_len(msg);
	    if (!off && !len)
	    {
		skipped++;
		continue;
	    }

	    if (skipped)
		TSLOGX(TSERROR, "%s: Missing indices in table 0x%"PRIx64"!", iq->name, i);

	    const struct iqsync_data * const iqsync = iqsync_data_msg(iq, off);

	    printf("%"PRIx64",%"PRIx64",%"PRIx64":%"PRIu64",%"PRIx64":%"PRIu64",%"PRIx64",%"PRIu64"\n",
		i,
		j,
		iqsync ? be64toh(iqsync->orig_src) : 0,
		iqsync ? be64toh(iqsync->orig_index) : 0,
		iqsync ? be64toh(iqsync->src) : 0,
		iqsync ? be64toh(iqsync->iq_index) : 0,
		off,
		len
	    );
	}
    }
}


void
iqueue_debug(
    iqueue_t * const iq,
    uint64_t id
)
{
    if (id == (uint64_t) -1)
    {
	iqueue_table_debug(iq);
	return;
    }

    size_t len;
    uint64_t offset = iqueue_offset(iq, id, &len);
    if (offset == (uint64_t) -1)
    {
	TSLOGX(TSINFO, "%s: %"PRIu64": No slot allocated",
	    iq->name,
	    id
	);
	return;
    }

    const volatile iqueue_msg_t * const slot = iqueue_get_slot(iq, id, 0);
    TSLOGX(TSINFO, "%s: %"PRIu64": offset=%"PRId64" len=%zu slot=%p%s",
	iq->name,
	id,
	offset,
	len,
	slot,
	((uintptr_t) slot & 7) ? " UNALIGNED" : ""
    );

    const struct iqsync_data * const msg = iqsync_data_msg(iq, offset);
    if (msg)
    {
	TSLOGX(TSINFO, "%s: %"PRIu64": sending src=%"PRIu64":%"PRIu64" len=%u",
	    iq->name,
	    id,
	    be64toh(msg->src),
	    be64toh(msg->iq_index),
	    be32toh(msg->len)
	);

	TSLOGX(TSINFO, "%s: %"PRIu64": orig src=%"PRIu64":%"PRIu64,
	    iq->name,
	    id,
	    be64toh(msg->orig_src),
	    be64toh(msg->orig_index)
	);
    }

    const void * const data = iqueue_get_data(iq, offset, 1);
    if (!data)
    {
	TSLOGX(TSERROR,
	    "%s: %"PRIu64": Unable to retrieve data at offset %"PRIu64"?",
	    iq->name,
	    id,
	    offset
	);
	return;
    }

    TSHDUMP(TSINFO, data, len);
}


#define IQUEUE_WRITER_MAX (1<<15)
#define IQUEUE_WRITER_MASK (256-1)

static shash_t *
_iqueue_writer_table(
    iqueue_t * const iq,
    unsigned table_id,
    int create
)
{
    if (table_id >= IQUEUE_WRITER_TABLES)
	return NULL;
    if (iq->writer_tables[table_id])
	return iq->writer_tables[table_id];

    iqueue_index_t * const idx = iq->idx;
    iqueue_msg_t table_msg = idx->writer_tables[table_id];

    if (table_msg.v == 0)
    {
	if (!create)
	    return NULL;

	const size_t table_len = IQUEUE_WRITER_MAX * sizeof(shash_entry_t);
	const size_t table_max_len = table_len + IQUEUE_WRITER_MASK;

	void * const table_buf = iqueue_allocate_raw(
	    iq,
	    table_max_len,
	    &table_msg
	);
	if (!table_buf)
	{
	    TSLOGX(TSERROR, "%s: Unable to allocate table space %zu bytes",
		iqueue_name(iq),
		table_max_len
	    );
	    return NULL;
	}

	// Force alignment of the table since it will have 16-byte
	// CAS operations done on it.
	uint64_t offset = iqueue_msg_offset(table_msg);
	offset = (offset + IQUEUE_WRITER_MASK) & ~IQUEUE_WRITER_MASK;
	table_msg = iqueue_msg(offset, table_len);

	// Atomic swap it into the header; if this fails we do not care.
	// Some space in the iqueue will leak, but that is not a problem.
	atomic_cas_64(
	    (void*)(uintptr_t) &idx->writer_tables[table_id].v,
	    0,
	    table_msg.v
	);

	// Re-read the writer_table; either we succeeded or someone else has
	// already written to it.
	table_msg = idx->writer_tables[table_id];

	TSLOGX(TSINFO, "%s: Created writer table offset 0x%"PRIx64" size %zu",
	    iqueue_name(iq),
	    iqueue_msg_offset(table_msg),
	    iqueue_msg_len(table_msg)
	);
    }

    const size_t table_len = iqueue_msg_len(table_msg);
    const uint64_t table_offset = iqueue_msg_offset(table_msg);

    const void * const table_buf = iqueue_get_data(iq, table_offset, 1);

    if (!table_buf)
    {
	TSLOGX(TSERROR, "%s: Unable to retrieve table at offset 0x%"PRIx64,
	    iqueue_name(iq),
	    table_offset
	);
	return NULL;
    }

    shash_t * const sh = shash_create(table_buf, table_len, 0);
    if (!sh)
    {
	TSLOGX(TSERROR, "%s: Unable to generate table %p @ %zu",
	    iqueue_name(iq),
	    table_buf,
	    table_len
	);
	return NULL;
    }

    // This might race with another thread, causing this to leak.
    // Oh well.
    iq->writer_tables[table_id] = sh;

    return sh;
}


shash_t *
iqueue_writer_table(
    iqueue_t * const iq,
    unsigned table_id,
    int create
)
{
    return _iqueue_writer_table(iq, table_id, create);
}



int
iqueue_writer_update(
    shash_t * const sh,
    shash_entry_t * const writer,
    const uint64_t new_timestamp
)
{
    while (1)
    {
	const uint64_t cur_timestamp = writer->value;

	// If the new value is less than the old value
	// (and the old value is not -1), then there is no update
	// to be performed.
	if (cur_timestamp != (uint64_t) -1
	&&  cur_timestamp >= new_timestamp)
	    return 0;

	if (shash_update(
	    sh,
	    writer,
	    cur_timestamp,
	    new_timestamp
	))
	    return 1;
    }
}
