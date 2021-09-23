/*	$TwoSigma: iqueue.h,v 1.21 2012/02/07 13:37:46 thudson Exp $	*/

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

#ifndef _iqueue_h_
#define _iqueue_h_

#include "twosigma.h"
#include <stdint.h>
#include <stdbool.h>
#include <sys/types.h>
#include "bswap.h"
#include "shash.h"

__BEGIN_DECLS

/** \file
 * Multi-writer, persistent binary log file.
 *
 * Goals:
 * 1. Lockless, multi-writer access to a memory mapped file
 * 2. Threads can die at any time without corrupting meta data
 * 3. Readers can track messages in order without communication to writers
 * 4. Data file has no overhead from system; can have user specified
 *    headers, delimiters, etc.
 */

typedef struct _iqueue iqueue_t;
typedef uint64_t iqueue_id_t;


/** Index queue message pointers contain the length and the offset
 * packed into a single 64-bit value.
 *
 * The maximum size of the messages is defined by 64 - IQUEUE_MSG_BITS,
 * or 1 MB.
 */
typedef struct {
    uint64_t v;
} iqueue_msg_t;

#define IQUEUE_MSG_SEALED   ((uint64_t)-1)
#define IQUEUE_MSG_BITS	    44
#define IQUEUE_MSG_MASK	    ((((uint64_t) 1) << IQUEUE_MSG_BITS) - 1)
#define IQUEUE_MSG_MAX	    ((((uint64_t) 1) << (64 - IQUEUE_MSG_BITS)) - 1)
#define IQUEUE_MSG_BAD_ID   ((uint64_t) -1)


static inline iqueue_msg_t
iqueue_msg(
    uint64_t offset,
    uint64_t len
) {
    return (iqueue_msg_t) {
	(len << IQUEUE_MSG_BITS) | (offset & IQUEUE_MSG_MASK)
    };
}

static inline uint64_t
iqueue_msg_offset(
    iqueue_msg_t msg
)
{
    return (uintptr_t)(msg.v & IQUEUE_MSG_MASK);
}

static inline uint64_t
iqueue_msg_len(
    iqueue_msg_t msg
)
{
    return (uint64_t)(msg.v >> IQUEUE_MSG_BITS);
}


/** Open an iqueue_t object backed by a file.
 *
 * @param writable: if true the file will be opened for read/write
 * and entries can be added with iqueue_allocate() / iqueue_update().
 *
 * @return NULL if there are any errors.
 */
iqueue_t *
iqueue_open(
    const char * index_file,
    bool writable
);


/** Create or open an iqueue_t object backed by a file.
 *
 * Atomically creates an iqueue file on disk, or returns the existing
 * iqueue of the same name.  iqueue_creation() can be called to determine
 * if the iqueue that was created was new or an existing one.
 *
 * @param creation_time: Non-zero time in nanoseconds to record in the
 * iqueue header.
 */
iqueue_t *
iqueue_create(
    const char * index_file,
    const uint64_t creation_time,
    const void * user_hdr,
    size_t user_hdr_len
);


/** Create a prefetch thread to pre-fault pages for writing. */
int
iqueue_prefetch_thread(
    iqueue_t * const iq,
    pthread_t * const thread_out
);

/** Create a syncbehind thread to write out and release old blocks. */
int
iqueue_syncbehind_thread(
    iqueue_t * const iq,
    pthread_t * const thread_out
);

/** Re-open an iqueue.
 * Useful if the iqueue has been sealed, archived and a new one created.
 *
 * After a sucessful reopen, iqueue_creation() can be used to see if
 * a new iqueue has been returned or if it is the same one.
 *
 * \return 0 on success, -1 on error and sets errno:
 * ENOENT: File does not exist (not fatal, might be transitory due archiving)
 *
 */
int
iqueue_reopen(
    iqueue_t * iq
);


/** Close a iqueue_t.
 *
 * \note It is necessary that all threads using the iqueue_t
 * be stopped before closing!
 */
void
iqueue_close(
    iqueue_t * iqueue
);


/** Archive an iqueue by optionally sealing and renaming
 *
 * @param seal_id only archive if no new items have been added.
 * If seal_id == -1, the archive will not be sealed.
 *
 * @return 0 on succecss, -1 on error and sets errno:
 */
int
iqueue_archive(
    iqueue_t * iq,
    uint64_t seal_id
);




/** mlock all current mapped segments and any future ones.
 *
 * It is necessary to have sufficient mlock-able ulimit
 * to lock enough data to be useful.
 *
 * Run:
 *     ulimit -l unlimited
 *
 * Or configure the amount in /etc/security/limits:
 *
 *     * - memlock 16777216 # 16 GB
 */
int
iqueue_mlock(
    iqueue_t * iq
);


/** Return the number of entries in the iqueue.
 * \note This is not guaranteed to be the last entry since other
 * processes may have added new ones.  However, it will make a best
 * effort to ensure that there the pointer is actually at the end.
 */
uint64_t
iqueue_entries(
    const iqueue_t * iqueue
);


/** Return the amount of space used in the data file */
uint64_t
iqueue_data_len(
    const iqueue_t * const iq
);

/** Retrieve the user header from the iqueue.  */
void *
iqueue_header(
    iqueue_t * iqueue,
    size_t * hdr_len_out
);


/** Return the creation time of the iqueue */
uint64_t
iqueue_creation(
    const iqueue_t * iqueue
);

/** Returns non-zero if iqueue has been sealed */
int
iqueue_is_sealed(
    iqueue_t * iqueue
);


/** Return the filename of the iqueue */
const char *
iqueue_name(
    const iqueue_t * iqueue
);


typedef enum {
    IQUEUE_MADV_WILLNEED,
    IQUEUE_MADV_DONTNEED,
} iqueue_madvise_t;

/** Advise the kernel about regions of the iqueue.
 * @param start and end define the indices that are of interest.
 *
 * \note Since the iqueue data segment might not
 * be contiguous and in order, it is possible for the portion of
 * the file defined by start to be earlier than the region
 * defined by end.  No madvise request will be made in that case.
 */
int
iqueue_madavise(
    iqueue_t * iqueue,
    iqueue_madvise_t advice,
    iqueue_id_t start,
    iqueue_id_t end
);


/** Retrieve the shared hash of writers.
 *
 * The iqueue maintains a list of entries of "writers" that can
 * record monotonically increasing heartbeat values to indicate
 * to other iqueue readers as to their status.  The typical datum
 * recorded is a timestamp, although it can be any 64-bit value
 * as long as it is monotonically increasing.
 *
 * The iqueue has four such tables:
 * * 0 is reserved for user heartbeats and will be synchronized by
 *   iqsync.
 * * 1 is reserved for iqsync's shared source table.
 * * 2 and 3 are available for users of the local iqueue.
 *
 * @param create will cause the hash to be created if it does not already
 * exist.  This is done in an atomic, idempotent fashion.
 */
shash_t *
iqueue_writer_table(
    iqueue_t * const iq,
    unsigned table_id,
    int create
);


/** Update the status of the writer.
 *
 * \param sh must have been returned from iqueue_writer_table().
 * \param writer must have been returned by shash_insert() or shash_get().
 *
 * \param timestamp is any value, although only values strictly higher
 * will be written to the field with the exception of when the previous
 * value of the timestamp has been set to -1, which can
 * be used to indicate that a writer has exited or failed.
 *
 * \return 0 on success, 1 if the current value exceeds the timestamp
 * and -1 on error.
 */
int
iqueue_writer_update(
    shash_t * const sh,
    shash_entry_t * const writer,
    uint64_t timestamp
);




#define IQUEUE_BLOCK_SHIFT	30
#define IQUEUE_BLOCK_SIZE	((uint64_t) (1 << IQUEUE_BLOCK_SHIFT))
#define IQUEUE_BLOCK_MASK	(IQUEUE_BLOCK_SIZE - 1)


/** Returns the id of the first entry in the iqueue */
iqueue_id_t
iqueue_begin(
    const iqueue_t * iq
);


/** Returns an id immediately after the last entry in the iqueue */
iqueue_id_t
iqueue_end(
    const iqueue_t * iq
);


#define IQUEUE_STATUS_OK               0
#define IQUEUE_STATUS_HAS_DATA         1
#define IQUEUE_STATUS_INDEX_INVALID    2
#define IQUEUE_STATUS_NO_DATA          3
#define IQUEUE_STATUS_SEALED           4
#define IQUEUE_STATUS_INVALID_ARGUMENT 5
#define IQUEUE_STATUS_NO_SPACE         6


/** Returns in IQUEUE_STATUS_* code for the slot's state
 *
 * IQUEUE_STATUS_HAS_DATA         There is a message in this slot.
 * IQUEUE_STATUS_NO_DATA          There is no message in this slot.
 * IQUEUE_STATUS_INDEX_INVALID    id is off the charts of a valid index.
 * IQUEUE_STATUS_SEALED           The iqueue has been sealed at this slot.
 * IQUEUE_STATUS_INVALID_ARGUMENT Something is wrong.
 */
int
iqueue_status(
    const iqueue_t * iq,
    iqueue_id_t id
);


/** Waits up to timeout_ns nanoseconds (or indefinitely if -1) for a non-
 * NO_DATA status for the given id and returns it.
 */
int
iqueue_status_wait(
    iqueue_t * iq,
    iqueue_id_t id,
    int64_t timeout_ns
);



/** Returns the offset of the message at id, or -1 on error. */
uint64_t
iqueue_offset(
    iqueue_t * iq,
    iqueue_id_t id,
    size_t * size_out
);


/** Return a pointer to the data at a given offset.
 * If do_map is specified, the region will be brought into memory with mmap().
 */
const void *
iqueue_get_data(
    iqueue_t * iq,
    uint64_t offset,
    int do_map
);


/** Returns the data and size for the message at id, or NULL or error.
 *
 * The message is immutable once committed, so it is not permitted to
 * modify the memory returned.
 */
static inline const void *
iqueue_data(
    iqueue_t * const iq,
    iqueue_id_t id,
    size_t * const size_out
)
{
    uint64_t offset = iqueue_offset(iq, id, size_out);
    if (unlikely(offset == (uint64_t) -1))
	return NULL;

    return iqueue_get_data(iq, offset, 1);
}

typedef struct
{
    // Constants
    iqueue_t * iq;
    uint64_t bulk_len;
    int auto_refill;
    uint64_t align_mask; // mask for force alignment; 0 == none, 0x7 == 8 byte, 0x1f == 32 byte, etc

    // Updated every time a reallocation is done
    uint8_t * base;
    uint64_t base_offset;
    uint64_t offset;
} iqueue_allocator_t;



/** Allocate a region from the iqueue */
void *
iqueue_allocate_raw(
    iqueue_t * iq,
    const size_t len,
    iqueue_msg_t * msg_out
);


/** Get a bulk allocator for the iqueue.
 *
 * These allow fast thread-local allocation of messages without
 * contending for the iqueue's data_tail pointer.
 *
 * If auto_refill is set, the allocator will automatically refill with
 * a fresh bulk_size amount of data.  If the data segment needs to be
 * extended, this will also perform the extension.
 */
int
iqueue_allocator_init(
    iqueue_t * iq,
    iqueue_allocator_t * allocator,
    size_t bulk_size,
    int auto_refill
);


/** Get a new chunk for the allocator.
 * \note if auto_refill is set when the allocator is created,
 * this does not need to be called.
 *
 * \return 0 on success, -1 on failure.
 */
int
iqueue_allocator_refill(
    iqueue_allocator_t * allocator
);


/** Allocate from the chunk reserved to the bulk allocator.
 *
 * If there is insufficient space in this allocator and auto_refill
 * is not true, then NULL will be returned.  Otherwise a new chunk
 * will be reserved.
 *
 * \note It is possible to allocate more than the IQUEUE_MSG_MAX
 * size, but it will not be possible to commit the resulting message
 * to the index.
 */
static inline void *
iqueue_allocate(
    iqueue_allocator_t * const allocator,
    const size_t len,
    iqueue_msg_t * msg_out
)
{
    if (unlikely(len > allocator->bulk_len))
	return NULL;

    while (1)
    {
	uint64_t base = allocator->base_offset + allocator->offset;
	uint64_t aligned = (base + allocator->align_mask)
	    & ~allocator->align_mask;
	uint64_t offset = aligned + len - allocator->base_offset;

	if (likely(offset <= allocator->bulk_len))
	{
	    allocator->offset = offset;
	    *msg_out = iqueue_msg(aligned, len);
	    return allocator->base + aligned - allocator->base_offset;
	}

	// It didn't fit; try to get more space
	if (!allocator->auto_refill
	|| iqueue_allocator_refill(allocator) < 0)
	    return NULL;
    }
}



/** Resize an allocation to make it smaller.
 *
 * \return 0 on inability to resize (not an error),
 * 1 on success, and -1 on error.
 */
int
iqueue_realloc(
    iqueue_allocator_t * allocator,
    iqueue_msg_t * msg,
    size_t new_len
);


/** Resize an oversized allocation.
 * Some applications, like iqsync, might allocate more than the IQUEUE_MSG_MAX
 * size messages and need a way to resize them that can not be represented by
 * the iqueue_msg_t embedded offset.
 *
 * \return 0 on inability to resize (not an error),
 * 1 on success, and -1 on error.
 */
int
iqueue_realloc_bulk(
    iqueue_allocator_t * allocator,
    iqueue_msg_t * msg,
    size_t old_len,
    size_t new_len
);



/** Store a message at the end of the log, writing the index into
 * a location with the same lockless guarantee as the index update.
 *
 * This means that the slot id in the index which will be written with
 * pointer to this message will be written to id_out, stored before
 * the message is committed to the index, allowing lockless consistency
 * if id_out points to inside of the message itself.
 *
 * \return 0 on success, or an error code on failure:
 *
 * IQUEUE_STATUS_INDEX_INVALID == there is no space left in the index
 * IQUEUE_STATUS_SEALED == the iqueue has been sealed against writing
 * IQUEUE_STATUS_INVALID_ARGUMENT == bad offset or length in the iqmsg.
 */
int
iqueue_update(
    iqueue_t * iq,
    iqueue_msg_t msg,
    iqueue_id_t * id_out
);


/** Same as iqueue_update, except writes to id_out in big-endian */
int
iqueue_update_be(
    iqueue_t * iq,
    iqueue_msg_t msg,
    iqueue_id_t * id_be_out
);


/** Convienence function to commit a message to the iqueue */
static inline int
iqueue_append(
    iqueue_t * const iq,
    const void * const buf,
    size_t len
)
{
    if (len >= IQUEUE_MSG_MAX)
	return IQUEUE_STATUS_INVALID_ARGUMENT;

    iqueue_msg_t iqmsg;
    void * const msg = iqueue_allocate_raw(iq, len, &iqmsg);
    if (!msg)
	return IQUEUE_STATUS_NO_SPACE;

    memcpy(msg, buf, len);
    int rc = iqueue_update(iq, iqmsg, NULL);
    return rc;
}



/** Attempt to store a message in the log at the desired slot.
 * entry should point to the buffer returned from iqueue_allocate()
 * once all of the log data has been copied into it.
 *
 * \return same as iqueue_update, with the additional status:
 *
 * IQUEUE_STATUS_HAS_DATA == A message is present in this slot.
 */
int
iqueue_try_update(
    iqueue_t * iq,
    iqueue_id_t slot,
    iqueue_msg_t msg
);


/** Seals the iqueue, blocking any further write attempts.
 *
 * Once an iqueue has been sealed no further writes will be permitted.
 * iqueue_update() and iqueue_get_status() will return IQUEUE_STATUS_SEALED
 * and iqueue_data() of an index past or equal to the index_tail will
 * return an error.
 *
 * \return same as iqueue_update
 */
int
iqueue_seal(
    iqueue_t * iq
);


/** Attempts to seal the iqueue at the given index, blocking any further
 * write attempts.
 *
 * This is useful if the application wants to be sure that it has processed
 * every message in an iqueue before sealing it, without racing with
 * a writer.
 *
 * \return same as iqueue_try_update
 */
int
iqueue_try_seal(
    iqueue_t * iq,
    iqueue_id_t id
);


/** Prefetch some data.
 *
 * Typically this is invoked by the prefetch thread, but can be
 * done by any user as well.
 *
 * \note Will grow the file if necessary and perform additional
 * mappings to make the new regions resident.
 */
int
iqueue_prefetch(
    iqueue_t * iq,
    uint64_t offset,
    uint64_t extent
);


/** Report some debugging information about an iqueue entry */
void
iqueue_debug(
    iqueue_t * iq,
    uint64_t id
);

__END_DECLS

#endif
