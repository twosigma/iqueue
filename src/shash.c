
#include "twosigma.h"
#include "tslog.h"
#include "shash.h"
#include "atomic.h"
#include "tslock.h"



#define TSFLEXHASH_NAME shash_cache
#define TSFLEXHASH_KEY_TYPE uint64_t
#define TSFLEXHASH_VAL_TYPE shash_entry_t *
#define TSFLEXHASH_COPY_VAL 1
#define TSFLEXHASH_CAPACITY 65536
#include "tsflexhash.h"


struct _shash_t
{
    // Pointer to the shared table.
    shash_entry_t * const hash;

    // Maximum number of entries in the shared table.
    // This is fixed at create time.
    const unsigned count;

    // If the table is read only, shash_insert() and shash_update()
    // calls will fail.
    const unsigned read_only;

    // Local cache of the keys with pointers to the entries.
    // This is updated on every failed shash_get() and shash_insert()
    // call and protected with cache_lock from other threads in the
    // same processes.
    shash_cache_t * const cache;
    tslock_t * const cache_lock;

    // Record of how many entries have been read from the shared table
    // so that the cache refresh doesn't need to re-read the entire
    // table.
    unsigned scan_index;
};


/** Populate the hash with any new entries in the shared table.
 * Must be called with the cache lock held.
 */
static void
_shash_populate(
    shash_t * const sh
)
{
    if (unlikely(!tsislocked(sh->cache_lock)))
        TSABORTX("%p: Cache is not locked?", sh->hash);

    unsigned i;

    for (i = sh->scan_index ; i < sh->count ; i++)
    {
        // Copy the struct to avoid race conditions in multiple reads
        shash_entry_t * const entry = &sh->hash[i];
        const uint64_t key = entry->key;

        if (key == 0)
            break;

        if (shash_cache_get(sh->cache, key) != NULL)
            TSABORTX("key 0x%"PRIx64" already in cache?  shash fatal failure",
                key
            );

        shash_cache_insert(sh->cache, key, entry);
    }

    TSLOGXL(TSDEBUG, "%p: Scanned from %u to %u",
        sh->hash,
        sh->scan_index,
        i
    );

    sh->scan_index = i;
}


/** Check to see if a key exists in the local cache, and if not,
 * repopulate the cache from the shared array.
 *
 * Must be called with the cache lock held.
 */
static shash_entry_t *
_shash_get(
    shash_t * const sh,
    const uint64_t key
)
{
    if (unlikely(!tsislocked(sh->cache_lock)))
        TSABORTX("%p: Cache is not locked?", sh->hash);

    shash_entry_t * const entry = shash_cache_get(sh->cache, key);
    if (entry)
        return entry;

    // Not found?  Try updating the cache
    _shash_populate(sh);
    return shash_cache_get(sh->cache, key);
}




shash_t *
shash_create(
    const void * shash_ptr,
    size_t shash_len,
    int read_only
)
{
    const uintptr_t shash_addr = (uintptr_t) shash_ptr;
    if (shash_addr & 0xF)
    {
        TSLOGXL(TSERROR, "%p: Incorrect alignment for shash!", shash_ptr);
        return NULL;
    }

    shash_t * const sh = calloc(1, sizeof(*sh));
    if (!sh)
    {
        TSLOGL(TSERROR, "allocation failure");
        return NULL;
    }

    memcpy(sh, &(shash_t) {
        .hash       = (void*) shash_addr,
        .count      = shash_len / sizeof(*sh->hash),
        .cache      = shash_cache_create(),
        .cache_lock = tslock_alloc(),
        .read_only  = read_only,
        .scan_index = 0,
    }, sizeof(*sh));

    // The cache does not need to be locked at this point since the
    // shash isn't shared with any other threads, but _shash_populate()
    // enforces a lock check.
    tslock(sh->cache_lock);
    _shash_populate(sh);
    tsunlock(sh->cache_lock);

    return sh;
}


shash_t *
shash_copy(
    shash_t * const old_sh
)
{
    shash_t * const sh = calloc(1, sizeof(*sh));
    if (!sh)
    {
        TSLOGL(TSERROR, "allocation failure");
        return NULL;
    }

    memcpy(sh, &(shash_t) {
        .hash       = old_sh->hash,
        .count      = old_sh->count,
        .cache      = shash_cache_create(),
        .cache_lock = tslock_alloc(),
        .read_only  = old_sh->read_only,
        .scan_index = 0,
    }, sizeof(*sh));

    // The cache does not need to be locked at this point since the
    // shash isn't shared with any other threads, but _shash_populate()
    // enforces a lock check.
    tslock(sh->cache_lock);
    _shash_populate(sh);
    tsunlock(sh->cache_lock);

    return sh;
}


void
shash_destroy(
    shash_t * const sh
)
{
    shash_cache_destroy(sh->cache);
    free(sh);
}


shash_entry_t *
shash_get(
    shash_t * const sh,
    const uint64_t key
)
{
    tslock(sh->cache_lock);
    shash_entry_t * const entry = _shash_get(sh, key);
    tsunlock(sh->cache_lock);
    return entry;
}


shash_entry_t *
shash_entries(
    shash_t * const sh,
    unsigned * const max_entries_out
)
{
    if (max_entries_out)
        *max_entries_out = sh->count;
    return sh->hash;
}


/** 16-byte compare and swap.
 * gcc-4.4 doesn't have an intrinsic for this operation, so it is implemented
 * with inline assembly here.  This should likely be moved to base/atomic.
 */
static inline int
cmpxchg16b(
    volatile void * addr,
    uint64_t old1,
    uint64_t old2,
    uint64_t new1,
    uint64_t new2
)
{
    char result;

    volatile uint64_t * const ptr = (volatile void*) addr;
    if (unlikely(0xF & (uintptr_t) ptr))
        TSABORTX("%p: Insufficient alignment for 16-byte atomics", addr);

    TSLOGXL(TSDIAG, "%p: %"PRIx64":%"PRIx64" -> %"PRIx64":%"PRIx64"",
        addr,
        ptr[0],
        ptr[1],
        new1,
        new2
    );

    __asm__ __volatile__(
        "lock; cmpxchg16b %0; setz %1"
        : "=m"(*ptr), "=q"(result)
        : "m"(*ptr), "d" (old2), "a" (old1), "c" (new2), "b" (new1)
        : "memory"
    );

    return (int) result;
}


/** Must be called with the cache_lock held. */
static shash_entry_t *
_shash_insert(
    shash_t * const sh,
    const uint64_t key,
    const uint64_t value
)
{
    if (unlikely(!tsislocked(sh->cache_lock)))
        TSABORTX("%p: Cache is not locked?", sh->hash);

    if (key == 0 || sh->read_only)
        return NULL;

    while (1)
    {
        shash_entry_t * const old_entry = _shash_get(sh, key);
        if (old_entry)
        {
            TSLOGXL(TSDIAG, "%p: Key 0x%"PRIx64" exists!", sh->hash, key);
            return NULL;
        }

        // Make sure it is still within the allocated region
        const unsigned slot = sh->scan_index;
        if (unlikely(slot >= sh->count))
        {
            TSLOGXL(TSERROR, "%p: Shared hash is full!", sh->hash);
            return NULL;
        }

        // sh->scan_index now points to the first empty slot.
        shash_entry_t * const entry = &sh->hash[slot];

        // If it is still empty we can cmpxchg16 our new entry
        // into its place.  Let's see how it goes
        if (!cmpxchg16b(entry, 0, 0, key, value))
            continue;

        // We won!
        TSLOGXL(TSDEBUG, "%p: Key 0x%"PRIx64" inserted in slot %u",
            sh->hash,
            key,
            slot
        );

        return entry;
    }
}


shash_entry_t *
shash_insert(
    shash_t * const sh,
    const uint64_t key,
    const uint64_t value
)
{
    tslock(sh->cache_lock);
    shash_entry_t * const entry = _shash_insert(sh, key, value);
    tsunlock(sh->cache_lock);
    return entry;
}


int
shash_update(
    shash_t * sh,
    shash_entry_t * entry,
    uint64_t old_value,
    uint64_t new_value
)
{
    // Verify that it is safe to write into this entry
    if (sh->read_only
    ||  entry <  &sh->hash[0]
    ||  entry >= &sh->hash[sh->count])
        return 0;

    // No locks need to be held for this update
    return atomic_cas_bool_64(
        (void*)(uintptr_t) &entry->value,
        old_value,
        new_value
    );
}
