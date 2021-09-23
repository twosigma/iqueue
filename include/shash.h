/** \file
 * Shared simple hash table.
 *
 * This lockless hash table is designed for use embedded in other
 * dma/transport files, such as iqueue headers for use of iqsync.
 *
 * The underlying hash lookup algorithm is not efficient since it
 * requires linear search, but the keys will never move once they
 * are added to the hash so it is possible to have a local hash that
 * maps pointers to the keys.
 *
 * The hash entries must be 16-byte aligned for the cmpxchgb16() to be
 * able to safely write to them.
 *
 * There is no init function since the only requirement is that
 * the hash entries must be zeroed when the space is made visible to
 * other threads or processes.
 *
 * \note key == 0 is not allowed.
 */
#ifndef _ts_dma_transport_shash_
#define _ts_dma_transport_shash_

#include <stdint.h>


typedef struct {
    const uint64_t key;
    const volatile uint64_t value;
} shash_entry_t;


typedef struct _shash_t shash_t;

__BEGIN_DECLS


/** Create a local representation of the shash.
 *
 * This does not create the underlying hash; shash_ptr must
 * have already been allocated and may already be in use
 * in a shared data structure.
 *
 * This stores a cache of the keys to speed up repeated calls to
 * shash_lookup().
 *
 * If read_only is set, shash_insert() and shash_update() will fail.
 * If read_only is not set, the const-ness will be cast away from the
 * buffer.
 *
 * Keys may not be deleted from a shash.  Once inserted, they will always
 * be present.
 */
shash_t *
shash_create(
    const void * shash_ptr,
    size_t shash_len,
    int read_only
);


/** Create a thread-local copy of a shash_t.
 *
 * Since the get operations require a lock to protect the shadow
 * hash, it can be expensive to serialize on the gets.  Create
 * a thread-local version that does not share its lock with the
 * original shash.
 */
shash_t *
shash_copy(
    shash_t * sh
);


/** Free local resources.
 *
 * This does not modify the underlying hash.
 */
void
shash_destroy(
    shash_t * shash
);


/** Retrieve a shash entry from the hash.
 *
 * This has O(n) time if the key has not been seen before,
 * unlike a normal hash, but repeated look ups will be O(1).
 *
 * If key does not exist in the hash, get will return NULL.
 */
shash_entry_t *
shash_get(
    shash_t * sh,
    uint64_t key
);


/** Insert a new key/value pair into the shared hash.
 *
 * This has O(n) time potentially since it will walk the hash
 * to examine any new items.
 *
 * If the key already exists in the shared hash or if there is no
 * space left in the shared hash, NULL will be returned.
 *
 * \note key == 0 is not allowed.
 */
shash_entry_t *
shash_insert(
    shash_t * sh,
    uint64_t key,
    uint64_t value
);


/** Insert a new key/value pair or return the existing one. */
static inline shash_entry_t *
shash_insert_or_get(
    shash_t * sh,
    uint64_t key,
    uint64_t value
)
{
    shash_entry_t * entry = shash_insert(sh, key, value);
    if (entry)
        return entry;
    return shash_get(sh, key);
}


/** Atomically set a new value if the old value has not changed.
 *
 * The internal logic is:
 *
 * If entry->value != old_value then
 *     return 0;
 * entry->value = new_value;
 * return 1;
 */
int
shash_update(
    shash_t * sh,
    shash_entry_t * entry,
    uint64_t old_value,
    uint64_t new_value
);


/** Return the pointer to the array of entries.
 */
shash_entry_t *
shash_entries(
    shash_t * const sh,
    unsigned * const max_entries
);


__END_DECLS

#endif
