/*
 *    Copyright 2021 Two Sigma Open Source, LLC
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
#include "twosigma.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/mman.h>
#include <errno.h>
#include <unistd.h>

#include "tslog.h"
#include "assert.h"
#include "err.h"
#include "tsflexhash_private.h"


typedef struct entry_block entry_block_t;

struct entry_block {
    entry_block_t *next;
};

typedef struct {
    uint32_t mask, size;
    void *pool;
    size_t entry_size, next_offset;
    const char *name;
    void *(*malloc_function)(size_t);
    void (*free_function)(void *);
    entry_block_t *entry_blocks;
    uint32_t collision_count;
    uint32_t capacity_bit_count;
    uint64_t chain_link_traversal_count;
    uint64_t align;
} tsflexhash_t;

typedef struct {   
    uint32_t mask, size;
    void *pool;
    size_t entry_size, next_offset;
    const char *name;
    void *(*malloc_function)(size_t);
    void (*free_function)(void *);
    entry_block_t *entry_blocks;
    uint32_t collision_count;
    uint32_t capacity_bit_count;
    uint64_t chain_link_traversal_count;
    void *entries;
} tsflexhash_new_t;

// this makes sure all pages are faulted in, and it checks that the malloc
// function is returning memory that is zeroed out.
static void
memtouch(void *p, size_t len)
{
    TSLOGX(TSDIAG, "Checking %zu bytes at %p", len, p);
    assert((len & 0x3) == 0);
    uint32_t *i = p;
    uint32_t *i1 = i + (len >> 2);
    while (i < i1) {
        if (*i != 0) {
            errx(1, "Prefaulted byte %p is nonzero", i);
        }
	i++;
    }
    TSLOGX(TSDIAG, "%zu bytes at %p are zero", len, p);
}

void *
tsflexhash_create_norehash(size_t entry_size, size_t next_offset, uint32_t capacity,
	const char *name, void *(*malloc_function)(size_t),
	void (*free_function)(void *))
{
    TSLOGX(TSDEBUG, "Creating %s hashtable of size %" PRIu32 ". Entry size: "
	    "%zu. Next offset: %zu", name, capacity, entry_size, next_offset);
    assert(capacity != 0);
    uint32_t mask = capacity - 1;
    if ((capacity & mask) != 0) {
	TSLOGX(TSERROR, "Capacity is not a power of two: %" PRIu32, capacity);
	abort();
    }
    // malloc() used to be okay, but once we started using
    // tsflexhash_replenish_pool_norehash(), we need memory to be zeroed out.
    if (malloc_function == &malloc) {
	TSLOGX(TSERROR, "malloc() is not a valid memory allocator.");
	abort();
    }
    size_t malloc_size = sizeof(tsflexhash_t) - sizeof(uint64_t) + ((capacity + 1) * entry_size);
    TSLOGX(TSDEBUG, "malloc_size: %zu", malloc_size);
    tsflexhash_t *hashtable = malloc_function(malloc_size);
    assert(hashtable != NULL);
    memtouch(hashtable, malloc_size);
    hashtable->mask = mask;
    hashtable->entry_size = entry_size;
    hashtable->next_offset = next_offset;
    hashtable->name = name;
    hashtable->malloc_function = malloc_function;
    hashtable->free_function = free_function;
    hashtable->pool = TSFLEXHASH_SENTINEL;
    hashtable->collision_count = 0;
    hashtable->chain_link_traversal_count = 0;
    hashtable->capacity_bit_count = ffs(capacity) - 1;
    tsflexhash_replenish_pool_norehash(hashtable, true);
    return hashtable;
}

void *
tsflexhash_create_rehash(size_t entry_size, size_t next_offset, uint32_t capacity,
	const char *name, void *(*malloc_function)(size_t),
	void (*free_function)(void *))
{
    TSLOGX(TSDEBUG, "Creating %s hashtable of size %" PRIu32 ". Entry size: "
	    "%zu. Next offset: %zu", name, capacity, entry_size, next_offset);
    assert(capacity != 0);
    uint32_t mask = capacity - 1;
    if ((capacity & mask) != 0) {
	TSLOGX(TSERROR, "Capacity is not a power of two: %" PRIu32, capacity);
	abort();
    }
    // malloc() used to be okay, but once we started using
    // tsflexhash_replenish_pool_rehash(), we need memory to be zeroed out.
    if (malloc_function == &malloc) {
	TSLOGX(TSERROR, "malloc() is not a valid memory allocator.");
	abort();
    }
    size_t malloc_size = sizeof(tsflexhash_t) + (capacity * entry_size);
    TSLOGX(TSDEBUG, "malloc_size: %zu", malloc_size);
    tsflexhash_new_t *hashtable = malloc_function(sizeof(tsflexhash_new_t));
    assert(hashtable != NULL);
    hashtable->mask = mask;
    hashtable->entry_size = entry_size;
    hashtable->next_offset = next_offset;
    hashtable->name = name;
    hashtable->malloc_function = malloc_function;
    hashtable->free_function = free_function;
    hashtable->pool = TSFLEXHASH_SENTINEL;
    hashtable->entries = malloc_function(capacity * entry_size);
    assert(hashtable->entries != NULL);
    memtouch(hashtable->entries, capacity * entry_size);
    hashtable->capacity_bit_count = ffs(capacity) - 1;
    hashtable->collision_count = 0;
    hashtable->chain_link_traversal_count = 0;
    tsflexhash_replenish_pool_rehash(hashtable, true);
    return hashtable;
}

void
tsflexhash_destroy_norehash(void *p)
{
    tsflexhash_t *hashtable = p;
    entry_block_t *entry_block = hashtable->entry_blocks;
    while (entry_block != NULL) {
	entry_block_t *next = entry_block->next;
	hashtable->free_function(entry_block);
	entry_block = next;
    }
    hashtable->free_function(hashtable);
}

void
tsflexhash_destroy_rehash(void *p)
{
    tsflexhash_new_t *hashtable = p;
    entry_block_t *entry_block = hashtable->entry_blocks;
    while (entry_block != NULL) {
	entry_block_t *next = entry_block->next;
	hashtable->free_function(entry_block);
	entry_block = next;
    }
    hashtable->free_function(hashtable->entries);
    hashtable->free_function(hashtable);
}

// this new version of tsflexhash_replenish_pool uses TSFLEXHASH_SENTINEL to
// mark the end of the list (instead of NULL). this allows us to add blocks of
// entries to the pool without having to touch each one. see tsflexhash_insert()
// for more information
void *
tsflexhash_replenish_pool_norehash(void *p, bool first)
{
    tsflexhash_t *hashtable = p;
    uint32_t replenish_count = (hashtable->mask + 1) / 4;
    if (replenish_count == 0) {
	replenish_count = 1;
    }
    size_t malloc_size = sizeof(entry_block_t) +
	(replenish_count * hashtable->entry_size);
    // use TSINFO here when the pool is replenished during use (i.e. not during
    // creation), since this means your table is over-loaded and you should
    // probably increase its size.
    int level = first ? TSDEBUG : TSINFO;
    TSLOGX(level, "Replenishing pool for %s. Will malloc %zu bytes "
	    "for %" PRIu32 " entries. Table currently has %" PRIu32
	    " entries and a capacity of %" PRIu32 ".",
	    hashtable->name,
	    malloc_size,
	    replenish_count,
	    hashtable->size,
	    hashtable->mask + 1);
    assert(hashtable->pool == TSFLEXHASH_SENTINEL);
    entry_block_t *block = hashtable->malloc_function(malloc_size);
    assert(block != NULL);
    if (first) {
	memtouch(block, malloc_size);
    }
    assert(block->next == NULL); // this memory should be all zeroes

    // update the linked list of blocks (used in tsflexhash_destroy)
    block->next = hashtable->entry_blocks;
    hashtable->entry_blocks = block;

    // update the hashtable's pool pointer
    void *entry = block + 1;
    hashtable->pool = entry;

    // find the 'next' pointer in the last entry in the block, and set it
    // to the sentinel value. this marks the end of the block
    char *last_entry = ((char *) entry) +
	(hashtable->entry_size * (replenish_count - 1));
    void **next = (void *) (last_entry + hashtable->next_offset);
    assert(*next == NULL); // this memory should be all zeroes
    *next = TSFLEXHASH_SENTINEL;

    return hashtable->pool;
}

void *
tsflexhash_replenish_pool_rehash(void *p, bool first)
{
    tsflexhash_new_t *hashtable = p;
    uint32_t replenish_count = (hashtable->mask + 1) / 4;
    if (replenish_count == 0) {
	replenish_count = 1;
    }
    size_t malloc_size = sizeof(entry_block_t) +
	(replenish_count * hashtable->entry_size);

    // For rehashing tables, don't print messages about the table growing

    assert(hashtable->pool == TSFLEXHASH_SENTINEL);
    entry_block_t *block = hashtable->malloc_function(malloc_size);
    assert(block != NULL);
    if (first) {
	memtouch(block, malloc_size);
    }
    assert(block->next == NULL); // this memory should be all zeroes

    // update the linked list of blocks (used in tsflexhash_destroy)
    block->next = hashtable->entry_blocks;
    hashtable->entry_blocks = block;

    // update the hashtable's pool pointer
    void *entry = block + 1;
    hashtable->pool = entry;

    // find the 'next' pointer in the last entry in the block, and set it
    // to the sentinel value. this marks the end of the block
    char *last_entry = ((char *) entry) +
	(hashtable->entry_size * (replenish_count - 1));
    void **next = (void *) (last_entry + hashtable->next_offset);
    assert(*next == NULL); // this memory should be all zeroes
    *next = TSFLEXHASH_SENTINEL;

    return hashtable->pool;
}

// simple malloc function. note that it must return memory that is zeroed out.
void *
tsflexhash_malloc(size_t size)
{
   void * p = calloc(1, size);
    if (p == NULL) {
	TSABORT("Failure allocating %zu bytes", size);
    }
    return p;
}

void
tsflexhash_free(void *p)
{
    assert(p != NULL);
    free(p);
}
