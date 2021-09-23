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

/**
 * A chaining hashtable that can be customized and optimized for different
 * key/value types. This code is designed to be used in performance-sensitive
 * applications.
 *
 * This header file works like a C++ template.  Each time you include this
 * header file you will be defining a new
 * type of hashtable and a new set of functions that go along with it. The
 * type and the function names will all be prefixed with a name you define via
 * a macro. The features of the table are also controlled by macros you define
 * before including this header file.  Here is an example of how this works:
 *
 * #include <stdio.h>
 *
 * typedef struct {
 *   int order_id;
 *   const char *ticker;
 *   int shares;
 * } order_t;
 *
 * #define TSFLEXHASH_NAME order_table
 * #define TSFLEXHASH_KEY_TYPE int
 * #define TSFLEXHASH_VAL_TYPE order_t
 * #include "tsflexhash.h"
 *
 * static order_table_t *ot;
 * static int next_order_id = 1;
 *
 * int main() {
 *   ot = order_table_create(4096);
 * }
 *
 * order_t *create_order(const char *ticker, int shares) {
 *   order_t *order = order_table_insert(ot, next_order_id);
 *   order->id = next_order_id++;
 *   order->ticker = ticker;
 *   order->shares = shares;
 *   return order;
 * }
 *
 * order_t *get_order(int order_id) {
 *   return order_table_get(ot, order_id);
 * }
 *
 * order_t *kill_order(int order_id) {
 *   order = order_table_remove(ot, order_id);
 * }
 *
 * void print_orders(void) {
 *   printf("total order_count: %" PRIu32 "\n", order_table_size(ot));
 *   order_table_iterator_t i;
 *   order_table_iterator_init(ot, &i);
 *   for (;;) {
 *     int order_id;
 *     order_t *order = order_table_iterator_next(ot, &i, &order_id);
 *     if (order == NULL) {
 *       break;
 *     }
 *     assert(order_id == order->id);
 *     printf("%d: %d shares of %s\n", order->id, order->shares, order_ticker);
 *   }
 * }
 *
 * #define TSFLEXHASH_AUTO_REHASH 1
 * #include "tsflexhash.h"
 * 
 * void create_order_with_auto_rehash(const char *ticker, int shares) {
 *   order_table_create(32);
 *   for (int i = 0; i < 4096; i++) {
 *      order_t *order = order_table_insert(ot, next_order_id); // rehash will auto happen here.
 *      order->id = next_order_id++;
 *      order->ticker = ticker;
 *      order->shares = shares;
 *    } 
 * }
 * 
 * #define TSFLEXHASH_REHASH 1
 * #include "tsflexhash.h"
 * void create_order_with_rehash(const char *ticker, int shares) {
 *   ot = order_table_create(256);
 *   for (int j = 0; j < 16; j++)
 *      for (int i = 0; i < 256; i++) {
 *          order_t *order = order_table_insert(ot, next_order_id); // rehash will auto happen here.
 *          order->id = next_order_id++;
 *          order->ticker = ticker;
 *          order->shares = shares;
 *       }
 *       order_table_rehash(ot);
 *    } 
 * }
 */

#include <sys/types.h>
#include <stddef.h>
#include <stdint.h>
#include <inttypes.h>
#include "tsflexhash_private.h"
#include "tsgosmacs.h"
#include "twosigma.h"

__BEGIN_DECLS

// a name like order_table
#ifndef TSFLEXHASH_NAME
# error "TSFLEXHASH_NAME not defined"
#endif /* TSFLEXHASH_NAME */

// set this if you are using char * keys and want the standard behavior
// (gosmacs hash function, NULL keys not allowed)
#ifndef TSFLEXHASH_STRING_KEYS
# define TSFLEXHASH_STRING_KEYS 0
#endif /* TSFLEXHASH_STRING_KEYS */

// same as above, but keys are const char *
#ifndef TSFLEXHASH_CONST_STRING_KEYS
# define TSFLEXHASH_CONST_STRING_KEYS 0
#endif /* TSFLEXHASH_CONST_STRING_KEYS */

#if TSFLEXHASH_STRING_KEYS
# define TSFLEXHASH_KEY_TYPE char *
# define TSFLEXHASH_KEY_CONST 0
#elif TSFLEXHASH_CONST_STRING_KEYS
# define TSFLEXHASH_KEY_TYPE const char *
# define TSFLEXHASH_KEY_CONST 1
#endif /* TSFLEXHASH_STRING_KEYS */

#if TSFLEXHASH_STRING_KEYS || TSFLEXHASH_CONST_STRING_KEYS
# define TSFLEXHASH_KEY_EQUALS(a,b) strcmp(a,b) == 0
# define TSFLEXHASH_KEY_HASH(k) gosmacs_hash(k)
#endif /* TSFLEXHASH_STRING_KEYS */

// a type like int64_t or char *
#ifndef TSFLEXHASH_KEY_TYPE
# error "TSFLEXHASH_KEY_TYPE not defined"
#endif /* TSFLEXHASH_KEY_TYPE */

// Optional: set this to 1 if your key type includes a "const" qualifier, 0 if
// it doesn't. If you set it to 0, "const" to be added to the key parameters
// of get() and remove(), which lets you pass const arguments in (e.g. string
// literals)
#ifndef TSFLEXHASH_KEY_CONST
# define TSFLEXHASH_GET_KEY_QUALIFIER
#elif TSFLEXHASH_KEY_CONST
# define TSFLEXHASH_GET_KEY_QUALIFIER
#else
# define TSFLEXHASH_GET_KEY_QUALIFIER const
#endif /* TSFLEXHASH_KEY_CONST */

// if you need to free your keys, set this to 1, and the remove function will
// have an extra argument that gives you a pointer to the stored key
#ifndef TSFLEXHASH_REMOVE_RETURNS_KEY
# define TSFLEXHASH_REMOVE_RETURNS_KEY 0
#endif /* TSFLEXHASH_REMOVE_RETURNS_KEY */

// if you want to use erase instead of remove. set this to 1.
// unlike remove, erase returns nothing and zeroes out the whole entry,
// so your insert is guaranteed to give you zero buffer when using erase,
// noted that erase is more expensive than remove because it zeroes out
// the entire entry rather than only set the invalid bit
#ifndef TSFLEXHASH_USE_ERASE
# define TSFLEXHASH_USE_ERASE 0
#endif /* TSFLEXHASH_USE_ERASE */

// an optional comparison function. this is only needed if your keys are
// pointers and you want to compare the values
#ifndef TSFLEXHASH_KEY_EQUALS
# define TSFLEXHASH_KEY_EQUALS(a,b) a == b
#endif /* TSFLEXHASH_KEY_EQUALS */

// an optional hashcode function. this is needed if your keys are pointers.
// you may also want to provide it if the default implementation results in too
// many collisions
// TSFLEXHASH_KEY_HASH should take one argument, key, and return hashed value of the key
// TSFLEXHASH_KEY_HASH_EXT should take two arguments, hashtable and key, and return hashed value of the key
#if !(defined TSFLEXHASH_KEY_HASH || defined TSFLEXHASH_KEY_HASH_EXT)
# define TSFLEXHASH_KEY_HASH(key) key
#endif /* TSFLEXHASH_KEY_HASH */

// the type of values stored in the table, e.g. order_t.
#ifndef TSFLEXHASH_VAL_TYPE
# error "TSFLEXHASH_VAL_TYPE not defined"
#endif /* TSFLEXHASH_VAL_TYPE */

// the size of your value type. this is only needed if you want to override the
// default size of sizeof(val_type)
// TSFLEXHASH_VAL_SIZE

// set to non-zero if you want to store pointers in the table, or primitive
// values, rather than structs. this changes the API for get() and insert()
// functions
#ifndef TSFLEXHASH_COPY_VAL
# define TSFLEXHASH_COPY_VAL 0
#endif /* TSFLEXHASH_COPY_VAL */

// set this if you want get(), remove(), and iterator_next() to return something
// other than 0 when there is no entry to return
#ifndef TSFLEXHASH_EMPTY_RETURN_VAL
# define TSFLEXHASH_EMPTY_RETURN_VAL (TSFLEXHASH_RETURN_TYPE) 0
#endif /* TSFLEXHASH_EMPTY_RETURN_VAL */

// set this to 0 if you want to speed up insert/remove operations by not
// incrementing/decrementing the hashtable size counter
#ifndef TSFLEXHASH_UPDATE_SIZE
# define TSFLEXHASH_UPDATE_SIZE 1
#endif /* TSFLEXHASH_UPDATE_SIZE */

// set this to 1 if you want to count number of collisions,
// and measure the sum of numbers of links in the chain
// that have to be traversed because of collisions
#ifndef TSFLEXHASH_COUNT_COLLISION
# define TSFLEXHASH_COUNT_COLLISION 0
#endif /* TSFLEXHASH_COUNT_COLLISION */

// the optional capacity of your hashtable. this must be a power of two. if you
// don't specify the capacity here, you must pass it as an argument to the
// create function
#ifndef TSFLEXHASH_CAPACITY
# define TSFLEXHASH_MASK(hashtable) ((hashtable)->mask)
#else
# define TSFLEXHASH_MASK(hashtable) ((TSFLEXHASH_CAPACITY) - 1)
#endif /* TSFLEXHASH_CAPACITY */

// set this to 1 if you want to use tsnuma_malloc() and tsnuma_free()
#if defined(TSFLEXHASH_NUMA) && TSFLEXHASH_NUMA
// tsnuma is broken, because according to Valgrind libnuma is broken
# if defined(JNI_TRUE)
#  error "You cannot use tsnuma in JNI code. It's broken!!!!"
# endif /* JNI_TRUE */
# include "tsnuma.h"
# define TSFLEXHASH_MALLOC tsnuma_malloc
# define TSFLEXHASH_FREE tsnuma_free

#endif /* TSFLEXHASH_NUMA */

// set these if you want the hashtable to use custom malloc() and free()
// functions. note that the malloc function you specify *must* return zeroed-out
// memory, otherwise the hashtable will become corrupted
#ifndef TSFLEXHASH_MALLOC
# define TSFLEXHASH_MALLOC tsflexhash_malloc
#endif /* TSFLEXHASH_MALLOC */

#ifndef TSFLEXHASH_FREE
# define TSFLEXHASH_FREE tsflexhash_free
#endif /* TSFLEXHASH_FREE */


// -------- no user serviceable parts below this line --------- //


#define TSFLEXHASH_NAME_LITERAL1(name) # name
#define TSFLEXHASH_NAME_LITERAL0(name) TSFLEXHASH_NAME_LITERAL1(name)
#define TSFLEXHASH_NAME_LITERAL TSFLEXHASH_NAME_LITERAL0(TSFLEXHASH_NAME)

#define TSFLEXHASH_MANGLE1(name,foo) name ## _ ## foo
#define TSFLEXHASH_MANGLE0(name,foo) TSFLEXHASH_MANGLE1(name,foo)
#define TSFLEXHASH_MANGLE(foo) TSFLEXHASH_MANGLE0(TSFLEXHASH_NAME,foo)

#define TSFLEXHASH_ENTRY_T TSFLEXHASH_MANGLE(entry_t)
#define TSFLEXHASH_T TSFLEXHASH_MANGLE(t)

#if TSFLEXHASH_COPY_VAL
# define TSFLEXHASH_RETURN_VALUE(entry) entry->val.obj;
# define TSFLEXHASH_RETURN_TYPE TSFLEXHASH_VAL_TYPE
#else
# define TSFLEXHASH_RETURN_VALUE(entry) &entry->val.obj;
# define TSFLEXHASH_RETURN_TYPE TSFLEXHASH_VAL_TYPE *
#endif /* TSFLEXHASH_COPY_VAL */

#ifdef TSFLEXHASH_AUTO_REHASH
# ifdef TSFLEXHASH_CAPACITY
#  error "You cannot set AUTO_REHASH and CAPACITY at the same time"
# endif /* TSFLEXHASH_CAPACITY */
# ifndef TSFLEXHASH_REHASH
#  define TSFLEXHASH_REHASH 1
# endif /* TSFLEXHASH_REHASH */
#endif /* TSFLEXHASH_AUTO_REHASH */

#ifdef TSFLEXHASH_KEY_HASH
# define TSFLEXHASH_ENTRY(hashtable,key) (&hashtable->entries[\
    ((uint32_t) TSFLEXHASH_KEY_HASH(key)) & \
    ((uint32_t) TSFLEXHASH_MASK(hashtable))])
#else
# define TSFLEXHASH_ENTRY(hashtable,key) (&hashtable->entries[\
    ((uint32_t) TSFLEXHASH_KEY_HASH_EXT(hashtable, key)) & \
    ((uint32_t) TSFLEXHASH_MASK(hashtable))])
#endif /* TSFLEXHASH_KEY_HASH */

#define TSFLEXHASH_ZERO_ENTRY(entry) \
    memset(&entry->val.obj, 0, sizeof(entry->val.obj))

typedef struct TSFLEXHASH_MANGLE(entry) TSFLEXHASH_MANGLE(entry_t);

struct TSFLEXHASH_MANGLE(entry) {
    TSFLEXHASH_ENTRY_T *next;
    TSFLEXHASH_KEY_TYPE key;
    
    union {
	TSFLEXHASH_VAL_TYPE obj;
#ifdef TSFLEXHASH_VAL_SIZE
	// if the user wants more space for each object, this pads the entry to
	// accomodate that
	char bytes[TSFLEXHASH_VAL_SIZE];
#endif /* TSFLEXHASH_VAL_SIZE */
    } val;
    
#ifdef TSFLEXHASH_REHASH
    // quotient is for rehash, when rehash doubles the size of capacity,
    // quotient will be used to direct what is the new location for an entry.
    uint32_t quotient;
#endif /* TSFLEXHASH_REHASH */
};

// we use the lowest bit of entry->next as a boolean to indicate whether
// the entry is valid.
#define TSFLEXHASH_IS_VALID(entry) (((uintptr_t) (entry)->next) & 0x1)
#define TSFLEXHASH_NEXT(entry) (__REINTERPRET_CAST(TSFLEXHASH_ENTRY_T *, (((uintptr_t) (entry)->next) & \
	    (~((uintptr_t) 0x1)))))
#define TSFLEXHASH_MARK_VALID(entry) ((entry)->next = \
	__REINTERPRET_CAST(TSFLEXHASH_ENTRY_T *, (__REINTERPRET_CAST(uintptr_t, (entry)->next) | 0x1)))
#define TSFLEXHASH_MARK_INVALID(entry) ((entry)->next = \
	__REINTERPRET_CAST(TSFLEXHASH_ENTRY_T *, (__REINTERPRET_CAST(uintptr_t, (entry)->next) & (~((uintptr_t) 0x1)))))

typedef struct TSFLEXHASH_NAME {
    uint32_t mask;
    uint32_t size;
    // pointer to the head of a linked-list of unused entry objects
    TSFLEXHASH_ENTRY_T *pool;
    // pointer values used by replenish_pool to malloc new entries and chain
    // them together
    const size_t entry_size, next_offset;
    // for debugging purposes. a string literal of TSFLEXHASH_NAME
    const char *name;
    void *(*malloc_function)(size_t);
    void (*free_function)(void *);
    // head of a linked list of overflow entry blocks. used by destroy()
    void *entry_blocks;
    // an array of entries. create() allocates the correct amount of space for
    // these (i.e. not 0)
    uint32_t collision_count;
    uint32_t capacity_bit_count;
    uint64_t chain_link_traversal_count;
#ifdef TSFLEXHASH_REHASH
    TSFLEXHASH_ENTRY_T *entries;    
#else
    TSFLEXHASH_ENTRY_T entries[0];
#endif /* TSFLEXHASH_REHASH */

}
// this will break create()
//__attribute__((aligned(TSFLEXHASH_ENTRY_ALIGNMENT)))
TSFLEXHASH_T;

typedef void*
(*(TSFLEXHASH_MANGLE(create_func)))(size_t,
                                    size_t,
                                    uint32_t, 
                                    const char *,
                                    void *(*)(size_t), 
                                    void (*)(void *));

static inline __attribute__((__always_inline__)) uint32_t
TSFLEXHASH_MANGLE(capacity)(TSFLEXHASH_T *hashtable)
{
    return hashtable->mask + 1;
}

static inline __attribute__((__always_inline__)) uint32_t
TSFLEXHASH_MANGLE(size)(TSFLEXHASH_T *hashtable)
{
    return hashtable->size;
}

static inline __attribute__((__always_inline__)) double
TSFLEXHASH_MANGLE(load_factor)(TSFLEXHASH_T *hashtable)
{
    return ((double) hashtable->size) / (hashtable->mask + 1);
}

static inline __attribute__((__always_inline__)) TSFLEXHASH_T *
#ifdef TSFLEXHASH_CAPACITY
TSFLEXHASH_MANGLE(create)(void)
#else
TSFLEXHASH_MANGLE(create)(uint32_t capacity)
#endif /* TSFLEXHASH_CAPACITY */
{
    TSFLEXHASH_MANGLE(create_func) create_func = NULL;
#ifdef TSFLEXHASH_REHASH    
    create_func = tsflexhash_create_rehash;
#else 
    create_func = tsflexhash_create_norehash;
#endif /* TSFLEXHASH_REHASH */
    
    return (TSFLEXHASH_T *) create_func(sizeof(TSFLEXHASH_ENTRY_T),
	    offsetof(TSFLEXHASH_ENTRY_T, next),
#ifdef TSFLEXHASH_CAPACITY
	TSFLEXHASH_CAPACITY,
#else
	capacity,
#endif /* TSFLEXHASH_CAPACITY */
	TSFLEXHASH_NAME_LITERAL,
	TSFLEXHASH_MALLOC,
	TSFLEXHASH_FREE
	);
}

static inline __attribute__((__always_inline__)) void
TSFLEXHASH_MANGLE(destroy)(TSFLEXHASH_T *hashtable)
{
#ifdef TSFLEXHASH_REHASH
    tsflexhash_destroy_rehash(hashtable);
#else
    tsflexhash_destroy_norehash(hashtable);
#endif /* TSFLEXHASH_REHASH */
}

static inline __attribute__((__always_inline__)) TSFLEXHASH_RETURN_TYPE
TSFLEXHASH_MANGLE(get)(TSFLEXHASH_T *hashtable,
	TSFLEXHASH_GET_KEY_QUALIFIER TSFLEXHASH_KEY_TYPE key)
{
    // index into the entry array
    TSFLEXHASH_ENTRY_T *entry = TSFLEXHASH_ENTRY(hashtable, key);
    for (;;) {
	if (__predict_true(TSFLEXHASH_IS_VALID(entry) &&
		    TSFLEXHASH_KEY_EQUALS(entry->key, key)))
	{
	    // keys match. return this entry's value
	    return TSFLEXHASH_RETURN_VALUE(entry);
	}
	// keys don't match. there was a collision. go to the next entry in the
	// chain
#ifdef TSFLEXHASH_COUNT_COLLISION
        hashtable->chain_link_traversal_count++;
#endif /* TSFLEXHASH_COUNT_COLLISION */
	entry = TSFLEXHASH_NEXT(entry);
	if (__predict_true(entry == NULL)) {
	    // no more entries. the key does not exist in the hashtable
	    return TSFLEXHASH_EMPTY_RETURN_VAL;
	}
    }
}

#ifdef TSFLEXHASH_REHASH
# define MAX_HASH_CAPACITY (1<<30)
static inline __attribute__((__always_inline__))
void
TSFLEXHASH_MANGLE(rehash)(TSFLEXHASH_T *hashtable)
{
    uint32_t capacity = hashtable->mask + 1;
    uint32_t new_capacity = capacity << 1;
    if (new_capacity > MAX_HASH_CAPACITY || new_capacity == 0) {
        return;
    }    
    
    TSFLEXHASH_ENTRY_T *entry_table = __STATIC_CAST(TSFLEXHASH_ENTRY_T *, hashtable->malloc_function(new_capacity * hashtable->entry_size));
    if (entry_table == NULL) {
        return;
    }
# if TSFLEXHASH_COUNT_COLLISION
    hashtable->chain_link_traversal_count = 0;
    hashtable->collision_count = 0;
# endif /* TSFLEXHASH_COUNT_COLLISION */
    
    for (uint32_t i = 0; i < capacity; i++) {
        TSFLEXHASH_ENTRY_T *entry = &hashtable->entries[i];
        if (TSFLEXHASH_IS_VALID(entry) == 0) {
            continue;
        }
        
        while (entry != NULL) {
            TSFLEXHASH_ENTRY_T *next_entry = TSFLEXHASH_NEXT(entry);
            TSFLEXHASH_ENTRY_T *new_entry = &entry_table[i];
            if ((entry->quotient & 0x01) != 0) {
                new_entry = &entry_table[i + capacity];
            }
            
            entry->quotient >>= 1;
            
            if (TSFLEXHASH_IS_VALID(new_entry) == 0) {
                new_entry->key = entry->key;
                new_entry->quotient = entry->quotient;
# ifdef TSFLEXHASH_VAL_SIZE
                memcpy(&new_entry->val, &entry->val, sizeof(entry->val));
# else
                new_entry->val = entry->val;
# endif /* TSFLEXHASH_VAL_SIZE */
            } else {
                TSFLEXHASH_ENTRY_T *next_new_entry = TSFLEXHASH_NEXT(new_entry);
                while (next_new_entry != NULL) {
                    new_entry = next_new_entry;
                    next_new_entry = TSFLEXHASH_NEXT(new_entry);
                }
                new_entry->next = entry;
                entry->next = NULL;
                TSFLEXHASH_MARK_VALID(entry);
# if TSFLEXHASH_COUNT_COLLISION
                hashtable->collision_count++;
# endif /* TSFLEXHASH_COUNT_COLLISION */
            }
            TSFLEXHASH_MARK_VALID(new_entry);
            entry = next_entry;
        }
    }
    
    hashtable->free_function(hashtable->entries);
    hashtable->entries = entry_table;
    hashtable->mask = new_capacity - 1;
    hashtable->capacity_bit_count++;
}
#endif /* TSFLEXHASH_REHASH */

static inline __attribute__((__always_inline__))
#if TSFLEXHASH_COPY_VAL
void
TSFLEXHASH_MANGLE(insert)(TSFLEXHASH_T *hashtable, TSFLEXHASH_KEY_TYPE key,
	TSFLEXHASH_VAL_TYPE val)
#else
TSFLEXHASH_RETURN_TYPE
TSFLEXHASH_MANGLE(insert)(TSFLEXHASH_T *hashtable, TSFLEXHASH_KEY_TYPE key)
#endif /* TSFLEXHASH_COPY_VAL */
{
    
#ifdef TSFLEXHASH_AUTO_REHASH
    // Check if 112% full for the current table.
    uint32_t hash_capacity = hashtable->mask + 1;
    if (hashtable->size > (hash_capacity >> 3) + hash_capacity) {
        TSFLEXHASH_MANGLE(rehash)(hashtable);
    }
#endif /* TSFLEXHASH_AUTO_REHASH */
    
    // compute the hash function.
#ifdef TSFLEXHASH_KEY_HASH
    uint32_t key_hash = ((uint32_t)TSFLEXHASH_KEY_HASH(key));
#else // TSFLEXHASH_KEY_HASH_EXT
    uint32_t key_hash = ((uint32_t)TSFLEXHASH_KEY_HASH_EXT(hashtable, key));
#endif /* TSFLEXHASH_KEY_HASH */
    
    // index into the array    
    TSFLEXHASH_ENTRY_T *entry = &hashtable->entries[key_hash & ((uint32_t)TSFLEXHASH_MASK(hashtable))];
    if (__predict_false(TSFLEXHASH_IS_VALID(entry))) {
	// the entry contains a value (collision). so grab a free entry from the
	// pool and insert it into the chain (at the head, since that's faster)
	TSFLEXHASH_ENTRY_T *new_entry = hashtable->pool;
	if (__predict_false(new_entry == TSFLEXHASH_SENTINEL)) {
	    // oops, the pool was empty. malloc a whole bunch of new entries.
	    // this is slow.
#ifdef TSFLEXHASH_REHASH
            new_entry = ((TSFLEXHASH_ENTRY_T *) tsflexhash_replenish_pool_rehash(
			hashtable, false));
#else
	    new_entry = ((TSFLEXHASH_ENTRY_T *) tsflexhash_replenish_pool_norehash(
			hashtable, false));
#endif /* TSFLEXHASH_REHASH */
	}
	if (new_entry->next == NULL) {
	    hashtable->pool = new_entry + 1;
	} else {
	    hashtable->pool = new_entry->next;
	}
	new_entry->next = entry->next;
	entry->next = new_entry;
	TSFLEXHASH_MARK_VALID(entry);
	entry = new_entry;
#if TSFLEXHASH_COUNT_COLLISION
        hashtable->collision_count++;
#endif /* TSFLEXHASH_COUNT_COLLISION */
    } // else { (key == 0) means the entry is empty }
    entry->key = key;
#ifdef TSFLEXHASH_REHASH
    uint32_t quotient = (key_hash >> (hashtable->capacity_bit_count));
    entry->quotient = quotient;
#endif /* TSFLEXHASH_REHASH */
    TSFLEXHASH_MARK_VALID(entry);
#if TSFLEXHASH_UPDATE_SIZE
    hashtable->size++;
#endif /* TSFLEXHASH_UPDATE_SIZE */
#if TSFLEXHASH_COPY_VAL
    entry->val.obj = val;
#else
    return TSFLEXHASH_RETURN_VALUE(entry);
#endif /* TSFLEXHASH_COPY_VAL */
}

static inline __attribute__((__always_inline__))
#if TSFLEXHASH_USE_ERASE
    void
TSFLEXHASH_MANGLE(erase)(TSFLEXHASH_T *hashtable,
#else
    TSFLEXHASH_RETURN_TYPE
TSFLEXHASH_MANGLE(remove)(TSFLEXHASH_T *hashtable,
#endif /* TSFLEXHASH_USE_ERASE */
	TSFLEXHASH_GET_KEY_QUALIFIER TSFLEXHASH_KEY_TYPE key
#if TSFLEXHASH_REMOVE_RETURNS_KEY
, TSFLEXHASH_KEY_TYPE *pkey
#endif /* TSFLEXHASH_REMOVE_RETURNS_KEY */
)
{
    TSFLEXHASH_ENTRY_T *entry, *alt;
    // index into the array
    entry = TSFLEXHASH_ENTRY(hashtable, key);
    if (__predict_true(TSFLEXHASH_IS_VALID(entry) &&
		TSFLEXHASH_KEY_EQUALS(entry->key, key)))
    {
	// keys matched. return this entry
#if TSFLEXHASH_UPDATE_SIZE
	hashtable->size--;
#endif /* TSFLEXHASH_UPDATE_SIZE */
#if TSFLEXHASH_REMOVE_RETURNS_KEY
	if (pkey != NULL) {
	    *pkey = entry->key;
	}
#endif /* TSFLEXHASH_REMOVE_RETURNS_KEY */
#if TSFLEXHASH_COPY_VAL
	alt = TSFLEXHASH_NEXT(entry);
	if (__predict_true(alt == NULL)) {
	    TSFLEXHASH_MARK_INVALID(entry);
# if TSFLEXHASH_USE_ERASE
            TSFLEXHASH_ZERO_ENTRY(entry);
	    return;
# else
	    return TSFLEXHASH_RETURN_VALUE(entry);
# endif /* TSFLEXHASH_USE_ERASE */
	}
# if !TSFLEXHASH_USE_ERASE
	TSFLEXHASH_RETURN_TYPE ret = TSFLEXHASH_RETURN_VALUE(entry);
# endif /* TSFLEXHASH_USE_ERASE */
	entry->next = alt->next;
	entry->key = alt->key;
	entry->val.obj = alt->val.obj;
	alt->next = hashtable->pool;
	hashtable->pool = alt;
# if TSFLEXHASH_USE_ERASE
        return;
# else
	return ret;
# endif /* TSFLEXHASH_USE_ERASE */
#else
	TSFLEXHASH_MARK_INVALID(entry);
# if TSFLEXHASH_USE_ERASE
        TSFLEXHASH_ZERO_ENTRY(entry);
	return;
# else
	return TSFLEXHASH_RETURN_VALUE(entry);
# endif /* TSFLEXHASH_USE_ERASE */
#endif /* TSFLEXHASH_COPY_VAL */
    }
    // keys didn't match (collision). follow the chain, just as with get()
    for (;;) {
	alt = entry;
	entry = TSFLEXHASH_NEXT(entry);
	if (__predict_false(entry == NULL)) {
#if TSFLEXHASH_USE_ERASE
            return;
#else
	    return TSFLEXHASH_EMPTY_RETURN_VAL;
#endif /* TSFLEXHASH_USE_ERASE */
	}
	if (__predict_true(TSFLEXHASH_KEY_EQUALS(entry->key, key))) {
	    if (! TSFLEXHASH_IS_VALID(alt)) {
		TSFLEXHASH_MARK_INVALID(entry);
	    }
	    alt->next = entry->next;
	    entry->next = hashtable->pool;
	    hashtable->pool = entry;
#if TSFLEXHASH_UPDATE_SIZE
	    hashtable->size--;
#endif /* TSFLEXHASH_UPDATE_SIZE */
#if TSFLEXHASH_REMOVE_RETURNS_KEY
	    if (pkey != NULL) {
		*pkey = entry->key;
	    }
#endif /* TSFLEXHASH_REMOVE_RETURNS_KEY */
#if TSFLEXHASH_USE_ERASE
            TSFLEXHASH_ZERO_ENTRY(entry);
            return;
#else
	    return TSFLEXHASH_RETURN_VALUE(entry);
#endif /* TSFLEXHASH_USE_ERASE */
	}
    }
}

static inline __attribute__((__always_inline__)) void
TSFLEXHASH_MANGLE(clear)(TSFLEXHASH_T *hashtable)
{
    TSFLEXHASH_ENTRY_T *entry, *a, *b;
    const uint32_t mask = TSFLEXHASH_MASK(hashtable);
    // pathscale barfs on a normal for() loop. but this seems to work...
    uint32_t idx = 0;
    while (idx <= mask) {
	entry = &hashtable->entries[idx];
	a = TSFLEXHASH_NEXT(entry);
	while (a != NULL) {
	    b = TSFLEXHASH_NEXT(a);
	    a->next = hashtable->pool;
	    hashtable->pool = a;
	    a = b;
	}
	entry->next = NULL;
	idx++;
    }
#if TSFLEXHASH_COUNT_COLLISION
    hashtable->collision_count = 0;
    hashtable->chain_link_traversal_count = 0;
#endif /* TSFLEXHASH_COUNT_COLLISION */
#if TSFLEXHASH_UPDATE_SIZE
    hashtable->size = 0;
#endif /* TSFLEXHASH_UPDATE_SIZE */
}

#define TSFLEXHASH_ITERATOR_T TSFLEXHASH_MANGLE(iterator_t)

typedef struct {
    TSFLEXHASH_ENTRY_T *entry;
    uint32_t idx;
    TSFLEXHASH_ENTRY_T dummy_entry;
} TSFLEXHASH_ITERATOR_T;

static inline __attribute__((__always_inline__)) void
TSFLEXHASH_MANGLE(iterator_init)(TSFLEXHASH_T *hashtable,
	TSFLEXHASH_ITERATOR_T *i)
{
    __USE(hashtable);
    i->entry = &i->dummy_entry;
    i->idx = -1;
    i->dummy_entry.next = NULL;
}

static inline __attribute__((__always_inline__)) TSFLEXHASH_RETURN_TYPE
TSFLEXHASH_MANGLE(iterator_next)(TSFLEXHASH_T *hashtable,
	TSFLEXHASH_ITERATOR_T *i, TSFLEXHASH_KEY_TYPE *key)
{
    TSFLEXHASH_ENTRY_T *entry = i->entry;
    for (;;) {
	/* coverity[deref_ptr] */
	entry = TSFLEXHASH_NEXT(entry);
	/* coverity[check_after_deref] */
	if (entry != NULL) {
	    break;
	}
	if (__predict_false(i->idx == TSFLEXHASH_MASK(hashtable))) {
	    return TSFLEXHASH_EMPTY_RETURN_VAL;
	}
	(i->idx)++;
	// pathscale has some weird bug that causes entry to get set to a
	// crazy value without this little disruption.
#ifdef __PATHSCALE__
	compiler_fence();
#endif /* __PATHSCALE__ */
	entry = &hashtable->entries[i->idx];
	/* coverity[deref_ptr] */
	if (TSFLEXHASH_IS_VALID(entry)) {
	    break;
	}
    }
    i->entry = entry;
    if (key != NULL) {
	*key = entry->key;
    }
    return TSFLEXHASH_RETURN_VALUE(entry);
}

#undef TSFLEXHASH_NAME
#undef TSFLEXHASH_STRING_KEYS
#undef TSFLEXHASH_CONST_STRING_KEYS
#undef TSFLEXHASH_KEY_CONST
#undef TSFLEXHASH_GET_KEY_QUALIFIER
#undef TSFLEXHASH_KEY_TYPE
#undef TSFLEXHASH_KEY_EQUALS
#undef TSFLEXHASH_KEY_HASH
#undef TSFLEXHASH_KEY_HASH_EXT
#undef TSFLEXHASH_REMOVE_RETURNS_KEY
#undef TSFLEXHASH_USE_ERASE
#undef TSFLEXHASH_VAL_TYPE
#undef TSFLEXHASH_VAL_SIZE
#undef TSFLEXHASH_CAPACITY
#undef TSFLEXHASH_MASK
#undef TSFLEXHASH_COPY_VAL
#undef TSFLEXHASH_EMPTY_RETURN_VAL
#undef TSFLEXHASH_COUNT_COLLISION
#undef TSFLEXHASH_UPDATE_SIZE
#undef TSFLEXHASH_MALLOC
#undef TSFLEXHASH_FREE
#undef TSFLEXHASH_NUMA

#undef TSFLEXHASH_NAME_LITERAL1
#undef TSFLEXHASH_NAME_LITERAL0
#undef TSFLEXHASH_NAME_LITERAL

#undef TSFLEXHASH_MANGLE1
#undef TSFLEXHASH_MANGLE0
#undef TSFLEXHASH_MANGLE

#undef TSFLEXHASH_ENTRY
#undef TSFLEXHASH_ENTRY_T
#undef TSFLEXHASH_T

#undef TSFLEXHASH_RETURN_VALUE
#undef TSFLEXHASH_RETURN_TYPE

#undef TSFLEXHASH_NEXT
#undef TSFLEXHASH_MARK_VALID
#undef TSFLEXHASH_MARK_INVALID
#undef TSFLEXHASH_ITERATOR_T

#undef TSFLEXHASH_AUTO_REHASH

__END_DECLS
