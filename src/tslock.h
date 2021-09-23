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

#ifndef _TSLOCK_H_
#define _TSLOCK_H_

/** \file
 * Ticket based spin locks for C threads.
 *
 * Implements a fast, fair spinlock for threads using the
 * ticket based algorithm.
 *
 * Usually, you should just use tslock_alloc() and tslock_destroy()
 * to create and destroy lock objects, since those functions guarantee
 * that the cache lines are optimized.
 *
 * If you really want to allocate a lock in BSS (global variable), use
 * the TSLOCK_DECLARE_STATIC macro.
 *
 * If you really want to embed a lock in your own struct, use the
 * extremely_dangerous_internal_tslock_s. If you don't understand how,
 * don't do it. Inlining WILL NOT improve performance on any machines
 * in use at Two Sigma, since our machines have branch and data predictors
 * that make the extra pointer indirection free.
 */
#include <emmintrin.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#if __GNUC__ >= 5
#include <stdatomic.h>
#include <stdalign.h>
#include <stdbool.h>

#if ATOMIC_INT_LOCK_FREE != 2
#error your architecture does not support lock-free ints
#endif

#else // __GNUC__
#include "atomic.h"
#endif // __GNUC__

#include <assert.h>
#include "tslog.h"

//when this is undefined, tslock_t will become opaque, safer, and faster
#define TSLOCK_COMPAT 1

//If you redefine this, it really needs to be a power of 2.
//If it's not a power of two, BAD THINGS WILL HAPPEN
#define TSLOCK_ALIGN_SIZE 64

#ifdef TSLOCK_COMPAT
//minimum alignment for correctness
#define TSLOCK_ALIGN_CHECK_SIZE 8
#else
#define TSLOCK_ALIGN_CHECK_SIZE TSLOCK_ALIGN_SIZE
#endif

struct extremely_dangerous_internal_tslock_s
{
#if __GNUC__ >= 5
    alignas(TSLOCK_ALIGN_SIZE) atomic_uint next_in_line;
    alignas(TSLOCK_ALIGN_SIZE) atomic_uint now_serving;
#else
    volatile uint32_t next_in_line __attribute__((aligned(TSLOCK_ALIGN_SIZE)));
    volatile uint32_t now_serving __attribute__((aligned(TSLOCK_ALIGN_SIZE)));
#endif
    pthread_t holder;
};

static_assert(sizeof(struct extremely_dangerous_internal_tslock_s) >= 128, "Ensure alignment attributes worked");

#ifndef TSLOCK_COMPAT
typedef struct _donteventhinkaboutit tslock_t;
#else
typedef struct extremely_dangerous_internal_tslock_s tslock_t;
#endif

#define TSLOCK_DECLARE_STATIC(name) \
	static struct extremely_dangerous_internal_tslock_s __attribute__((aligned(TSLOCK_ALIGN_SIZE))) name##_underlying; \
	static tslock_t * const name = __STATIC_CAST(tslock_t *, __STATIC_CAST(uintptr_t, &name##_underlying));

/*
 * The lock returns by tslock_alloc can be freed with tslock_destroy()
 */
static tslock_t * __attribute__((used))
tslock_alloc(void)
{
    void * lock = NULL;

    const size_t lock_sz =
	sizeof(struct extremely_dangerous_internal_tslock_s);

    int rc = posix_memalign(
        &lock,
        TSLOCK_ALIGN_SIZE,
        lock_sz
    );
    if (rc != 0) {
        return NULL;
    }

#if __GNUC__ >= 5
    struct extremely_dangerous_internal_tslock_s* lock_internal = __STATIC_CAST(struct extremely_dangerous_internal_tslock_s*, lock);
    atomic_init(&lock_internal->next_in_line, 0);
    atomic_init(&lock_internal->now_serving, 0);
    lock_internal->holder = 0;
#else
    memset(lock, 0, lock_sz);
#endif

    return __STATIC_CAST(tslock_t *, lock);
}

static int tslock_complained_loudly;

static inline void
tslock_alignment_check(
    void * lock,
    const char * const file,
    const int line
)
{
    if (unlikely(((uintptr_t) lock & (TSLOCK_ALIGN_CHECK_SIZE-1)))) {
	TSABORTX(
	    "%s:%d: tslock_t is not properly aligned. "
	    "Please use tslock_alloc() or alignas(TSLOCK_ALIGN_SIZE) or "
	    "else your performance could greatly suffer. "
	    "Also, the way this lock is currently being used doesn't "
	    "guarantee that it actually behaves like a lock.",
	    file, line);
    }

#ifdef TSLOCK_COMPAT
    if (!tslock_complained_loudly && unlikely(((uintptr_t) lock & (TSLOCK_ALIGN_SIZE-1)))) {
	TSLOGX(TSERROR,
	    "%s:%d: You are not using the new tslock_t api. "
	    "Please use tslock_alloc() or alignas(TSLOCK_ALIGN_SIZE) or "
	    "else your performance could greatly suffer. "
	    "The old api will be deprecated soon.",
	    file, line);
	tslock_complained_loudly = 1;
    }
#endif
}


static inline void
_tslock(
    tslock_t * lock_ptr,
    const char * const file,
    const int line
)
{
    struct extremely_dangerous_internal_tslock_s * lock =
        __PUNNED_CAST(struct extremely_dangerous_internal_tslock_s *, lock_ptr);

    tslock_alignment_check(lock, file, line);

    const pthread_t self = pthread_self();

#if __GNUC__ >= 5
    const uint32_t ticket = atomic_fetch_add(&lock->next_in_line, 1);
#else
    const uint32_t ticket = atomic_inc_32_nv(&lock->next_in_line) - 1;
#endif
    // Fast path for uncontested, unlocked case
#if __GNUC__ >= 5
    if (atomic_load(&lock->now_serving) == ticket)
#else
    if (lock->now_serving == ticket)
#endif
    {
        lock->holder = self;
        return;
    }

    // Check to see if the lock holder is self, in which case deadlock
    // is imminent.
    if (lock->holder == self)
	TSLOGX(TSWARN, "%s:%d: Possible deadlock: lock is held by self",
	    file, line);

    // We are locked, spin until we are ready
    uint64_t spin_count = 0;
#if __GNUC__ >= 5
    while (atomic_load(&lock->now_serving) != ticket)
#else
    while (lock->now_serving != ticket)
#endif
    {
#ifdef __PATHSCALE__
	// psc generates an infinite loop for this for some reason
	compiler_fence();
#else
	_mm_pause();
#endif
	if ((++spin_count & 0xFFFFFFF) == 0)
	    TSLOGX(TSWARN,
		"%s:%d: Possible deadlock"
		" spin_count=%"PRIu64
		" ticket=%"PRIx32
		" next_ticket=%"PRIx32
		" now_serving=%"PRIx32,
		file,
		line,
		spin_count,
		ticket,
#if __GNUC__ >= 5
		atomic_load(&lock->next_in_line),
		atomic_load(&lock->now_serving)
#else
		lock->next_in_line,
		lock->now_serving
#endif
	    );
    }

    lock->holder = self;
}

#define tslock(lock) _tslock(lock, __FILE__, __LINE__)


static inline void
_tsunlock(
    tslock_t * lock_ptr,
    const char * const file,
    const int line
)
{
    struct extremely_dangerous_internal_tslock_s * lock =
	__PUNNED_CAST(struct extremely_dangerous_internal_tslock_s *, lock_ptr);

    if (unlikely(!lock->holder))
    {
	// This will not detect all double unlocks, but hopefully
	// some of them will be found and fixed through this warning
	TSLOGX(TSERROR, "%s:%d: Double unlock detected", file, line);
	return;
    }

    lock->holder = 0;
#if __GNUC__ >= 5
    atomic_fetch_add(&lock->now_serving, 1);
#else
    lock->now_serving++;
#endif
}

#define tsunlock(lock) \
    _tsunlock((lock), __FILE__, __LINE__)

#define tstrylock(lock) \
    _tstrylock((lock), __FILE__, __LINE__)

static inline int
_tstrylock(
    tslock_t * lock_ptr,
    const char * const file,
    const int line
)
{
    struct extremely_dangerous_internal_tslock_s * lock =
	__PUNNED_CAST(struct extremely_dangerous_internal_tslock_s *, lock_ptr);

    tslock_alignment_check(lock, file, line);

#if __GNUC__ >= 5
    const uint32_t ticket = atomic_load(&lock->next_in_line);

    // If it is already locked, we can't win
    if (atomic_load(&lock->now_serving) != ticket)
        return 0;

    // If it hasn't changed, then we might be able to get it
    // if the same ticket is still there
    uint32_t existing_ticket = ticket;
    if (atomic_compare_exchange_strong(&lock->next_in_line, &existing_ticket, ticket+1) == false)
        return 0;
#else
    const uint32_t ticket = lock->next_in_line;

    // If it is already locked, we can't win
    if (lock->now_serving != ticket)
        return 0;

    // If it hasn't changed, then we might be able to get it
    // if the same ticket is still there
    if (atomic_cas_32(&lock->next_in_line, ticket, ticket+1) != ticket)
        return 0;
#endif

    // We have it.  Record our ownership
    lock->holder = pthread_self();
    return 1;
}


/* Check if the lock is currently held.
 *
 * The values must be read into locals to avoid undefined behaviour
 * as to which volatile will be read first.
 */
static inline int
tsislocked(
    tslock_t * lock_ptr
)
{
    struct extremely_dangerous_internal_tslock_s * lock =
        __PUNNED_CAST(struct extremely_dangerous_internal_tslock_s *, lock_ptr);

#if __GNUC__ >= 5
    const uint32_t now_serving = atomic_load(&lock->now_serving);
    const uint32_t next_in_line = atomic_load(&lock->next_in_line);
#else
    const uint32_t now_serving = lock->now_serving;
    const uint32_t next_in_line = lock->next_in_line;
#endif

    return now_serving != next_in_line;
}



/* Check to see if any other threads are waiting on the lock.
 *
 * Must be called with the lock held, otherwise the now_serving
 * value might change unexpectedly.  The values must be read into
 * locals to avoid undefined behaviour as to which volatile will
 * be read first.
 */
static inline int
tslock_contended(
    const tslock_t * const lock_ptr
)
{
    struct extremely_dangerous_internal_tslock_s * lock =
        __PUNNED_CAST(struct extremely_dangerous_internal_tslock_s *, lock_ptr);

#if __GNUC__ >= 5
    const uint32_t now_serving = atomic_load(&lock->now_serving);
    const uint32_t next_in_line = atomic_load(&lock->next_in_line);
#else
    const uint32_t now_serving = lock->now_serving;
    const uint32_t next_in_line = lock->next_in_line;
#endif

    return now_serving + 1 != next_in_line;
}

static void __attribute__((used)) tslock_destroy(
    tslock_t * lock_ptr
)
{
    if (!lock_ptr)
        return;

    struct extremely_dangerous_internal_tslock_s * lock =
        __PUNNED_CAST(struct extremely_dangerous_internal_tslock_s *, lock_ptr);

    int okay = !tsislocked(lock_ptr) || lock->holder == pthread_self();

    if (!okay) {
        TSLOGX(TSERROR,
            "You freed a lock while it was held by someone else. "
            "Expect breakage.");
    }

    free(lock_ptr);
}

#endif
