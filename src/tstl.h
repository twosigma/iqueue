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

/*
 * Simplified thread-local storage for pthreads. see testsrc/tstltest.c for an example
 */

#ifndef _TSTL_H_
#define _TSTL_H_

#include <stdint.h>
#ifdef _REENTRANT
#include <pthread.h>
#endif

/*
 * if true, the common case in tstl_get will be inlined for better performance. this should
 * only be set to 0 for testing purposes.
 */
#define TSTL_INLINE_GET 1

__BEGIN_DECLS

/**
 * A thread-local variable. Initialize variables of this type using one of the two macros below.
 */
typedef struct _tstl tstl_t;

/**
 * Use this macro to init a threadlocal of any type (struct, int, string, whatever). Just supply
 * a create_function and an optional argument, and optionally provide a destroy_function that will
 * be called when the thread dies.
 */
#ifdef _REENTRANT
#define TSTL_MUTEX_INITIALIZER .mutex = PTHREAD_MUTEX_INITIALIZER,
#else
#define TSTL_MUTEX_INITIALIZER .p = NULL,
#endif
#define TSTL_INITIALIZER(cf, df, ca) \
	{ \
	    .create_function = cf, \
	    .destroy_function = df, \
	    .create_arg = ca, \
	    .initialized = 0, \
	    TSTL_MUTEX_INITIALIZER \
	}

/**
 * Use this simpler macro to init a threadlocal for a simple buffer (e.g. char[]). Just supply the
 * size, in bytes.
 */
#define TSTL_BUF_INITIALIZER(size) TSTL_INITIALIZER(_tstl_buf_create, free, (void *) size)

/**
 * Get the value of a tstl_t variable.  This will cause the create_function to be invoked
 * if it is the first time you're getting the value on the current thread.
 */
static inline __attribute__((__always_inline__)) void *tstl_get(tstl_t *);

/**
 * A "round robin" thread-local type. This is like a tstl_t that keeps multiple values for each thread and returns
 * them in round-robin order. This is useful for char buffers or other objects where you might want to use several
 * instances simulatenously on a given thread.
 */
typedef struct _tstl_rr tstl_rr_t;

void * tstl_rr_get(tstl_rr_t *rr);

void *_tstl_rr_inner_create(void *);
void _tstl_rr_inner_free(void *);

#define TSTL_RR_INITIALIZER(cf, df, ca, count) \
	{ \
	    .create_function = cf, \
	    .destroy_function = df, \
	    .create_arg = ca, \
	    .tstl = TSTL_INITIALIZER(_tstl_rr_inner_create, _tstl_rr_inner_free, (void *) count) \
	}

#define TSTL_RR_BUF_INITIALIZER(size, count) TSTL_RR_INITIALIZER(_tstl_buf_create, free, (void *) size, count)

// no user-servicable parts below this line

struct _tstl {
    void *(*create_function)(void *);
    void (*destroy_function)(void *);
    void *create_arg;
    volatile int initialized;
#ifdef _REENTRANT
    volatile pthread_key_t key;
    pthread_mutex_t mutex;
#else
    void *p;
#endif
};

struct _tstl_rr {
    void *(*create_function)(void *);
    void (*destroy_function)(void *);
    void *create_arg;
    tstl_t tstl;
};

void *_tstl_get(tstl_t *tstl);

static inline __attribute__((__always_inline__)) void *
tstl_get(tstl_t *tstl)
{
#if TSTL_INLINE_GET
    if (__predict_true(tstl->initialized)) {
	compiler_fence();
#ifdef _REENTRANT
	void *p = pthread_getspecific(tstl->key);
	if (__predict_true(p != NULL)) {
	    return p;
	}
#else
	return tstl->p;
#endif
    }
#endif
    return _tstl_get(tstl);
}

void *_tstl_buf_create(void *);

char *tstl_strerror(int);

__END_DECLS

#endif
