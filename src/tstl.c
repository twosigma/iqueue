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
#include <pthread.h>
#include <errno.h>
#include <assert.h>
#include <err.h>
#include <emmintrin.h>
#include "tslog.h"
#include "tstl.h"

void *
_tstl_get(tstl_t *tstl)
{
#ifdef _REENTRANT
    int rc;
    /* Multi-threaded version */
    if (__predict_false(!tstl->initialized)) {
	if ((rc = pthread_mutex_lock(&tstl->mutex))) {
	    errno = rc;
	    err(1, "pthread_mutex_lock failed");
	}

	if (!tstl->initialized) {
	    rc = pthread_key_create(
		__UNVOLATILE_T(pthread_key_t *, &tstl->key),
		tstl->destroy_function);

	    if (rc) {
		errno = rc;
		err(1, "pthread_key_create failed");
	    }

	    _mm_mfence();

	    tstl->initialized = 1;
	}

	_mm_mfence();

	if ((rc = pthread_mutex_unlock(&tstl->mutex))) {
	    errno = rc;
	    err(1, "pthread_mutex_unlock failed");
	}
    }

    void *p = pthread_getspecific(tstl->key);
    if (p)
	return p;

    p = (*tstl->create_function)(tstl->create_arg);
    assert(p != NULL);
    if ((rc = pthread_setspecific(tstl->key, p))) {
	errno = rc;
	err(1, "pthread_setspecific failed");
    }

    return p;
#else
    /* Single threaded version */
    if (tstl->p == NULL) {
	void *p = (*tstl->create_function)(tstl->create_arg);
	assert(p != NULL);
	tstl->p = p;
    }
    tstl->initialized = 1;
    return tstl->p;
#endif
}


void *
_tstl_buf_create(void *arg)
{
    size_t size;
    void *p;

    size = (size_t) arg;
    p = calloc(1, size);
    if (! p) {
	err(1, "calloc(1, %zu) failed", size);
	abort();
    }
    return p;
}

typedef struct {
    uint32_t count, next;
    void (*destroy_function)(void *);
    void *pointers[0];
} _tstl_rr_inner_t;

void *
_tstl_rr_inner_create(void *create_arg)
{
    size_t count = (size_t) create_arg;
    _tstl_rr_inner_t *inner = calloc(1, sizeof(*inner) + (count * sizeof(void *)));
    assert(inner != NULL);
    inner->count = count;
    return inner;
}

void
_tstl_rr_inner_free(void *arg)
{
    _tstl_rr_inner_t *inner = arg;
    uint32_t count = inner->count;
    void **pointers = inner->pointers;
    void (*destroy_function)(void *) = inner->destroy_function;
    if (destroy_function != NULL) {
	for (uint32_t i = 0; i < count; i++) {
	    void *pointer = pointers[i];
	    if (pointer != NULL) {
		destroy_function(pointer);
	    }
	}
    }
    free(inner);
}

void *
tstl_rr_get(tstl_rr_t *rr)
{
    _tstl_rr_inner_t *inner = tstl_get(&rr->tstl);
    uint32_t next = inner->next;
    void *ret = inner->pointers[next];
    if (__predict_false(ret == NULL)) {
	inner->pointers[next] = ret = rr->create_function(rr->create_arg);
	assert(ret != NULL);
	// hack alert: set inner->destroy_function here since we don't have a pointer to it in _tstl_rr_inner_create()
	inner->destroy_function = rr->destroy_function;
    }
    inner->next = (next + 1) % inner->count;
    return ret;
}

#define TSTL_STRERROR_BUF_SIZE 256
static tstl_t tstl_strerror_buf = TSTL_BUF_INITIALIZER(TSTL_STRERROR_BUF_SIZE);

char *
tstl_strerror(int errnum)
{
    char *buf = tstl_get(&tstl_strerror_buf);
    assert(buf != NULL);
    if (strerror_r(errnum, buf, TSTL_STRERROR_BUF_SIZE) == -1) {
	TSLOG(TSWARN, "strerror_r(%d, %p, %d) failed", errnum, buf, TSTL_STRERROR_BUF_SIZE);
	snprintf(buf, TSTL_STRERROR_BUF_SIZE, "error #%d", errnum);
    }
    return buf;
}
