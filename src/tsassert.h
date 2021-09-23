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

#ifndef _TSASSERT_H_
#define _TSASSERT_H_

#include <assert.h>
#include <stdbool.h>
__BEGIN_DECLS

__attribute__((__noreturn__,__format__(__printf__, 5, 6)))
void tsassert_fail(const char *, const char *, unsigned int, const char *, const char *, ...);

__END_DECLS

// like assert(), but isn't dependent on NDEBUG. i.e. it always asserts, even
// if NDEBUG is defined. this is useful when your test condition has side
// effects, e.g. see tsassert_pthread_mutex_lock below.
#define tsassert(x) \
    do { \
	if (__predict_false(! (x))) \
	    tsassert_fail(__STRING(x), __FILE__, __LINE__, __func__, NULL); /* NOLINT */ \
    }  while (false)

#define tsassert_format(x, fmt, ...) \
    do { \
	if (__predict_false(! (x))) \
	    tsassert_fail(__STRING(x), __FILE__, __LINE__, __func__, fmt, ## __VA_ARGS__); /*NOLINT*/ \
    }  while (false)

#define tsassert_pthread_mutex_lock(x) tsassert(pthread_mutex_lock(x) == 0)
#define tsassert_pthread_mutex_unlock(x) tsassert(pthread_mutex_unlock(x) == 0)

/** Compile time failure if a structure is not sized correctly */
#define size_check(t,size) static_assert(sizeof(t) == size, "Incorrect size of '" #t "'")

#define memcpy_buf(dst, src) \
do { \
    static_assert(sizeof(dst) >= sizeof(src), ""); \
    memcpy(dst, src, sizeof(src)); \
}  while (false)

#define memcpy_buf_exact(dst, src) \
do { \
    static_assert(sizeof(dst) == sizeof(src), ""); \
    memcpy(dst, src, sizeof(src)); \
}  while (false)

#endif
