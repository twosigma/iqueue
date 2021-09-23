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
 * Private functions used by tsflexhash.h. Users of the tslfexhash library
 * should not call these functions directly.
 */
#ifndef _TSFLEXHASH_PRIVATE_H_
#define _TSFLEXHASH_PRIVATE_H_

#include <stdint.h>
#include <stdbool.h>

__BEGIN_DECLS

#define TSFLEXHASH_SENTINEL ((void *) ~ ((uintptr_t) 0))

void *tsflexhash_create_norehash(size_t, size_t, uint32_t, const char *,
	void *(*)(size_t), void (*)(void *));

void *tsflexhash_create_rehash(size_t, size_t, uint32_t, const char *,
	void *(*)(size_t), void (*)(void *));

void tsflexhash_destroy_norehash(void *);

void tsflexhash_destroy_rehash(void *);

void *tsflexhash_replenish_pool_norehash(void *, bool);

void *tsflexhash_replenish_pool_rehash(void *, bool);

void *tsflexhash_malloc(size_t);

void tsflexhash_free(void *);

__END_DECLS

#endif
