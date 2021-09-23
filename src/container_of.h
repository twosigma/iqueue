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
#ifndef _CONTAINER_OF_H_
#define _CONTAINER_OF_H_

#include "twosigma.h"

__BEGIN_DECLS

/**
 * container_of - cast a member of a structure out to the containing structure
 * param #1: the pointer to the member.
 * param #2: the type of the container struct this is embedded in.
 * param #3: the name of the member within the struct.
 */
#define container_of(ptr, type, member) ({                   \
    /* check the type of `member' is correct given `ptr' */  \
    const __typeof__ (__REINTERPRET_CAST(type*, 0)->member) *__mptr = (ptr); \
    /* do the actual offset-and-cast */                      \
    __REINTERPRET_CAST(type*, (__REINTERPRET_CAST(uintptr_t, __mptr) - offsetof(type,member)));})

__END_DECLS

#endif
