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
#ifndef _TEST_RESOURCE_H
#define _TEST_RESOURCE_H

#include <sys/types.h>

typedef struct ts_test_resource {
    void *data;
    void (*cleanup)(void *);
    struct ts_test_resource *next;
} ts_test_resource_t;

__BEGIN_DECLS
extern ts_test_resource_t *test_resources;
extern ts_test_resource_t *test_resource;

#define TS_ON_DESTROY(obj, func) \
    signal_handler_install(); \
    ts_test_resource_t *new_resource = calloc(1, sizeof(ts_test_resource_t)); \
    new_resource->data = obj; \
    new_resource->cleanup = func;\
    new_resource->next = test_resources; \
    test_resources = new_resource; \

char * ts_test_resource_get_dir(const char *);
void ts_test_local_resource_clean_up(void);
void signal_handler_install(void);
__END_DECLS
#endif
