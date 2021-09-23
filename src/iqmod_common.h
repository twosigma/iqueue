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
#ifndef _iqmod_common_h_
#define _iqmod_common_h_

#include "twosigma.h"
#include "tsassert.h"

#include <errno.h>
#include <err.h>

typedef struct {
    uint8_t table;
    uint64_t key;
} shash_coord_t;

static uint64_t *
parse_tuple(size_t min_tokens, size_t max_tokens, const char *str, size_t *found_tokens, uint64_t default_value)
{
    uint64_t *list = malloc(max_tokens * sizeof(*list));
    tsassert(list);
    char *buff = strdup(str);
    tsassert(buff);

    *found_tokens = 0;
    char *token;
    char *saveptr = NULL;
    while ((token = strtok_r(buff, ":", &saveptr))) {
	if (*found_tokens >= max_tokens) {
            errx(1, "Expected at most `%zu` items in `%s`, found `%zu`", max_tokens, str, *found_tokens+1);
	}
	buff = NULL;
	errno = 0;
	char *endptr;
	list[(*found_tokens)++] = strtoul(token, &endptr, 0);
        if (errno) {
            err(1, "Strtol failed parsing argument `%s`", str);
        }
	if (*endptr != '\0') {
	    errx(1, "Strtol found text at the end of a number in `%s`", str);
	}
    }

    if (*found_tokens < min_tokens) {
        errx(1, "Expected at least `%zu` items in `%s`, found `%zu`", min_tokens, str, *found_tokens);
    }

    for (size_t i = *found_tokens; i < max_tokens; ++i) {
	list[i] = default_value;
    }

    return list;
}
#endif
