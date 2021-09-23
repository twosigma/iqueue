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
#include "tsassert.h"

#include <errno.h>
#include <err.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include "iqueue.h"
#include "iqmod_common.h"

#define TSFLEXHASH_NAME shash_mod_table
#define TSFLEXHASH_KEY_TYPE shash_coord_t
#define TSFLEXHASH_VAL_TYPE uint64_t
#define TSFLEXHASH_KEY_HASH(key) (key.table ^ key.key)
#define TSFLEXHASH_KEY_EQUALS(k1, k2) (k1.table == k2.table && k1.key == k2.key)
#include "tsflexhash.h"

static void
print_help(char **argv)
{
    fprintf(stderr,
"\n"
"iqmod_inplace - modify an iqueue in place.\n"
"usage: %s [OPTIONS] input.iqx\n"
"\n"
"DESCRIPTION:\n"
"iqmod_inplace modifies a given iqueue's creation_time or shash entries.\n"
"\n"
"OPTIONS: (repeatable - numbers in oct, dec, or hex)\n"
"\t-h                        This help.\n"
"\t-c <creation_time>        Creation time in decimal nanos.\n"
"\t-s <table>:<key>:<value>  Override the original shash, adding if none.\n"
"\n"
"EXAMPLES:\n"
"Modify creation time:\n"
"\t iqmod_inplace -c 1500303647133731422 test.iqx\n"
"Modify shash in table 0(heartbeat table) and slot 123:\n"
"\t iqmod_inplace -s 0:123:1500305470665041605\n"
"Modify shash in table 1(iqsync source table) and slot 1500305470665041605:\n"
"\t iqmod_inplace -s 1:1500305470665041605:98 test.iqx\n"
"\n"
	, basename(argv[0]));
}

static void
mod_shashes(
    iqueue_t *input,
    shash_mod_table_t *set_map)
{
    shash_t *input_table;

    shash_mod_table_iterator_t it;
    shash_mod_table_iterator_init(set_map, &it);
    for (;;) {
	shash_coord_t key;
	uint64_t *val = shash_mod_table_iterator_next(set_map, &it, &key);
	if (!val) {
	    break;
	}
        // Now we know we need the table, create it if it's missing.
        input_table = iqueue_writer_table(input, key.table, 1);
        shash_entry_t *entry = shash_get(input_table, key.key);
        if (entry == NULL) {
            shash_insert(input_table, key.key, *val);
        } else {
            shash_update(input_table, entry, entry->value, *val);
        }
    }
}

int
main(int argc, char **argv)
{
    size_t found_tokens;
    uint64_t *token_list;

    shash_mod_table_t *shash_set_map = shash_mod_table_create(32);
    uint64_t creation_time = 0;

    int option = -1;
    while ((option = getopt(argc, argv, "hc:s:")) != -1) {
	switch (option) {
	case 'h':
	    print_help(argv);
	    return EXIT_FAILURE;
	case 'c':
	    token_list = parse_tuple(1, 1, optarg, &found_tokens, 0);
	    creation_time = token_list[0];
	    break;
	case 's':
        {
	    token_list = parse_tuple(3, 3, optarg, &found_tokens, 0);
	    shash_coord_t coord = (shash_coord_t){token_list[0], token_list[1]};
	    uint64_t *table_value = shash_mod_table_insert(shash_set_map, coord);
	    *table_value = token_list[2];
	    break;
        }
	default:
	    fprintf(stderr, "Uknown option `%c` (%d)\n", option, option);
	    print_help(argv);
	    return EXIT_FAILURE;
	}
    }

    if (optind == 1) {
	fprintf(stderr, "Expecting at least 1 editing option, but found none\n");
	print_help(argv);
	return EXIT_FAILURE;
    }

    if (argc - optind != 1) {
	fprintf(stderr, "Expecting exactly 1 non-option arguments, found `%d`\n", argc - optind);
	print_help(argv);
	return EXIT_FAILURE;
    }

    const char *input_path = argv[optind];

    iqueue_t *input = iqueue_open(input_path, true);
    if (!input) {
	err(1, "Could not open input iqx `%s`", input_path);
    }

    if (creation_time != 0) {
        if (iqueue_update_creation_time(input, creation_time) == -1) {
            err(1, "Failed to modify creation_time for `%s`", input_path);
        }
    }

    mod_shashes(input, shash_set_map);
    return EXIT_SUCCESS;
}
