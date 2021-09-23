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
#include <fcntl.h>
#include <getopt.h>
#include <libgen.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "iqueue.h"
#include "iqmod_common.h"
#include "tsclock.h"

// Absolute minimum number of files that can be open at once.
#define MAX_UNWARNED_FILES _POSIX_OPEN_MAX

#define TSFLEXHASH_NAME shash_mod_table
#define TSFLEXHASH_KEY_TYPE shash_coord_t
#define TSFLEXHASH_VAL_TYPE uint64_t
#define TSFLEXHASH_KEY_HASH(key) (key.table ^ key.key)
#define TSFLEXHASH_KEY_EQUALS(k1, k2) (k1.table == k2.table && k1.key == k2.key)
#include "tsflexhash.h"

#define TSFLEXHASH_NAME msg_mod_table
#define TSFLEXHASH_KEY_TYPE uint64_t
#define TSFLEXHASH_VAL_TYPE uint64_t
#include "tsflexhash.h"

static void
print_help(char **argv)
{
    fprintf(stderr,
"\n"
"iqmod_copy - create a modified copy of an iqueue.\n"
"usage: %s [OPTIONS] input.iqx output.iqx\n"
"\n"
"DESCRIPTION:\n"
"iqmode creates a copy of a given iqueue with added, removed, or edited messages, header, or shash entries.\n"
"Messages to be edited or inserted will be dumped to a binary file, which can be edited as necessary, and\n"
"will be automatically loaded back into the new iqueue. The binary files will be stored under `<iqueue.iqx>.mod/`\n"
"\n"
"OPTIONS: (repeatable - numbers in oct, dec, or hex)\n"
"\t-h                        This help.\n"
"\t-t                        Edit the header.\n"
"\t-i <idx>[:<cnt=1>]        Insert <cnt> new messages between <idx-1> and <idx> (files <idx>-1.insert to <idx>-<cnt>.insert).\n"
"\t-d <idx>[:<cnt=1>]        Drop messages <idx> to <idx+cnt-1>.\n"
"\t-e <idx>[:<cnt=1>]        Edit messages <idx> to <idx+cnt-1>. (files <idx>.edit to <idx+cnt-1>.edit)\n"
"\t-s <table>:<key>:<value>  Override the original shash, adding if none. The value can be smaller than the input one.\n"
"\t-x <table>:<key>          Drop the original shash, if any.\n"
"\n"
"EXAMPLES:\n"
"Remove messages 10-100 inclusive, and edit header:\n"
"\t iqmod_copy -t -d 10:101\n"
"Insert 2 messages at position 10 and 11, so the output will have 0-9 + new1 + new2 + 10-last:\n"
"\t iqmod_copy -i 10:2\n"
"Edit message 307 and drop shash in table 0 and slot 123:\n"
"\t iqmod_copy -e 307 -x 0:123\n"
"\n"
	, basename(argv[0]));
}

static void
mod_shashes(
    int8_t table,
    iqueue_t *input, iqueue_t *output,
    shash_mod_table_t *set_map,
    shash_mod_table_t *drop_map)
{
    shash_t *input_table = iqueue_writer_table(input, table, 0);
    shash_t *output_table = NULL;

    // Add all the user-specified keys.
    shash_mod_table_iterator_t it;
    shash_mod_table_iterator_init(set_map, &it);
    for (;;) {
	shash_coord_t key;
	uint64_t *val = shash_mod_table_iterator_next(set_map, &it, &key);
	if (!val) {
	    break;
	}
	if (key.table != table) {
	    // Skip entries for other tables.
	    continue;
	}
	if (!output_table) {
	    // Now we know we need the table, create it if it's missing.
	    output_table = iqueue_writer_table(output, table, 1);
	}
	shash_insert(output_table, key.key, *val);
    }

    if (!input_table) {
	return;
    }

    // Copy all the existing entries, possibly modifying or dropping some.
    unsigned shash_len;
    shash_entry_t *entries = shash_entries(input_table, &shash_len);
    for (unsigned i = 0; i < shash_len; ++i) {
	shash_coord_t coord = (shash_coord_t){table, entries[i].key};
	uint64_t *op_set = shash_mod_table_get(set_map, coord);
	uint64_t *op_drop = shash_mod_table_get(drop_map, coord);
	if (op_set && op_drop) {
	    errx(1, "Cannot both set and drop shash %d:%"PRIu64, table, entries[i].key);
	}

	if (op_drop || op_set) {
	    continue;
	}
	if (!entries[i].key) {
	    // Skip key == 0, which is used internally by iqueue.
	    continue;
	}
	if (!output_table) {
	    // Now we know we need the table, create it if it's missing.
	    output_table = iqueue_writer_table(output, table, 1);
	}

	// Try to insert. If we set the value with the user-provided data, insert won't change it.
	shash_insert(output_table, entries[i].key, entries[i].value);
    }
}

static const char *
get_insert_buffer_path(
    const char *edit_path,
    uint64_t idx,
    uint64_t cnt)
{
    char *file_path;
    tsassert(asprintf(&file_path, "%s/%"PRIu64"-%"PRIu64".insert", edit_path, idx, cnt) >= 0);
    return file_path;
}

static const char *
get_edit_buffer_path(
    const char *edit_path,
    uint64_t idx)
{
    char *file_path;
    tsassert(asprintf(&file_path, "%s/%"PRIu64".edit", edit_path, idx) >= 0);
    return file_path;
}

static const char *
get_header_buffer_path(
    const char *edit_path)
{
    char *file_path;
    tsassert(asprintf(&file_path, "%s/header", edit_path) >= 0);
    return file_path;
}

// Write a message (from iqueue, or new) to an editable buffer file.
static void
write_editable_buffer(
    const char *buffer_path,
    const void *msg,
    size_t msg_len)
{
    int file;
    if ((file = creat(buffer_path, S_IRWXU)) == -1) {
	err(1, "Could not create file `%s`", buffer_path);
    }
    if (write(file, msg, msg_len) == -1) {
	err(1, "Could not write to file `%s`", buffer_path);
    }
    if (close(file) == -1) {
	err(1, "Could not close file `%s`", buffer_path);
    }
}

// Load the content of an editable buffer file.
static const void *
read_editable_buffer(
    const char *buffer_path,
    size_t *msg_len)
{
    int file;
    if ((file = open(buffer_path, O_RDONLY)) == -1) {
	err(1, "Could not open file `%s`", buffer_path);
    }
    struct stat file_stats;
    if (fstat(file, &file_stats) == -1) {
	err(1, "Could get size of file `%s`", buffer_path);
    }
    *msg_len = file_stats.st_size;
    const void *msg = mmap(0, *msg_len, PROT_READ, MAP_PRIVATE, file, 0);
    if (msg == MAP_FAILED) {
	err(1, "Failed mapping file `%s`", buffer_path);
    }
    return msg;
}

static iqueue_t *
mod_content(
    const char *edit_path,
    iqueue_t *input,
    const char *output_path,
    bool edit_header,
    msg_mod_table_t *insert_map,
    msg_mod_table_t *drop_map,
    msg_mod_table_t *edit_map)
{
    uint64_t input_len = iqueue_entries(input);
    bool wait_for_edits = false;
    size_t header_len;
    const char *header = iqueue_header(input, &header_len);

    // Create the editable buffer file for the header, if it needs to be changed.
    if (edit_header) {
	wait_for_edits = true;
	write_editable_buffer(get_header_buffer_path(edit_path), header, header_len);
    }

    // Create all the editable buffer files for insert and edit.
    {
	msg_mod_table_iterator_t it;
	msg_mod_table_iterator_init(insert_map, &it);
	for (;;) {
	    uint64_t idx;
	    uint64_t *add_count = msg_mod_table_iterator_next(insert_map, &it, &idx);
	    if (!add_count) {
		break;
	    }
	    if (idx > input_len) {
		errx(1, "Cannot insert at %"PRIu64", input lenght is %"PRIu64, idx, input_len);
	    }
	    wait_for_edits = true;
	    for (size_t i = 1; i <= *add_count; ++i) {
		write_editable_buffer(get_insert_buffer_path(edit_path, idx, i), NULL, 0);
	    }
	}
    }
    {
	msg_mod_table_iterator_t it;
	msg_mod_table_iterator_init(edit_map, &it);
	for (;;) {
	    uint64_t idx;
	    uint64_t *toedit = msg_mod_table_iterator_next(edit_map, &it, &idx);
	    if (!toedit) {
		break;
	    }
	    if (idx >= input_len) {
		errx(1, "Cannot edit %"PRIu64", input lenght is %"PRIu64, idx, input_len);
	    }
	    wait_for_edits = true;
	    size_t msg_len;
	    const void *msg = iqueue_data(input, idx, &msg_len);
	    if (!msg) {
		errx(1, "Could not read iqueue message %"PRIu64, idx);
	    }
	    write_editable_buffer(get_edit_buffer_path(edit_path, idx), msg, msg_len);
	    madvise(__UNCONST_T(void *, msg), msg_len, MADV_DONTNEED);
	}
    }

    // Wait for the user to confirm that she is done editing.
    if (wait_for_edits) {
	printf("Data ready to be edited under `%s`. Press ENTER when done editing.", edit_path);
	int char_read;
	while ((char_read = getchar()) != -1 && char_read != '\n');
	if (char_read == -1) {
	    errx(1, "Failed reading from stdin");
	}
    }

    // Create the new iqueue.
    if (edit_header) {
	const char *header_buffer_file = get_header_buffer_path(edit_path);
	header = read_editable_buffer(header_buffer_file, &header_len);
	unlink(header_buffer_file);
    }

    uint64_t creation_time = tsclock_getnanos(0);
    iqueue_t *output = iqueue_create(output_path, creation_time, header, header_len);
    if (!output) {
	err(1, "Could not create output iqx `%s`\n", output_path);
    }
    if (iqueue_creation(output) != creation_time) {
	errx(1, "Output iqx `%s` already exist, please give a new path.", output_path);
    }

    // Copy all the messages from the input iqueue with the necessary changes.
    for (uint64_t idx = 0; idx <= input_len; ++idx) {
	uint64_t *op_insert = msg_mod_table_get(insert_map, idx);
	uint64_t *op_edit = msg_mod_table_get(edit_map, idx);
	uint64_t *op_drop = msg_mod_table_get(drop_map, idx);
	if (op_edit && op_drop) {
	    errx(1, "Cannot both edit and drop message %"PRIu64, idx);
	}

	// Pick up the messages that will be inserted at this point.
	if (op_insert) {
	    for (uint64_t i = 1; i <= *op_insert; ++i) {
		const char *buffer_file = get_insert_buffer_path(edit_path, idx, i);
		size_t msg_len;
		const void *msg = read_editable_buffer(buffer_file, &msg_len);
		tsassert(!iqueue_append(output, msg, msg_len));
		madvise(__UNCONST_T(void *, msg), msg_len, MADV_DONTNEED);
		unlink(buffer_file);
	    }
	}

	if (idx >= input_len) {
	    break;
	}

	// Skip copying the current message if dropping.
	if (op_drop) {
	    continue;
	}

	// Pick up the edited message to use for this position.
	if (op_edit) {
	    const char *buffer_file = get_edit_buffer_path(edit_path, idx);
	    size_t msg_len;
	    const void *msg = read_editable_buffer(buffer_file, &msg_len);
	    tsassert(!iqueue_append(output, msg, msg_len));
	    madvise(__UNCONST_T(void *, msg), msg_len, MADV_DONTNEED);
	    unlink(buffer_file);
	    continue;
	}

	// None of the other options apply, just copy verbatim from the input.
	size_t msg_len;
	const char *msg = iqueue_data(input, idx, &msg_len);
	tsassert(!iqueue_append(output, msg, msg_len));
	madvise(__UNCONST_T(void *, msg), msg_len, MADV_DONTNEED);
    }

    return output;
}

int
main(int argc, char **argv)
{
    bool edit_header = false;

    size_t found_tokens;
    uint64_t *token_list;

    shash_mod_table_t *shash_set_map = shash_mod_table_create(32);
    shash_mod_table_t *shash_drop_map = shash_mod_table_create(32);

    msg_mod_table_t *msg_insert_map = msg_mod_table_create(32);
    msg_mod_table_t *msg_edit_map = msg_mod_table_create(32);
    msg_mod_table_t *msg_drop_map = msg_mod_table_create(32);

    shash_coord_t coord;
    uint64_t *table_value;
    int option = -1;
    while ((option = getopt(argc, argv, "hi:d:e:ts:x:")) != -1) {
	switch (option) {
	case 'h':
	    print_help(argv);
	    return EXIT_FAILURE;
	case 'i':
	    token_list = parse_tuple(1, 2, optarg, &found_tokens, 1);
	    table_value = msg_mod_table_insert(msg_insert_map, token_list[0]);
	    *table_value = token_list[1];
	    break;
	case 'e':
	    token_list = parse_tuple(1, 2, optarg, &found_tokens, 1);
	    for (size_t i = 0; i < token_list[1]; ++i) {
		msg_mod_table_insert(msg_edit_map, token_list[0]+i);
	    }
	    break;
	case 'd':
	    token_list = parse_tuple(1, 2, optarg, &found_tokens, 1);
	    for (size_t i = 0; i < token_list[1]; ++i) {
		msg_mod_table_insert(msg_drop_map, token_list[0]+i);
	    }
	    break;
	case 't':
	    edit_header = true;
	    break;
	case 's':
	    token_list = parse_tuple(3, 3, optarg, &found_tokens, 0);
	    coord = (shash_coord_t){token_list[0], token_list[1]};
	    table_value = shash_mod_table_insert(shash_set_map, coord);
	    *table_value = token_list[2];
	    break;
	case 'x':
	    token_list = parse_tuple(2, 2, optarg, &found_tokens, 0);
	    coord = (shash_coord_t){token_list[0], token_list[1]};
	    shash_mod_table_insert(shash_drop_map, coord);
	    break;
	default:
	    fprintf(stderr, "Uknown option `%c` (%d)\n", option, option);
	    print_help(argv);
	    return EXIT_FAILURE;
	}
    }

    // Warn if the user is trying to edit too many messages at once.
    if (msg_mod_table_size(msg_insert_map) + msg_mod_table_size(msg_edit_map) > MAX_UNWARNED_FILES) {
	printf("Trying to insert and edit `%"PRIu32"` messages. Ctrl-C to stop here, ENTER to continue\n",
	    msg_mod_table_size(msg_insert_map) + msg_mod_table_size(msg_edit_map));
	int char_read;
	while ((char_read = getchar()) != -1 && char_read != '\n');
	if (char_read == -1) {
	    errx(1, "Failed reading from stdin");
	}
    }

    if (optind == 1) {
	fprintf(stderr, "Expecting at least 1 editing option, but found none\n");
	print_help(argv);
	return EXIT_FAILURE;
    }

    if (argc - optind != 2) {
	fprintf(stderr, "Expecting exactly 2 non-option arguments, found `%d`\n", argc - optind);
	print_help(argv);
	return EXIT_FAILURE;
    }

    const char *input_path = argv[optind];
    const char *output_path = argv[optind+1];

    char *edit_path;
    if (asprintf(&edit_path, "%s.mod", input_path) == -1) {
	errx(1, "Could not create path for edit folder `%s`", edit_path);
    }

    iqueue_t *input = iqueue_open(input_path, false);
    if (!input) {
	err(1, "Could not open input iqx `%s`", input_path);
    }

    if (mkdir(edit_path, S_IRWXU)) {
	errx(1, "Could not create edit folder `%s`", edit_path);
    }
    printf("Creating edit folder at `%s`. Will erase on success.\n", edit_path);

    // Copy the modified messages first, as creation requires the new header.
    iqueue_t *output = mod_content(edit_path, input, output_path, edit_header, msg_insert_map, msg_drop_map, msg_edit_map);

    // Copy the modified shashes (iqueue uses tables 0-3).
    for (size_t i = 0; i <= 3; ++i) {
	mod_shashes(i, input, output, shash_set_map, shash_drop_map);
    }

    rmdir(edit_path);
    return EXIT_SUCCESS;
}
