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
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <getopt.h>
#include <ctype.h>
#include <errno.h>
#include "tslog.h"
#include "iqueue.h"
#include "tsclock.h"
#include "io_utils.h"

static struct option long_options[] = {
    { "help",           no_argument,        0, '?' },
    { "iqueue",         required_argument,  0, 'f' },
    { "create",         no_argument,        0, 'C' },
    { "header",         no_argument,        0, 'H' },
    { "stats",          no_argument,        0, 's' },
    { "watch",          no_argument,        0, 'w' },
    { "append",         no_argument,        0, 'a' },
    { "line",           no_argument,        0, 'l' },
    { "follow",         no_argument,        0, 'F' },
    { "seal",           no_argument,        0, 'S' },
    { "archive",        no_argument,        0, 'A' },
    { "zero",           no_argument,        0, '0' },
    { "binary",         no_argument,        0, 'b' },
    { "no-header",      no_argument,        0, 'N' },
    { "copyin",         no_argument,        0, '1' },
    { "copyout",        no_argument,        0, '2' },
    { "writer",         required_argument,  0, 'W' },
    { "print-entry",    required_argument,  0, 'n' },
    { "begin",          required_argument,  0, 'B' },
    { "end",            required_argument,  0, 'E' },
    { "debug",          required_argument,  0, 'd' },
    { 0, 0, 0, 0},
};

static void
__attribute__((noreturn))
usage(
    FILE * stream,
    const char * msg
)
{
    static const char usage_str[] =
"Usage:\n"
"   iqueue [options...]\n"
"\n"
"Options:\n"
"   -h | -? | --help            This help\n"
"\n"
"General options:\n"
"   -f | --iqueue /path/file    iqueue file (.iqx) to update\n"
"   -C | --create               Initialize the message log\n"
"   -H | --header               Read a user header from stdin (for create)\n"
"   -s | --stats                Print stats about the queue\n"
"   -w | --watch                Print stats periodically\n"
"   -a | --append               Read a message from stdin and append it\n"
"   -W | --writer N,V           Set writer N heartbeat value to V\n"
"   -l | --line                 Read a new message per line and append them\n"
"   -F | --follow               Print new messages as they are added\n"
"                               or re-open a sealed iqueue in --line append\n"
"   -S | --seal                 Seal the iqueue from further writes\n"
"   -A | --archive              When sealing, archive the file\n"
"   -N | --no-header            Do not print the user header\n"
"   -n | --print-entry N        Print only entry number N\n"
"   -B | --begin N              Start from entry N\n"
"   -E | --end N                End with entry N\n"
"   -d | --debug N              Debug entry N (or the entire queue if N==-1)\n"
"   -b | --binary               Print binary messages\n"
"   -0 | -z | --zero            Print nul-separated messages in ascii mode\n"
"\n"
"DCAT formatting:\n"
"\n"
"For archiving and version transformations, iqueues can be transformed into\n"
"a DC formatted file with --copyout.  The user header will be populated\n"
"into the DC user header, and restored when a new iqueue is created.\n"
"\n"
"   --copyout                   Output in a form suitable for --copyin\n"
"   --copyin                    Read in the form output by --copyout,\n"
"                               implies --create, and will copy user header\n"
"                               from dcat file.\n"
;

    fprintf(stream, "%s%s", msg, usage_str);
    exit(EXIT_FAILURE);
}


int
iqueue_reopen_wait(
    iqueue_t * const iq
)
{
    const uint64_t creation = iqueue_creation(iq);
    TSLOGXL(TSINFO, "%s: Waiting for new queue", iqueue_name(iq));

    while (1)
    {
        // Try to re-open it until we have a file
        if (iqueue_reopen(iq) < 0)
        {
            if (errno != ENOENT)
                return -1;

            usleep(50000);
            continue;
        }

        // We have a real file; see if it is the same one
        if (iqueue_creation(iq) == creation)
        {
            sleep(1);
            continue;
        }

        // New iqueue, new creation.  We're done
        return 0;
    }
}

static ssize_t
read_all(
    int fd,
    uint8_t * buf,
    size_t len
)
{
    size_t offset = 0;

    while (offset < len)
    {
        ssize_t rc = read(fd, buf+offset, len-offset);
        if (rc < 0)
        {
            TSLOGL(TSERROR, "read failed");
            return -1;
        }

        offset += rc;

        if (rc == 0)
            return offset;
    }

    TSLOGXL(TSERROR, "message too long! limit is %zu bytes", len);
    return -1;
}


static int
iqueue_append_one(
    iqueue_t * const iq,
    int fd,
    int zero_out
)
{
    if (iqueue_is_sealed(iq))
        TSABORTX("can not append: iqueue is sealed");

    uint8_t * const buf = calloc(1, IQUEUE_MSG_MAX);
    const size_t max_size = zero_out ? IQUEUE_MSG_MAX - 1 : IQUEUE_MSG_MAX;
    ssize_t len = read_all(fd, buf, max_size);
    if (len < 0)
        TSABORTX("Error reading from stdin!");
    if (zero_out)
        buf[len++] = '\0';

    int rc = iqueue_append(iq, buf, len);
    free(buf);

    if (rc != 0)
    {
        TSLOGXL(TSERROR, "%s: Update failed rc=%d", iqueue_name(iq), rc);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}


/** Read a line with either a newline or zero separator.
 *
 * On end of file, the last line will be returned, with or without a separator.
 *
 * \return the number of bytes read, including the separator.
 */
static ssize_t
read_entire_line(
    const int fd,
    char * const buf,
    const size_t len,
    const char separator
)
{
    size_t off = 0;

    while (off < len-1)
    {
        ssize_t rlen = read(fd, &buf[off], 1);
        if (rlen < 0)
            return rlen;

        if (rlen == 0)
        {
            // Closed file and no partial line
            if (off == 0)
                return -1;

            // Partial line read
            off++;
            break;
        }

        // Check for end of line
        if (separator == buf[off++])
            break;
    }

    // nul terminate the string, just in case and return the length
    // read, including the separator
    buf[off] = '\0';
    return off;
}


/** Convert a text string to binary and append */
static int
iqueue_append_octdump(
    iqueue_t * const iq,
    const char * const buf,
    const size_t len
)
{
    iqueue_msg_t iqmsg;
    size_t out_len = 0;
    uint8_t * const m = iqueue_allocate_raw(iq, len, &iqmsg);

    for (size_t i = 0 ; i < len ; i++)
    {
        char c = buf[i];
        if (c != '\\')
        {
            m[out_len++] = c;
            continue;
        }

        if (buf[i+1] == '\\')
        {
            m[out_len++] = '\\';
            i++;
            continue;
        }

        // Convert from an octal dump to hex
        uint8_t o = 0;
        o = (buf[++i] - '0') | (o << 3);
        o = (buf[++i] - '0') | (o << 3);
        o = (buf[++i] - '0') | (o << 3);

        m[out_len++] = o;
    }

    // Resize the iqmsg, throwing away some space at the end
    iqmsg = iqueue_msg(iqueue_msg_offset(iqmsg), out_len);

    return iqueue_update(iq, iqmsg, NULL);
}

static int
iqueue_append_line(
    iqueue_t * const iq,
    const int fd,
    const bool zero_separator,
    const bool do_follow
)
{
    if (iqueue_is_sealed(iq))
    {
        if (!do_follow)
            TSABORTX("%s: can not append: iqueue is sealed", iqueue_name(iq));
        if (iqueue_reopen_wait(iq) < 0)
            TSABORTX("%s: reopen failed", iqueue_name(iq));
    }

    char * const buf = calloc(1, IQUEUE_MSG_MAX);

    while (1)
    {
        int rc;
        ssize_t len = read_entire_line(fd, buf, IQUEUE_MSG_MAX, zero_separator ? '\0' : '\n');
        if (len == -1)
            break;

        // Do not include any newlines if we have them
        if (zero_separator)
            len--;
        else
        if (buf[len-1] == '\n')
            buf[--len] = '\0';

retry:
        rc = iqueue_append_octdump(iq, buf, len);
        if (rc == 0)
            continue;

        if (rc != IQUEUE_STATUS_SEALED && !do_follow)
        {
            TSLOGXL(TSERROR, "%s: Update failed rc=%d", iqueue_name(iq), rc);
            return EXIT_FAILURE;
        }

        // We have a sealed iqueue; try to reopen until we have a new one
        if (iqueue_reopen_wait(iq) < 0)
        {
            TSLOGL(TSERROR, "%s: Unable to reopen", iqueue_name(iq));
            return EXIT_FAILURE;
        }

        goto retry;
    }

    free(buf);

    return EXIT_SUCCESS;
}


static int
iqueue_stats_output(
    iqueue_t * const iq
)
{
    printf("%s:"
        " %"PRIu64" (0x%"PRIx64")"
        " data=%"PRIu64
        " entries=%"PRIu64
        "%s"
        "\n",
        iqueue_name(iq),
        iqueue_creation(iq),
        iqueue_creation(iq),
        iqueue_data_len(iq),
        iqueue_entries(iq),
        iqueue_is_sealed(iq) ? " sealed" : ""
    );

    for (unsigned id = 0 ; id < 4 ; id++)
    {
        shash_t  * const sh = iqueue_writer_table(iq, id, 0);
        if (!sh)
            continue;

        unsigned max_entries;
        const shash_entry_t * const table = shash_entries(sh, &max_entries);

        printf("Writers %u:\n", id);

        for (unsigned i = 0 ; i < max_entries ; i++)
        {
            const shash_entry_t * const writer = &table[i];
            if (writer->key == 0)
                break;
            printf("    %"PRIx64": %"PRIu64"\n",
                writer->key,
                writer->value
            );
        }
    }

    return 0;
}


static int
iqueue_watch_output(
    iqueue_t * const iq,
    const unsigned sleep_us
)
{
    uint64_t old_time = tsclock_getnanos(0);
    uint64_t old_entries = iqueue_entries(iq);

    printf("%s: creation %"PRIu64" (0x%"PRIx64"): %"PRIu64" entries\n",
        iqueue_name(iq),
        iqueue_creation(iq),
        iqueue_creation(iq),
        old_entries
    );

    while (1)
    {
        usleep(sleep_us);
        const uint64_t new_time = tsclock_getnanos(0);
        const uint64_t new_entries = iqueue_entries(iq);
        if (new_entries == old_entries)
        {
            if (iqueue_is_sealed(iq))
                break;
            continue;
        }

        printf("%s: %"PRIu64" entries (%.0f entries/sec)\n",
            iqueue_name(iq),
            new_entries,
            (new_entries - old_entries) * 1.0e9 / (new_time - old_time)
        );

        old_time = new_time;
        old_entries = new_entries;
    }

    TSLOGXL(TSINFO, "%s: iqueue has been sealed", iqueue_name(iq));

    return 0;
}


static int
iqueue_seal_and_archive(
    iqueue_t * const iq,
    const int do_archive
)
{
    const char * const old_name = iqueue_name(iq);
    int rc = iqueue_seal(iq);
    if (rc != 0)
    {
        TSLOGXL(TSERROR, "%s: Unable to seal: %s",
            old_name,
            rc == IQUEUE_STATUS_SEALED ? "already sealed" :
            rc == IQUEUE_STATUS_INDEX_INVALID ? "invalid index" :
            "unknown error"
        );
        return EXIT_FAILURE;
    }

    if (!do_archive)
        return EXIT_SUCCESS;

    if (iqueue_archive(iq, IQUEUE_MSG_BAD_ID) < 0)
        return EXIT_FAILURE;

    TSLOGXL(TSINFO, "%s: archived", iqueue_name(iq));

    return EXIT_SUCCESS;
}


static int
iqueue_update_writer(
    iqueue_t * const iq,
    const char * const writer_flag
)
{
    char * end;
    const uint64_t id = strtoul(writer_flag, &end, 0);
    if (!end || end[0] != ',' || end[1] == '\0')
        usage(stderr, "Unable to parse writer, must be 'N,V'\n");

    const uint64_t value = strtoul(end+1, &end, 0);
    if (!end || end[0] != '\0')
        usage(stderr, "Unable to parse value, must be 'N,V'\n");

    const unsigned table_id = 0;
    shash_t * const sh = iqueue_writer_table(iq, table_id, 1);
    if (!sh)
        TSABORTX("%s: Unable to create/retrieve write table %u?",
            iqueue_name(iq),
            table_id
        );

    shash_entry_t * writer = shash_insert(sh, id, value);
    if (writer)
    {
        // Writer did not exist; we are done.
        TSLOGXL(TSINFO, "%s: Writer %u.0x%"PRIx64" value %"PRIu64,
            iqueue_name(iq),
            table_id,
            id,
            value
        );
        return 0;
    }

    // Writer already existed.  Retrieve it and try an update
    writer = shash_get(sh, id);
    if (!writer)
        TSABORTX("%s: Writer %u.0x%"PRIx64" should exist?",
            iqueue_name(iq),
            table_id,
            id
        );

    if (iqueue_writer_update(sh, writer, value))
    {
        TSLOGXL(TSINFO, "%s: Writer %u.0x%"PRIx64" value %"PRIu64,
            iqueue_name(iq),
            table_id,
            id,
            value
        );
        return 0;
    }

    TSLOGXL(TSWARN, "%s: Writer %u.0x%"PRIx64" tried to write %"PRIu64", current value %"PRIu64,
        iqueue_name(iq),
        table_id,
        id,
        value,
        writer->value
    );

    return 0;
}


int
main(
    int argc,
    char **argv
)
{
    const char * iqueue_file = NULL;
    int create_flag = 0;
    int writable = 0;
    uint64_t print_entry = -1;
    uint64_t debug_entry = -2;
    uint64_t begin_entry = 0;
    uint64_t end_entry = -1;
    int option_index = 0;
    int append = 0;
    int append_line = 0;
    int follow = 0;
    int do_seal = 0;
    int do_archive = 0;
    int binary_out = 0;
    int zero_out = 0;
    int header = 1;
    int do_stats = 0;
    int do_watch = 0;
    int read_header = 0;
    const char * writer_flag = NULL;

    while (1)
    {
        int c = getopt_long(
            argc,
            argv,
            "h?f:CGaFz0bAHI:D:n:B:E:swNlW:",
            long_options,
            &option_index
        );

        if (c == -1)
            break;

        switch (c)
        {
        case 0: break;
        default: usage(stderr, ""); break;
        case 'h': case '?': usage(stdout, ""); break;

        // Messagebox options
        case 'f': iqueue_file = optarg; break;
        case 'C': create_flag = 1; break;
        case 'n': print_entry = strtoul(optarg, NULL, 0); break;
        case 'd': debug_entry = strtoul(optarg, NULL, 0); break;
        case 'B': begin_entry = strtoul(optarg, NULL, 0); break;
        case 'E': end_entry = strtoul(optarg, NULL, 0); break;
        case 'a': append = 1; writable = true; break;
        case 'l': append_line = 1; writable = true; break;
        case 'F': follow = 1; break;
        case 'S': do_seal = 1; writable = true; break;
        case 'A': do_archive = 1; break;
        case 'z':
        case '0': zero_out = 1; break;
        case 'b': binary_out = 1; break;
        case 'N': header = 0; break;
        case 'H': read_header = 1; break;
        case 's': do_stats = 1; break;
        case 'w': do_watch = 1; break;
        case 'W': writer_flag = optarg; writable = true; break;
        }
    }

    if (!iqueue_file)
        usage(stderr, "iqueue file must be specified!\n");
    if (argc != optind)
        usage(stderr, "Extra arguments?\n");

    uint8_t * user_hdr = NULL;
    size_t user_hdr_len = 0;

    if (read_header)
    {
        if (create_flag != 1)
            usage(stderr, "--read-header is not useful unless creating\n");
        user_hdr = alloca(65536);
        user_hdr_len = read_all(STDIN_FILENO, user_hdr, 65536);
        if (user_hdr_len == (size_t) -1)
            TSABORTX("Error reading user header");
    }

    if (create_flag)
    {
        const uint64_t creation = tsclock_getnanos(0);
        iqueue_t * const iq = iqueue_create(
            iqueue_file,
            creation,
            user_hdr,
            user_hdr_len
        );
        if (!iq)
            TSABORTX("%s: Unable to create", iqueue_file);
        if (iqueue_creation(iq) != creation)
            TSLOGXL(TSINFO, "%s: iqueue already existed", iqueue_file);
        return EXIT_SUCCESS;
    }

    iqueue_t * const iq = iqueue_open(iqueue_file, writable);
    if (!iq)
        TSABORTX("Failed to %s %s",
            create_flag == 1 ? "create" : "open",
            iqueue_file
        );

    if (writer_flag)
        return iqueue_update_writer(iq, writer_flag);

    if (do_stats)
        return iqueue_stats_output(iq);
    if (do_watch)
        return iqueue_watch_output(iq, 1e6);
    if (append)
        return iqueue_append_one(iq, STDIN_FILENO, zero_out);
    if (append_line)
        return iqueue_append_line(iq, STDIN_FILENO, zero_out, follow);

    if (do_seal)
        return iqueue_seal_and_archive(iq, do_archive);
    else
    if (do_archive)
        return iqueue_archive(iq, IQUEUE_MSG_BAD_ID);

    if (debug_entry != (uint64_t) -2)
    {
        iqueue_debug(iq, debug_entry);
        return EXIT_SUCCESS;
    }

    // Print the user header on the file, if there is one
    if (header)
    {
        size_t hdr_len;
        const uint8_t * const hdr_buf = iqueue_header(iq, &hdr_len);
        size_t offset = 0;
        while (offset < hdr_len)
        {
            ssize_t wlen = write(
                STDOUT_FILENO,
                hdr_buf + offset,
                hdr_len - offset
            );
            if (wlen <= 0)
                TSABORT("header write failed");
            offset += wlen;
        }
    }

    uint64_t id = begin_entry;
    if (print_entry != (uint64_t) -1)
        id = print_entry;

    while (1)
    {
        if (end_entry != (uint64_t) -1 && id > end_entry)
            break;
        size_t len;
        const uint8_t * data = iqueue_data(iq, id, &len);
        if (!data)
        {
            if (!follow)
                break;
            if (iqueue_is_sealed(iq))
                break;
            usleep(100);
            continue;
        }

        id++;

        if (binary_out)
        {
            ssize_t wlen = write_all(STDOUT_FILENO, data, len);
            if ((size_t) wlen != len)
                TSABORT("write failed");
        } else {
            // ASCII output
            for (uint64_t i = 0 ; i < len ; i++)
            {
                uint8_t c = data[i];
                if (c == '\\')
                    printf("\\\\");
                else
                if (isprint(c)) // || isspace(c))
                    printf("%c", c);
                else
                    printf("\\%03o", c);
            }

            printf("%c", zero_out ? '\0' : '\n');
        }

        // If they have called us with print entry, we are done
        if (print_entry != (uint64_t) -1)
            break;
    }

    iqueue_close(iq);
    return 0;
}
