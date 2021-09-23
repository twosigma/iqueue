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
/** \file
 * Test the DC wrapper for iqueue processing.
 *
 * If no DC file is given on the command line, a fake one will be created
 * and an iqueue will be read from it.
 */
#include "twosigma.h"
#include <stdio.h>
#include <inttypes.h>
#include <unistd.h>
#include "tslog.h"
#include "tsclock.h"
#include "iqueue-dc.h"
#include "segfault.h"


static const uint64_t row_count = 1 << 20;


static void
write_dc_file(
    const char * dc_file
)
{
    static const tsutil_dcat_type_t types[] = {
        { "c1", TSUTIL_DCAT_SHORT_TYPE, -1 },
        { "v1", TSUTIL_DCAT_BUF_TYPE, -1 },
        { "c2", TSUTIL_DCAT_LONG_TYPE, -1 },
        { "v2", TSUTIL_DCAT_BUF_TYPE, -1 },
        { 0, 0, 0 },
    };

    tsutil_dcat_file_t * file = tsutil_dcat_write_file(
        dc_file,
        "<!-- User header goes here -->",
        types,
        0
    );
    if (!file)
        TSABORTX("%s: Unable to write?", dc_file);

    // Write an all invalid row
    tsutil_dcat_write_newrow(
        file,
        0xF,
        tsclock_getnanos(0)
    );

    for (uint64_t i = 1 ; i < row_count ; i++)
    {
        tsutil_dcat_write_newrow(
            file,
            i & 1,
            tsclock_getnanos(0)
        );

        // Alternate the first column being valid
        if ((i & 1) == 0)
            tsutil_dcat_write_short(file, (uint16_t) i);
        tsutil_dcat_write_buf(file, "v1 message goes here", i & 15);
        tsutil_dcat_write_long(file, (uint64_t) i);
        tsutil_dcat_write_buf(file, "v2 messge goes here", (~i) & 15);
    }

    tsutil_dcat_close(file);
}


int main(
    int argc,
    const char ** argv
)
{
    segfault_handler_install();
    tslevel = TSDEBUG;

    const char * dc_file = "/tmp/test.dc";

    // Create a fake file if one is not specified
    if (argc <= 1)
        write_dc_file(dc_file);
    else
        dc_file = argv[1];

    const char * user_hdr;
    const tsutil_dcat_type_t * types;
    iqueue_t * const iq = iqueue_open_dc(
        dc_file,
        &user_hdr,
        &types
    );

    if (!iq)
        TSABORTX("%s: Unable to map", dc_file);

    const uint64_t entries = iqueue_entries(iq);

    if (argc <= 1 && entries != row_count)
        TSABORTX("%s: Should have %"PRIu64" entries, not %"PRIu64,
            dc_file,
            row_count,
            entries
        );

    TSLOGXL(TSINFO, "%s: Mapped %"PRIu64" entries", dc_file, entries);

    uint64_t id = 0;
    uint64_t total_len = 0;

    uint64_t read_time = -tsclock_getnanos(0);
    while (1)
    {
        size_t len;
        const void * data = iqueue_data(iq, id, &len);
        if (!data)
            break;

        const uint64_t timestamp = be64toh(*(const uint64_t*) data);
        TSLOGXL(TSDIAG, "%"PRIu64": %"PRIu64" %zu", id, timestamp, len);

        id++;
        total_len += len;
        __USE(timestamp);
    }
    read_time += tsclock_getnanos(0);

    TSLOGXL(TSINFO, "%s: Average row size: %"PRIu64" bytes, %"PRIu64" ns/row",
        dc_file,
        id ? total_len / id : 0,
        id ? read_time / id : 0
    );

    iqueue_close(iq);

    if (argc <= 1)
        unlink(dc_file);

    return 0;
}
