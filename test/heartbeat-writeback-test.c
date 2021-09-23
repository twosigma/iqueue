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
 * Test interaction between the writeback thread and the iqueue heartbeats.
 * Goal: determine if the writeback can cause gaps.
 */
#include "twosigma.h"
#include <stdio.h>
#include <inttypes.h>
#include "tslog.h"
#include "tsclock.h"
#include "iqueue.h"
#include "ftrace.h"

int main(int argc, char ** argv)
{
    ftrace_open();
    ftrace_start();

    if (argc <= 1)
        TSABORTX("usage: %s /path/to/test.iqx", argv[0]);

    const char * const filename = argv[1];
    iqueue_t * const iq = iqueue_create(
        filename,
        0,
        NULL,
        0
    );

    if (!iq)
        TSABORTX("%s: unable to create", filename);

    shash_t * const sh = iqueue_writer_table(iq, 0, 1);
    if (!sh)
        TSABORTX("%s: unable to create write table", filename);

    shash_entry_t * const entry = shash_insert_or_get(sh, 0xDECAFBAD, 0);
    if (!entry)
        TSABORTX("%s: unable to create writer", filename);
    int check = 0;

    for (uint64_t i = 0 ; ; i++)
    {
        const uint64_t now = tsclock_getnanos(0);
        if (check)
        {
            const uint64_t old = entry->value;
            ftrace_mark("gap %"PRIu64" ns", now - old);
            ftrace_stop();
            if (now - old > 100e3)
                TSLOGXL(TSINFO, "%"PRIu64" gap", now - old);
            check = 0;
        }

        iqueue_writer_update(sh, entry, now);
        check = 1;

        if ((i & (0xFFFFF)) == 0)
            iqueue_append(iq, &now, sizeof(now));
    }

    //unlink(filename);
    return 0;
}
