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
 * Test ping-pong latency between two iqueues synchronized with iqsync
 * in bi-directional mode.
 *
 * On machine1, run:
 *  iqsync-pingpong-test ping
 *
 * On machine2, run:
 *  iqsync-pingpong-test pong
 *
 * Then on machine1, run:
 *  iqsync --sleep 0 --push --pull -f /tmp/ping.iqx machine2:/tmp/pong.iqx
 *
 */
#include "twosigma.h"
#include <stdio.h>
#include <unistd.h>
#include "tslog.h"
#include "iqueue.h"
#include "tsclock.h"
#include "segfault.h"


static inline uint64_t
write_timestamp(
    iqueue_t * const iq,
    uint64_t reply
)
{
    iqueue_msg_t iqmsg;
    uint64_t * const msg = iqueue_allocate_raw(iq, 3 * sizeof(*msg), &iqmsg);
    if (!msg)
        TSABORTX("allocate failed");
    uint64_t ts = msg[0] = tsclock_getnanos(0);
    msg[2] = reply;

    if (iqueue_update_be(iq, iqmsg, &msg[1]) != 0)
        TSABORTX("update failed");

    return ts;
}


static inline const uint64_t *
read_timestamp(
    iqueue_t * const iq,
    uint64_t iq_index
)
{
    size_t len;

    while (1)
    {
        const uint64_t * const msg = iqueue_data(iq, iq_index, &len);
        if (msg)
            return msg;
    }
}


int
main(
    int argc,
    char ** argv
)
{
    segfault_handler_install();

    if (argc <= 1)
        TSABORTX("Usage: %s (ping|pong) [/path/to/iqx]", argv[0]);

    const char * name;
    int do_ping;
    if (strcmp(argv[1], "ping") == 0)
    {
        name = "PING";
        do_ping = 1;
    } else
    if (strcmp(argv[1], "pong") == 0)
    {
        name = "PONG";
        do_ping = 0;
    } else
        TSABORTX("Must be ping or pong");

    const char * iq_name;
    if (argc > 2)
        iq_name = argv[2];
    else
    if (do_ping)
        iq_name = "/tmp/ping.iqx";
    else
        iq_name = "/tmp/pong.iqx";

    unlink(iq_name);
    iqueue_t * const iq = iqueue_create(iq_name, tsclock_getnanos(0), NULL, 0);
    if (!iq)
        TSABORT("%s: failed", name);

    uint64_t write_ts, read_ts;
    const uint64_t * reply;

    if (do_ping)
    {
        // Send the first message and wait for the response
        write_ts = write_timestamp(iq, 0);
        reply = read_timestamp(iq, 1);
        read_ts = reply[2];

        if (write_ts != read_ts)
            TSABORTX("Invalid reply: %"PRIu64" != %"PRIu64, write_ts, read_ts);

        TSLOGXL(TSINFO, "%s: Received ack: %"PRIu64,
            name,
            tsclock_getnanos(0) - write_ts
        );
    } else {
        // Read the first message and send a response
        reply = read_timestamp(iq, 0);
        TSLOGXL(TSINFO, "%s: saw first message", name);

        write_timestamp(iq, reply[0]);
    }


    const uint64_t max_iter = 1 << 20;

    uint64_t sum = 0;
    uint64_t sum2 = 0;
    uint64_t count = 0;

    for (uint64_t i = 2 ; i < max_iter ; i += 2)
    {
        if (!do_ping)
        {
            // Pong just waits for a message and then sends reply
            reply = read_timestamp(iq, i);
            write_timestamp(iq, reply[0]);
            continue;
        }

        // Check round trip (from us to them and back)
        write_ts = write_timestamp(iq, 0);
        reply = read_timestamp(iq, i + 1);
        read_ts = reply[2];

        if (write_ts != read_ts)
        {
            TSHDUMPL(TSINFO, reply, 32);
            TSABORTX("write %"PRIu64" != read %"PRIu64, read_ts, write_ts);
        }
        uint64_t delta = tsclock_getnanos(0) - write_ts;
        sum += delta;
        sum2 += delta * delta;

        if ((count++ & 0xFFFF) != 0xFFFF)
            continue;

        uint64_t avg = sum / count;

        printf("round trip: total %"PRIu64" avg %"PRIu64" ns\n",
            sum,
            avg
        );

        count = sum = sum2 = 0;
    }

    return 0;
}
