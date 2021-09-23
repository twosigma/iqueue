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
#include "tslog.h"
#include "iqueue.h"
#include "tsclock.h"
#include "segfault.h"


static inline uint64_t
write_timestamp(
    iqueue_t * const iq
)
{
    iqueue_msg_t iqmsg;
    uint64_t * const msg = iqueue_allocate_raw(iq, 2 * sizeof(*msg), &iqmsg);
    if (!msg)
        TSABORTX("allocate failed");
    uint64_t ts = msg[0] = tsclock_getnanos(0);
    if (iqueue_update_be(iq, iqmsg, &msg[1]) != 0)
        TSABORTX("update failed");

    return ts;
}


static inline uint64_t
read_timestamp(
    iqueue_t * const iq,
    uint64_t iq_index
)
{
    size_t len;

    while (1)
    {
        int status = iqueue_status_wait(iq, iq_index, 1);
        if (status == IQUEUE_STATUS_HAS_DATA)
            return *(const uint64_t*) iqueue_data(iq, iq_index, &len);
    }
}


int
main(
    int argc,
    char ** argv
)
{
    __USE(argc);
    segfault_handler_install();

    const char * const iq1_name = "/tmp/iq1.iqx";
    const char * const iq2_name = "/tmp/iq2.iqx";

    iqueue_t * const iq1 = iqueue_create(iq1_name, 0, NULL, 0);
    if (!iq1)
        TSABORT("iq1");

    iqueue_t * const iq2 = iqueue_create(iq2_name, 0, NULL, 0);
    if (!iq2)
        TSABORT("iq2");

    uint64_t read_ts, write_ts, delta1 = 0, delta2 = 0;

    printf("Run: %s --tail --push --pull -f %s localhost:%s\n",
        argv[0],
        iq1_name,
        iq2_name
    );

    write_ts = write_timestamp(iq1);
    read_ts = read_timestamp(iq2, 0);

    TSLOGXL(TSINFO, "Received ack: %"PRIu64, tsclock_getnanos(0) - read_ts);

    const uint64_t max_iter = 1 << 20;

    for (uint64_t i = 1 ; i < max_iter ; i += 2)
    {
        // Check one-way (from 1 into 2)
        write_ts = write_timestamp(iq1);
        read_ts = read_timestamp(iq2, i);

        if (write_ts != read_ts)
            TSABORTX("write %"PRIu64" != read %"PRIu64, read_ts, write_ts);
        delta1 += tsclock_getnanos(0) - write_ts;

        // Check ping pong (from 2 back into 1)
        write_timestamp(iq2);
        read_timestamp(iq1, i+1);

        delta2 += tsclock_getnanos(0) - write_ts;

        if ((i & 0xFFFF) != 0xFFFF)
            continue;

        printf("one way: total %"PRIu64" avg %"PRIu64" ns\n",
            delta1,
            delta1 / i / 2
        );

        printf("two way: total %"PRIu64" avg %"PRIu64" ns\n",
            delta2,
            delta2 / i / 2
        );
    }

    return 0;
}
