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
 * Report the overhead of writing to an uncontested iqueue with
 * and without locking enabled.
 */
#include "twosigma.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <pthread.h>
#include "tslog.h"
#include "tsclock.h"
#include "tssched.h"
#include "iqueue.h"
#include "segfault.h"


static void
histogram(
    const char * const label,
    const uint64_t * const samples,
    const uint64_t count
)
{
    uint64_t sum = 0;
    uint64_t sum2 = 0;
    uint64_t min = -1;
    uint64_t max = 0;

    for (uint64_t i = 0 ; i < count ; i++)
    {
        const uint64_t x = samples[i];
        sum += x;
        sum2 += x * x;
        if (x > max)
            max = x;
        if (x < min)
            min = x;
    }

    const uint64_t avg = sum / count;

    printf("%s: min=%"PRIu64" avg=%"PRIu64" max=%"PRIu64" var=%"PRIu64"\n",
        label,
        min,
        avg,
        max,
        sum2 / count - avg * avg
    );
}


static void
overhead_test(
    const char * const label,
    const char * const iqfile,
    const uint64_t msg_count,
    const uint64_t msg_len,
    const int lock_type
)
{
    unlink(iqfile);
    iqueue_t * const iq = iqueue_create(iqfile, 0, NULL, 0);
    if (!iq)
        TSABORTX("%s: Unable to create", iqfile);

    if ((lock_type & 1) && iqueue_mlock(iq) == -1)
    {
        TSLOGXL(TSERROR, "%s: Unable to lock", iqfile);
        return;
    } else
    if ((lock_type & 2) && iqueue_prefetch_thread(iq, NULL) == -1)
        TSABORTX("%s: Unable to start prefetch", iqfile);

    iqueue_allocator_t allocator;
    if (iqueue_allocator_init(iq, &allocator, 1 << 16, 1) < 0)
        TSABORTX("%s: Unable to create allocator", iqfile);

    // Limit the write speed to 512 MB/s
    const uint64_t delay = 1.0e9 * msg_len / (512 << 20);

    uint64_t * const update_times = calloc(msg_count, sizeof(*update_times));

    for (uint64_t i = 0 ; i < msg_count ; i++)
    {
        const uint64_t start = tsclock_getnanos(0);
        iqueue_msg_t iqmsg;
        uint8_t * const msg = iqueue_allocate(&allocator, msg_len, &iqmsg);
        if (!msg)
            TSABORTX("%s: Unable to allocate %"PRIu64, iqfile, i);

        memset(msg, i & 0xFF, msg_len);

        if (iqueue_update(iq, iqmsg, NULL) != 0)
            TSABORTX("%s: Unable to update %"PRIu64, iqfile, i);

        const uint64_t end = tsclock_getnanos(0);

        update_times[i] = end - start;

        // delay so that we limit our maximum write speed to 200 MB/s
        while ((uint64_t) tsclock_getnanos(0) < start + delay)
            continue;
    }

    iqueue_close(iq);

#if 0
    for (uint64_t i = 0 ; i < msg_count ; i++)
        printf("%"PRIu64",%s,%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64"\n",
            i,
            label,
            msg_len,
            alloc_times[i],
            memset_times[i],
            update_times[i]
        );
#else
    histogram(label, update_times, msg_count);
#endif

    free(update_times);
}


int
main(
    int argc,
    char ** argv
)
{
    segfault_handler_install();

    const char * const iqfile = argc > 1 ? argv[1] : "/tmp/overhead.iqx";
    const uint64_t msg_len = 200;
    //const uint64_t msg_count = (2048ull << 20) / msg_len;
    const uint64_t msg_count = (512ull << 20) / msg_len;

    printf("iter,test,len,alloc,memset,update\n");

    overhead_test("mlock", iqfile, msg_count, msg_len, 1);
    overhead_test("mlock+prefetch", iqfile, msg_count, msg_len, 3);
    overhead_test("normal", iqfile, msg_count, msg_len, 0);
    overhead_test("prefetch", iqfile, msg_count, msg_len, 2);
    //overhead_test("prefetch", iqfile, msg_count, msg_len, 1);

    //unlink(iqfile);

    return 0;
}
