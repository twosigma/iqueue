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
#include <pthread.h>
#include "tslog.h"
#include "tsclock.h"
#include "tssched.h"
#include "iqueue.h"
#include "segfault.h"


static volatile int go;
static uint64_t msg_len = 200;
static const uint64_t latency_iters = 1 << 20;

static void *
latency_pong(
    void * priv
)
{
    iqueue_t * const iq = priv;
    iqueue_allocator_t allocator;
    if (iqueue_allocator_init(iq, &allocator, 1 << 20, 1) < 0)
        TSABORTX("Failed to create allocator");

    while (!go)
        ;

    for (uint64_t iter = 0 ; iter < latency_iters ; iter += 2)
    {
        if (iqueue_status_wait(iq, iter, -1) != IQUEUE_STATUS_HAS_DATA)
            TSABORTX("Failed to get message %"PRIu64, iter);

        size_t len;
        const uint64_t * const msg = iqueue_data(iq, iter, &len);

        char data[len];
        memcpy(data, msg, len);
        const uint64_t now = tsclock_getnanos(0);

        iqueue_msg_t offset;
        uint64_t * const reply = iqueue_allocate(&allocator, len, &offset);
        if (!reply)
            TSABORTX("Failed to allocate reply %"PRIu64, iter);

        TSLOGXL(TSDIAG, "%"PRIu64": %p => %p @ %"PRIx64,
            iter,
            msg,
            reply,
            iqueue_msg_offset(offset)
        );

        memcpy(reply, msg, len);
        reply[0] = now - msg[0];

        if (iqueue_update(allocator.iq, offset, NULL) != 0)
            TSABORTX("Failed to update reply %"PRIu64, iter);
    }

    return NULL;
}


static void *
latency_ping(
    void * priv
)
{
    iqueue_t * const iq = priv;
    iqueue_allocator_t allocator;
    if (iqueue_allocator_init(iq, &allocator, 1 << 20, 1) < 0)
        TSABORTX("Failed to create allocator");

    while (!go)
        ;

    uint64_t data[msg_len / sizeof(uint64_t)];
    memset(data, 0xA5, sizeof(data));

    uint64_t total_time = 0;
    const uint64_t warmup_iters = 1001;

    for (uint64_t iter = 1 ; iter < latency_iters ; iter += 2)
    {
        uint64_t now = tsclock_getnanos(0);
        if (iter == warmup_iters)
            total_time = -now;

        size_t len = sizeof(data);
        iqueue_msg_t offset;
        uint64_t * const msg = iqueue_allocate(&allocator, len, &offset);
        if (!msg)
            TSABORTX("Failed to allocate msg %"PRIu64, iter);
        TSLOGXL(TSDIAG, "%"PRIu64": %p @ %"PRIx64, iter-1, msg, iqueue_msg_offset(offset));

        memcpy(msg, data, len);
        msg[0] = now;

        uint64_t id;
        if (iqueue_update(allocator.iq, offset, &id) != 0)
            TSABORTX("Failed to update msg %"PRIu64, iter);
        if (id != iter - 1)
            TSABORTX("Iter %"PRIu64" posted to slot %"PRIu64"!", iter-1, id);

        if (iqueue_status_wait(iq, iter, -1) != IQUEUE_STATUS_HAS_DATA)
            TSABORTX("Failed to get message %"PRIu64, iter);
        const uint64_t * const reply = iqueue_data(iq, iter, &len);

        if (len != sizeof(data))
            TSABORTX("Got bad size! id %"PRIu64" expected %zu got %zu", iter, sizeof(data), len);
        if (memcmp(data+1, reply+1, len - sizeof(*data)) != 0)
            TSABORTX("Reply mismatch!");
    }

    total_time += tsclock_getnanos(0);
    TSLOGXL(TSINFO,
        "%"PRIu64" bytes average one-way latency: %"PRIu64" ns",
        sizeof(data),
        total_time / (latency_iters - warmup_iters)
    );

    return NULL;
}


static void
latency_test(
    const char * label,
    int reader_cpu,
    int writer_cpu
)
{
    const char * file = "/dev/shm/test3.iqx";
    unlink(file);

    iqueue_t * const iq = iqueue_create(file, 0, NULL, 0);
    if (!iq)
        TSABORT("Unable to create iqueue!");

    pthread_t threads[2];

    // One-way latency test
    pthread_create(&threads[0], NULL, latency_ping, iq);
    pthread_create(&threads[1], NULL, latency_pong, iq);
    tssched_set_thread_affinity(threads[0], reader_cpu);
    tssched_set_thread_affinity(threads[1], writer_cpu);

    TSLOGXL(TSINFO, "Reader cpu %d writer cpu %d", reader_cpu, writer_cpu);
    go = 1;
    pthread_join(threads[0], NULL);
    pthread_join(threads[1], NULL);

#if 0
    for (uint64_t iter = 8193 ; iter < latency_iters ; iter += 2)
    {
        size_t len;
        const uint64_t * const msg = iqueue_data(iq, iter, &len);
        printf("%s %"PRIu64" %"PRIu64"\n", label, msg_len, *msg);
    }
#else
    // Compute a histagram of the data set
    for (uint64_t iter = 8193 ; iter < latency_iters ; iter += 2)
    {
        size_t len;
        const uint64_t * const msg = iqueue_data(iq, iter, &len);
        printf("%s %"PRIu64" %"PRIu64"\n", label, msg_len, *msg);
    }
#endif

    iqueue_close(iq);
}


int main(void)
{
    segfault_handler_install();

    uint64_t msg_lens[] = { 8, 64, 128, 200, 512, 1024 };

    for (unsigned i = 0 ; i < __arraycount(msg_lens) ; i++)
    {
        msg_len = msg_lens[i];
        latency_test("hyper", 1, 13);
        latency_test("same", 1, 3);
        latency_test("cross", 1, 2);
    }

    return 0;
}
