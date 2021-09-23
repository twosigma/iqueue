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
 * Flood an iqueue with multiple writers to measure contended write time.
 *
 * Reports stats on stdout for redirection into a log file.
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

static volatile int go;
static iqueue_t * iq;
static const size_t msg_len = 200;
static const uint64_t write_iters = 1 << 16;

struct thread_context
{
    int nanosleep_time;
    int id;
    const char * filename;
    iqueue_t * iq;
    pthread_t thread;
    int cpu;
    uint64_t total_time;
};


static void *
write_thread(
    void * priv
)
{
    struct thread_context * const context = priv;
    const uint64_t my_id = context->cpu;

    while (true) {
        context->iq = iqueue_create(context->filename, 0, NULL, 0);
        if (context->iq)
            break;
    }
    iq = context->iq;

    iqueue_allocator_t allocator;
    if (iqueue_allocator_init(context->iq, &allocator, 1<<16, 1) < 0)
        TSABORTX("Unable to create iqueue allocator");

    while (!go)
        ;

    uint64_t last_delta = 0;
    uint64_t msg[msg_len / sizeof(uint64_t)];
    context->total_time = 0;

    for (uint64_t iter = 0 ; iter < write_iters ; iter++)
    {
        int64_t start = tsclock_getnanos(0);
        iqueue_msg_t offset;
        uint64_t * const data = iqueue_allocate(&allocator, msg_len, &offset);

        if (!data)
            TSABORTX("%"PRIu64": Unable to allocate message %"PRIu64, my_id, iter);

        memcpy(data, msg, msg_len);
        data[0] = my_id;
        data[1] = last_delta;

        if (iqueue_update(allocator.iq, offset, NULL) != 0)
            TSABORTX("%"PRIu64": Unable to write message %"PRIu64, my_id, iter);

        int64_t stop = tsclock_getnanos(0);
        last_delta = stop - start;
        context->total_time += last_delta;

        while (tsclock_getnanos(0) < stop + context->nanosleep_time)
            ;
    }

    return 0;
}


static void
write_test(
    const char * label,
    int thread_count,
    int * thread_cpus
)
{
    const char * filename = "/dev/shm/test3.iqx";
    unlink(filename);

    struct thread_context contexts[thread_count];

    for (int i = 0 ; i < thread_count ; i++)
    {
        struct thread_context * const context = &contexts[i];
        context->iq = NULL;
        context->filename = filename;
        context->cpu = thread_cpus[i];
        context->nanosleep_time = 0;

        pthread_create(&context->thread, NULL, write_thread, context);
        tssched_set_thread_affinity(context->thread, thread_cpus[i]);
    }

    go = 1;

    for (int i = 0 ; i < thread_count ; i++)
    {
        struct thread_context * const context = &contexts[i];
        pthread_join(context->thread, NULL);
        fprintf(stderr, "%s %d %"PRIu64" ns/write\n",
            label,
            context->cpu,
            context->total_time / write_iters
        );
    }

    uint64_t read_time = -tsclock_getnanos(0);
    for (uint64_t iter = 0 ; iter < write_iters * thread_count ; iter ++)
    {
        size_t len;
        const uint64_t * const msg = iqueue_data(iq, iter, &len);
        if (!msg)
            TSABORTX("Unable to read message %"PRIu64, iter);
    }
    read_time += tsclock_getnanos(0);
    fprintf(stderr, "%s %"PRIu64" ns/read\n",
        label,
        read_time / (write_iters * thread_count)
    );

    for (uint64_t iter = 0 ; iter < write_iters * thread_count ; iter++)
    {
        size_t len;
        const uint64_t * const msg = iqueue_data(iq, iter, &len);
        printf("%s %"PRIu64" %"PRId64"\n", label, msg[0], msg[1]);
    }

    iqueue_close(iq);

    unlink(filename);
}


int main(void)
{
    write_test("hyper", 2,
        (int[]) { 1, 13 });

    write_test("same-2", 2,
        (int[]) { 1, 3 });

    write_test("same-4", 4,
        (int[]) { 1, 3, 5, 7 });

    write_test("same-8", 8,
        (int[]) { 1, 3, 5, 7, 9, 11, 13, 15 });

    write_test("cross-2", 2,
        (int[]) { 1, 2 });

    write_test("cross-4", 4,
        (int[]) { 1, 2, 3, 4 });

    write_test("cross-8", 8,
        (int[]) { 1, 2, 3, 4, 5, 6, 7, 8 });

    write_test("cross-16", 16,
        (int[]) { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });

    return 0;
}
