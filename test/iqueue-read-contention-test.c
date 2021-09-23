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
/**
 * Multiple readers on an iqueue with a single writer, testing
 * contended read time.
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
static const size_t msg_len = 200;
static const uint64_t write_iters = 1 << 16;

struct thread_context
{
    // Shared.
    iqueue_t *iq;
    pthread_t thread;
    const char *filename;
    int cpu;
    uint64_t total_time;

    // Used by writer only.
    int nanosleep_time;

    // Used by reader only.
    bool ready;
};


static void *
write_thread(
    void *priv)
{
    struct thread_context *const context = priv;
    const uint64_t my_id = context->cpu;

    while (true) {
        context->iq = iqueue_create(context->filename, 0, NULL, 0);
        if (!context->iq) {
            TSLOGX(TSERROR, "Unable to create iqueue!");
        } else {
            break;
        }
    }

    iqueue_allocator_t allocator;
    if (iqueue_allocator_init(context->iq, &allocator, 1<<16, 1) < 0)
        TSABORTX("Unable to create iqueue allocator");

    // Signal reads to open iqueue.
    go = 1;

    // Wait for readers to be done.
    while (go != 2);

    // Start writing.
    uint64_t last_delta = 0;
    for (uint64_t iter = 0; iter < write_iters; ++iter)
    {
        int64_t start = tsclock_getnanos(0);
        iqueue_msg_t offset;
        uint64_t * const data = iqueue_allocate(&allocator, msg_len, &offset);

        if (!data)
            break;
        data[0] = my_id;
        data[1] = last_delta;
        data[2] = tsclock_getnanos(0);

        if (iqueue_update(allocator.iq, offset, NULL) != 0)
            TSABORTX("%"PRIu64": Unable to write message %"PRIu64, my_id, iter);

        int64_t stop = tsclock_getnanos(0);
        last_delta = stop - start;
        context->total_time += last_delta;

        while (tsclock_getnanos(0) < stop + context->nanosleep_time);
    }

    iqueue_close(context->iq);

    return 0;
}

static void *
read_thread(
    void *priv)
{
    struct thread_context * const context = priv;

    // Wait for writer to create iqueue.
    while (go != 1);

    // Open iqueue for reading and signal that we're ready.
    context->iq = iqueue_open(context->filename, false);
    context->ready = true;

    // Wait for all readers to be ready.
    while (go != 2);

    uint64_t readed = 0;  // Because read is a keyword...
    while (readed < write_iters) {
        size_t len;
        const uint64_t *const msg = iqueue_data(context->iq, readed, &len);
        if (msg) {
            ++readed;
            uint64_t delta = tsclock_getnanos(0) - msg[2];
            context->total_time += delta;
        }
    }
    fprintf(stderr, "Average read for %d - %"PRIu64" ns\n",
        context->cpu,
        context->total_time / write_iters);

    iqueue_close(context->iq);

    return 0;
}

static void
read_test(
    const char *label,
    int nanosleep_time,
    int thread_count,
    int *thread_cpus)
{
    const char * filename = "/dev/shm/test3.iqx";
    unlink(filename);

    struct thread_context contexts[thread_count];

    for (int i = 0; i < thread_count; i++) {
        struct thread_context * const context = &contexts[i];
        context->iq = NULL;
        context->filename = filename;
        context->cpu = thread_cpus[i];
        context->nanosleep_time = nanosleep_time;
        context->total_time = 0;
        context->ready = false;

        pthread_create(&context->thread, NULL, i == 0 ? write_thread : read_thread, context);
        tssched_set_thread_affinity(context->thread, thread_cpus[i]);
    }

    // Wait for all readers to be ready.
    while (true) {
        bool all_done = true;
        for (int i = 1; i < thread_count; ++i) {
            if (!contexts[i].ready) {
                all_done = false;
                break;
            }
        }
        if (all_done) break;
    }

    // Signal to start.
    go = 2;

    // Reporting.
    uint64_t total_time = 0;
    for (int i = 0; i < thread_count; ++i)
    {
        struct thread_context * const context = &contexts[i];
        pthread_join(context->thread, NULL);
        if (i == 0) {
            fprintf(stderr, "%s %d %"PRIu64" ns/write\n",
                label,
                context->cpu,
                context->total_time / write_iters
                );
        } else {
            total_time += context->total_time;
        }
    }
    TSLOGX(TSINFO, "%s Average read: %.1lfns",
        label,
        (double)total_time / thread_count / write_iters);

    // Sometimes the file system is slow and not having this cause
    // weird issues when the next writer starts up.
    sleep(1);
}

int
main(
    int argc,
    char **argv)
{
    if (argc != 2) {
        TSLOGX(TSERROR, "usage: iqueue-read-contention-test <nanosleep time>");
        return 1;
    }

    int nanosleep_time = atoi(argv[1]);

    // The first CPU is the writer.
    read_test("same-8", nanosleep_time, 9,
        (int[]) { 1, 3, 5, 7, 9, 11, 13, 15, 17 });

    read_test("cross-8", nanosleep_time, 9,
        (int[]) { 1, 9, 10, 11, 12, 13, 14, 15, 16  });

    read_test("cross-16", nanosleep_time, 17,
        (int[]) { 1, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 21, 30, 31, 32, 33, 34 });

    return 0;
}
