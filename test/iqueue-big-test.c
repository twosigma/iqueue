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
 * Create a big iqueue, way bigger than available memory to test page
 * cache reuse.
 */
#include "twosigma.h"
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <err.h>
#include <semaphore.h>
#include <pthread.h>
#include "tslog.h"
#include "tsclock.h"
#include "iqueue.h"
#include "segfault.h"
#include "atomic.h"



static int ftrace_enabled_fd = -1;
static int ftrace_marker_fd = -1;

static void
ftrace_open(void)
{
    const char * const enabled_file = "/sys/kernel/debug/tracing/tracing_enabled";
    const char * const marker_file = "/sys/kernel/debug/tracing/trace_marker";
    ftrace_enabled_fd = open(enabled_file, O_WRONLY);
    if (ftrace_enabled_fd < 0)
        warn("%s: Unable to open (ignored)", enabled_file);

    ftrace_marker_fd = open(marker_file, O_WRONLY);
    if (ftrace_marker_fd < 0)
        warn("%s: Unable to open (ignored)", marker_file);
}


static void
ftrace_stop(void)
{
    if (ftrace_enabled_fd < 0)
        return;

    ssize_t wlen = write(ftrace_enabled_fd, "0\n", 2);
    if (wlen == 2)
        return;

    perror("ftrace_enabled");
    close(ftrace_enabled_fd);
    ftrace_enabled_fd = -1;
}


static void
__attribute__((__format__(__printf__, 1, 2)))
ftrace_mark(
    const char * fmt,
    ...
)
{
    if (ftrace_marker_fd < 0)
        return;

    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    int len = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    size_t wlen = write(ftrace_marker_fd, buf, len);
    if (wlen > 0)
        return;

    perror("ftrace_marker");
    close(ftrace_marker_fd);
    ftrace_marker_fd = -1;
}


static sem_t prefetch_sem;
static uint64_t last_map_offset;

static void *
prefetch_thread(
    void * const iq_ptr
)
{
    iqueue_t * const iq = iq_ptr;
    const uint64_t map_chunk = 1 << 29;

    while (1)
    {
        int rc = sem_wait(&prefetch_sem);
        if (rc < 0)
        {
            if (errno == EINTR || errno == EAGAIN || errno == ETIMEDOUT)
                continue;
            break;
        }

        // Map 512 MB ahead of the current location
        const uint64_t mask = (1 << 29) - 1;
        uint64_t map_offset = (last_map_offset + mask + 1) & ~mask;

        //TSLOGXL(TSINFO, "%s: prefetching %"PRIx64, iqueue_name(iq), map_offset);
        uint64_t prefetch_time = -tsclock_getnanos(0);

        uint64_t * data = (void*)(uintptr_t) iqueue_get_data(iq, map_offset, 1);
        if (!data)
            break;

        for (uint64_t offset = 0 ; offset < map_chunk / 8 ; offset += 4096 / 8)
            atomic_cas_64(data + offset, 0, 0);

        prefetch_time += tsclock_getnanos(0);
        TSLOGXL(TSINFO, "%s: prefetched %"PRIx64" - %"PRIx64": %"PRIu64" ns",
            iqueue_name(iq), map_offset, map_offset + map_chunk, prefetch_time);
    }

    TSLOGXL(TSERROR, "%s: prefetch exiting at offset %"PRIu64,
        iqueue_name(iq),
        last_map_offset
    );

    return NULL;
}


int
main(
    int argc,
    char ** argv
)
{
    segfault_handler_install();
    ftrace_open();

    const uint64_t report_interval = 1000e6; // ns
    const uint64_t target_rate = 750000; // ops/sec
    const uint64_t ns_per_op = 1e9 / target_rate;
    const uint64_t spike = 1e6; // 1 ms == big spike

    const size_t msg_len = 200;
    uint64_t data_len = 100ul << 30;
    const uint64_t msg_count = data_len / msg_len;

    const char * const iqx_file = argc > 1 ? argv[1] : "/blink/test/huge-test.iqx";
    unlink(iqx_file);
    iqueue_t * const iq = iqueue_create(iqx_file, 0, NULL, 0);

    if (!iq)
        TSABORTX("%s: Unable to create", iqx_file);

    TSLOGXL(TSINFO, "%s: Finished map", iqx_file);

    iqueue_allocator_t allocator;
    if (iqueue_allocator_init(iq, &allocator, 1 << 20, 1) < 0)
        TSABORTX("%s: Unable to create allocator", iqx_file);

    allocator.align_mask = 32 - 1;

if (0) {
    pthread_t thread;
    sem_init(&prefetch_sem, 0, 1);
    last_map_offset = -1; // force immediate load of first region
    if (1) pthread_create(&thread, NULL, prefetch_thread, iq);
    sleep(1);
}

    pthread_t thread;
    if (1) iqueue_prefetch_thread(iq, &thread);

    iqueue_id_t id = -1;

    uint64_t sum_count = 0;
    uint64_t allocate_sum = 0;
    uint64_t memcpy_sum = 0;
    uint64_t update_sum = 0;
    uint64_t allocate_max = 0;
    uint64_t memcpy_max = 0;
    uint64_t update_max = 0;
    uint64_t allocate_spike = 0;
    uint64_t memcpy_spike = 0;
    uint64_t update_spike = 0;
    uint64_t start = tsclock_getnanos(0);
    uint64_t last_report = start;
    uint64_t last_op = 0;

    while (id+1 < msg_count)
    {
        // Wait for the next time slice;
        uint64_t allocate_start;
        do {
            allocate_start = tsclock_getnanos(0);
        } while (allocate_start < last_op + ns_per_op);
        last_op = allocate_start;

        iqueue_msg_t iqmsg;
        uint64_t * const msg = iqueue_allocate(&allocator, msg_len, &iqmsg);
        if (!msg)
            break;

        const uint64_t offset = iqueue_msg_offset(iqmsg);

        if ((offset >> 29) != (last_map_offset >> 29))
        {
            last_map_offset = offset;
            sem_post(&prefetch_sem);
        }

        uintptr_t msg_base = (uintptr_t) msg;
        if (msg_base & 0x1F)
            TSABORTX("%s: message is not aligned: %p", iqx_file, msg);

        const uint64_t memcpy_start = tsclock_getnanos(0);

        memset(msg, (++id) & 0xFF, msg_len);
        msg[1] = memcpy_start;

        const uint64_t update_start = tsclock_getnanos(0);

        if (0) ftrace_mark("iqueue update %"PRIx64, offset);

        int rc = iqueue_update_be(iq, iqmsg, (iqueue_id_t*) &msg[0]);
        if (rc != 0)
        {
            TSLOGXL(TSINFO, "%s: %p update failed: rc=%d", iqx_file, msg, rc);
            break;
        }

        const uint64_t validate_start = tsclock_getnanos(0);

        const uint64_t msg_id = be64toh(msg[0]);
        if (msg_id != id)
            TSABORTX("%s: %"PRIu64" != expected index %"PRIu64, iqx_file, msg_id, id);

        const uint64_t allocate_time = memcpy_start - allocate_start;
        const uint64_t memcpy_time = update_start - memcpy_start;
        const uint64_t update_time = validate_start - update_start;
        allocate_sum += allocate_time;
        memcpy_sum += memcpy_time;
        update_sum += update_time;
        sum_count++;

        if (allocate_time > allocate_max)
            allocate_max = allocate_time;
        if (allocate_time > spike)
        {
            ftrace_mark("allocate spike %"PRIu64" iqueue offset %"PRIx64"\n",
                allocate_time,
                offset
            );
            ftrace_stop();
            allocate_spike++;
        }

        if (memcpy_time > memcpy_max)
            memcpy_max = memcpy_time;
        if (memcpy_time > spike)
        {
            ftrace_mark("memcpy spike %"PRIu64" iqueue offset %"PRIx64"\n", memcpy_time, offset);
            ftrace_stop();
            memcpy_spike++;
        }

        if (update_time > update_max)
            update_max = update_time;
        if (update_time > spike)
        {
            ftrace_mark("update spike %"PRIu64" iqueue offset %"PRIx64"\n", update_time, offset);
            ftrace_stop();
            update_spike++;
        }

        const uint64_t report_delta = validate_start - last_report;
        if (report_delta < report_interval)
            continue;

        ftrace_mark("iqueue offset %"PRIx64"\n", offset);

        TSLOGXL(TSINFO, "%"PRIx64" %.3f%%: %.3f kops/sec %.3f MB/s:"
            " allocate %"PRIu64" (max %"PRIu64", %"PRIu64" spikes)"
            " memcpy %"PRIu64" (max %"PRIu64", %"PRIu64" spikes)"
            " update %"PRIu64" (max %"PRIu64", %"PRIu64" spikes)"
            "%s",
            offset,
            (id * 100.0) / msg_count,
            (sum_count * 1.0e9 / 1.0e3) / report_delta,
            (sum_count * msg_len * 1.0e9 / 1.0e6) / report_delta,
            allocate_sum / sum_count,
            allocate_max,
            allocate_spike,
            memcpy_sum / sum_count,
            memcpy_max,
            memcpy_spike,
            update_sum / sum_count,
            update_max,
            update_spike,
            allocate_spike || memcpy_spike || update_spike ? " !!!!" : ""
        );

        last_report = validate_start;
        allocate_sum = memcpy_sum = update_sum = 0;
        allocate_max = memcpy_max = update_max = 0;
        allocate_spike = memcpy_spike = update_spike = 0;
        sum_count = 0;
    }

    last_report = tsclock_getnanos(0);
    allocate_sum = memcpy_sum = update_sum = 0;
    allocate_max = memcpy_max = update_max = 0;
    allocate_spike = memcpy_spike = update_spike = 0;
    sum_count = 0;

#if 0
    TSLOGXL(TSINFO,
        "%s: Added %"PRIu64" entries, expected %"PRIu64", %.2f%% waste",
        iqx_file,
        id,
        data_len / msg_len,
        100.0 - (id * msg_len * 100.0) / data_len
    );
#endif

    // Read all of the elements and verify that they have no overlap
    id = -1;
    while (1)
    {
        const uint64_t read_start = tsclock_getnanos(0);

        size_t len;
        const uint64_t * const msg = iqueue_data(iq, ++id, &len);
        if (!msg)
            break;

        const uint64_t validate_start = tsclock_getnanos(0);

        if (len != msg_len)
            TSABORTX("%"PRIu64": bad length %zu?", id, len);

        const uint64_t msg_id = be64toh(msg[0]);
        if (msg_id != id)
            TSABORTX("%"PRIu64": bad id %"PRIu64"?", id, msg_id);

        // Verify that all of the message is correct
        const uint8_t * const buf = (const uint8_t*) msg;
        for (size_t i = 16 ; i < msg_len ; i++)
            if (buf[i] != (id & 0xFF))
                TSABORTX("%"PRIu64": offset %zu bad value", id, i);

        const uint64_t validate_end = tsclock_getnanos(0);

        const uint64_t read_time = validate_start - read_start;
        const uint64_t validate_time = validate_end - validate_start;
        allocate_sum += read_time;
        memcpy_sum += validate_time;
        sum_count++;

        if (read_time > allocate_max)
            allocate_max = read_time;
        if (read_time > spike)
            allocate_spike++;
        if (validate_time > memcpy_max)
            memcpy_max = validate_time;
        if (validate_time > spike)
            memcpy_spike++;

        const uint64_t report_delta = validate_end - last_report;
        if (report_delta < report_interval)
            continue;

        TSLOGXL(TSINFO, "%.3f%%: %.3f kops/sec %.3f MB/s:"
            " read %"PRIu64" (max %"PRIu64", %"PRIu64" spikes)"
            " memcmp %"PRIu64" (max %"PRIu64", %"PRIu64" spikes)"
            "",
            (id * 100.0) / msg_count,
            (sum_count * 1.0e9 / 1.0e3) / report_delta,
            (sum_count * msg_len * 1.0e9 / 1.0e6) / report_delta,
            allocate_sum / sum_count,
            allocate_max,
            allocate_spike,
            memcpy_sum / sum_count,
            memcpy_max,
            memcpy_spike
        );

        last_report = validate_start;
        allocate_sum = memcpy_sum = update_sum = 0;
        allocate_max = memcpy_max = update_max = 0;
        allocate_spike = memcpy_spike = update_spike = 0;
        sum_count = 0;
    }

    TSLOGXL(TSINFO, "%s: all messages checked out ok", iqx_file);

    iqueue_close(iq);

    return 0;
}
