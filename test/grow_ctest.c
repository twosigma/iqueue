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
 * Create multiple threads all allocating new spcae in the iqueue
 * to stress the sparse growing routines.
 */
#include "twosigma.h"

#include "ctest.h"
#include "ctest_resource.h"

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



static void *
allocate_thread(
    void * const iq_ptr
)
{
    iqueue_t * const iq = iq_ptr;
    uint64_t count = 0;

    while (1)
    {
        iqueue_msg_t msg;
        const void * buf = iqueue_allocate_raw(iq, 1 << 16, &msg);
        if (!buf)
            break;
        count++;
    }

    TSLOGXL(TSINFO, "Created %"PRIu64" sections", count);
    return NULL;
}



TS_ADD_TEST(test)
{
    tslevel = TSDEBUG;

    char *dir = ts_test_resource_get_dir("iqueue_grow");
    char *iqx_file = NULL;
    if (asprintf(&iqx_file, "%s/test.iqx", dir) == -1)
        TSABORTX("failed to asprintf");

    iqueue_t * const iq = iqueue_create(iqx_file, 0, NULL, 0);
    TS_TEST_ASSERT(iq);

    TSLOGXL(TSINFO, "%s: Finished map", iqx_file);

    const int num_threads = 8;
    pthread_t threads[num_threads];
    for (int i = 0 ; i < num_threads ; i++)
    {
        if (pthread_create(&threads[i], NULL, allocate_thread, iq) < 0)
            TSABORT("thread create");
    }

    for (int i = 0 ; i < num_threads ; i++)
        pthread_join(threads[i], NULL);

    unlink(iqx_file);

    return true;
}
