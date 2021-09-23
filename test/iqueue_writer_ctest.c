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
 * Verify that the iqueue writer and heartbeat functions work.
 */
#include "twosigma.h"

#include "tslog.h"
#include "ctest.h"
#include "ctest_resource.h"

#include <stdio.h>
#include <unistd.h>
#include <inttypes.h>
#include <pthread.h>
#include "iqueue.h"
#include "tsclock.h"

TS_ADD_TEST(test)
{
    char *dir = ts_test_resource_get_dir("iqueue_writer");
    char *filename = NULL;
    if (asprintf(&filename, "%s/writer.iqx", dir) == -1)
        TSABORTX("failed to asprintf");

    iqueue_t * const iq = iqueue_create(filename, 0, NULL, 0);
    TS_TEST_ASSERT(iq);

    shash_t * const table = iqueue_writer_table(iq, 0, 1);
    TS_TEST_ASSERT(table);

    shash_entry_t * const writer = shash_insert(table, 9999, 1234);
    TS_TEST_ASSERT(writer);

    TS_TEST_ASSERT(iqueue_writer_update(table, writer, 1233) == 0);
    TS_TEST_ASSERT(iqueue_writer_update(table, writer, 1234) == 0);
    TS_TEST_ASSERT(iqueue_writer_update(table, writer, 1235) == 1);
    TS_TEST_ASSERT(iqueue_writer_update(table, writer, (uint64_t) -1) == 1);
    TS_TEST_ASSERT(iqueue_writer_update(table, writer, 1) == 1);
    TS_TEST_ASSERT(iqueue_writer_update(table, writer, 2) == 1);

    shash_entry_t * const writer2 = shash_insert_or_get(table, 9999, 1237);
    TS_TEST_ASSERT(writer2);
    TS_TEST_EQUALS(writer, writer2);

    TS_TEST_EQUALS(writer2->value, 2);

    unlink(filename);
    return true;
}
