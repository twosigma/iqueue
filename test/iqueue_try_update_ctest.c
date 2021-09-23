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
 * Verify that iqueue_try_update() returns the correct values.
 */
#include "twosigma.h"

#include "tslog.h"
#include "ctest.h"
#include "ctest_resource.h"

#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include "iqueue.h"
#include "tsclock.h"


TS_ADD_TEST(test)
{
    char *dir = ts_test_resource_get_dir("iqueue_try_update");
    char *filename = NULL;
    if (asprintf(&filename, "%s/test.iqx", dir) == -1)
        TSABORTX("failed to asprintf");

    iqueue_t * const iq = iqueue_create(filename, 0, NULL, 0);
    TS_TEST_ASSERT(iq);

    shash_t * const table = iqueue_writer_table(iq, 0, 1);
    TS_TEST_ASSERT(table);

    shash_entry_t * const writer = shash_insert(table, 9999, 1234);
    TS_TEST_ASSERT(writer);

    iqueue_writer_update(table, writer, 1233);
    iqueue_writer_update(table, writer, 1234);
    iqueue_writer_update(table, writer, 1235);
    iqueue_writer_update(table, writer, 0);
    iqueue_writer_update(table, writer, 1);
    iqueue_writer_update(table, writer, 2);

    TS_TEST_EQUALS(iqueue_entries(iq), 0);

    iqueue_append(iq, "zero", 5);
    iqueue_append(iq, "one", 4);
    iqueue_append(iq, "two", 4);
    iqueue_append(iq, "three", 6);

    iqueue_msg_t iqmsg;
    char * const msg = iqueue_allocate_raw(iq, 5, &iqmsg);
    TS_TEST_ASSERT(msg);

    msg[0] = 'f';
    msg[1] = 'o';
    msg[2] = 'u';
    msg[3] = 'r';
    msg[4] = '\0';


    // Try to append at index 4, which should be ok
    TS_TEST_EQUALS(iqueue_try_update(iq, 4, iqmsg), 0);

    // Try to append at index 4 again, which should fail
    TS_TEST_EQUALS(iqueue_try_update(iq, 4, iqmsg), IQUEUE_STATUS_HAS_DATA);

    // Try to append at index 6, which would leave a hole
    TS_TEST_EQUALS(iqueue_try_update(iq, 6, iqmsg), IQUEUE_STATUS_INDEX_INVALID);

    // Seal the iqueue at an invalid index
    TS_TEST_EQUALS(iqueue_try_seal(iq, 6), IQUEUE_STATUS_INDEX_INVALID);

    // Seal it too early
    TS_TEST_EQUALS(iqueue_try_seal(iq, 4), IQUEUE_STATUS_HAS_DATA);

    // Seal it just right
    TS_TEST_EQUALS(iqueue_try_seal(iq, 5), IQUEUE_STATUS_OK);

    // Verify that iqueue_try_update fails now
    TS_TEST_EQUALS(iqueue_try_update(iq, 4, iqmsg), IQUEUE_STATUS_HAS_DATA);

    TS_TEST_EQUALS(iqueue_try_update(iq, 5, iqmsg), IQUEUE_STATUS_SEALED);

    // Try to append at index 7, which would leave a hole
    TS_TEST_EQUALS(iqueue_try_update(iq, 7, iqmsg), IQUEUE_STATUS_INDEX_INVALID);

    // Try to append at index 6, which would be ok, except that the
    // iqueue is sealed
    TS_TEST_EQUALS(iqueue_try_update(iq, 6, iqmsg), IQUEUE_STATUS_INDEX_INVALID);

    return true;
}
