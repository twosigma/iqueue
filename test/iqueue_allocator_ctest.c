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

#include "ctest.h"
#include "ctest_resource.h"

#include <stdio.h>
#include <unistd.h>
#include <inttypes.h>
#include "tslog.h"
#include "iqueue.h"

TS_ADD_TEST(refilling_allocator)
{
    char *dir = ts_test_resource_get_dir("refilling_allocator");
    char *iq_file = NULL;
    if (asprintf(&iq_file, "%s/align-test.iqx", dir) == -1)
        TSABORTX("failed to asprintf");

    iqueue_t * const iq = iqueue_create(iq_file, 0, NULL, 0);
    TS_TEST_ASSERT(iq);

    iqueue_allocator_t allocator;
    TS_TEST_ASSERT(iqueue_allocator_init(iq, &allocator, 1 << 16, 1) == 0);

    allocator.align_mask = 32 - 1;

    // Populate the iqueue with aligned messages
    const size_t alloc_len = 9403;
    const size_t msg_len = 1003;
    iqueue_id_t id = -1;
    const uint64_t msg_count = 1 << 20;

    while (++id < msg_count)
    {
        iqueue_msg_t iqmsg;
        uint64_t * const msg = iqueue_allocate(&allocator, alloc_len, &iqmsg);
        TS_TEST_ASSERT(msg);

        uintptr_t msg_base = (uintptr_t) msg;
        TS_TEST_ASSERT((msg_base & 0x1F) == 0);

        memset(msg, id & 0xFF, alloc_len);

        TS_TEST_ASSERT(iqueue_realloc(&allocator, &iqmsg, msg_len) >= 0);
        TS_TEST_ASSERT(iqueue_update_be(iq, iqmsg, (iqueue_id_t*) &msg[0]) == 0);

        const uint64_t msg_id = be64toh(msg[0]);
        TS_TEST_EQUALS(msg_id, id);

        TSLOGXL(TSDEBUG, "%"PRIu64": %p", msg[0], msg);
    }

    TSLOGXL(TSINFO,
        "%s: Added %"PRIu64" entries",
        iq_file,
        id
    );

    // Read all of the elements and verify that they have no overlap
    id = -1;
    while (1)
    {
        size_t len;
        const uint64_t * const msg = iqueue_data(iq, ++id, &len);
        if (id == msg_count) {
            TS_TEST_ASSERT(msg == 0);
            break;
        }

        TS_TEST_ASSERT(msg);

        TS_TEST_EQUALS(len, msg_len);

        const uint64_t msg_offset = (uintptr_t) msg;
        TS_TEST_ASSERT((msg_offset & allocator.align_mask) == 0);

        const uint64_t msg_id = be64toh(msg[0]);
        TS_TEST_EQUALS(msg_id, id);

        const uint8_t * const buf = (const uint8_t*) msg;
        for (size_t i = 8 ; i < msg_len ; i++) {
            TS_TEST_ASSERT(buf[i] == (id & 0xFF));
        }
    }

    TSLOGXL(TSINFO, "%s: all messages checked out ok", iq_file);

    iqueue_close(iq);
    unlink(iq_file);

    return true;
}

TS_ADD_TEST(nonrefilling_allocator)
{
    char *dir = ts_test_resource_get_dir("nonrefilling_allocator");
    char *iq_file = NULL;
    if (asprintf(&iq_file, "%s/nonrefilling-allocator-test.iqx", dir) == -1)
        TSABORTX("failed to asprintf");

    const size_t min_alloc_len = 1;
    const size_t max_alloc_len = 64;
    const unsigned min_message_count = 0;
    const unsigned max_message_count = 8;

    iqueue_t * const iq = iqueue_create(iq_file, 0, NULL, 0);
    TS_TEST_ASSERT(iq);

    iqueue_id_t id = -1;
    for (size_t alloc_len = min_alloc_len; alloc_len < max_alloc_len; alloc_len++)
    {
        for (unsigned message_count = min_message_count; message_count < max_message_count; message_count++)
        {
            const size_t bulk_len = alloc_len * message_count;
            iqueue_allocator_t allocator;
            TS_TEST_ASSERT(iqueue_allocator_init(iq, &allocator, bulk_len, 0) == 0);

            for (unsigned i = 0; i < message_count; i++)
            {
                iqueue_msg_t iqmsg;
                void * const msg = iqueue_allocate(&allocator, alloc_len, &iqmsg);
                TS_TEST_ASSERT(msg);

                memset(msg, ++id & 0xFF, alloc_len);

                iqueue_id_t msg_id;
                TS_TEST_ASSERT(iqueue_update(iq, iqmsg, &msg_id) == 0);

                TS_TEST_EQUALS(msg_id, id);

                TSLOGXL(TSDEBUG, "%"PRIu64": %p", msg_id, msg);
            }

            iqueue_msg_t iqmsg;
            TS_TEST_ASSERT(iqueue_allocate(&allocator, 1, &iqmsg) == NULL);
        }
    }

    // Read all of the elements and verify that they have no overlap
    id = -1;
    for (size_t msg_len = min_alloc_len; msg_len < max_alloc_len; msg_len++)
    {
        for (unsigned message_count = min_message_count; message_count < max_message_count; message_count++)
        {
            for (unsigned i = 0; i < message_count; i++)
            {
                size_t len;
                const void * const msg = iqueue_data(iq, ++id, &len);
                TS_TEST_ASSERT(msg);

                TS_TEST_EQUALS(len, msg_len);

                const uint8_t * const buf = (const uint8_t*) msg;
                for (size_t byte = 0 ; byte < msg_len ; byte++) {
                    TS_TEST_EQUALS(buf[byte], (id & 0xFF));
                }
            }
        }
    }

    TSLOGXL(TSINFO, "%s: all messages checked out ok", iq_file);

    iqueue_close(iq);
    unlink(iq_file);

    return true;
}
