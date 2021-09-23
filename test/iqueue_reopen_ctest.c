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
 * Test that the sealing and re-opening functions work
 */
#include "twosigma.h"

#include "ctest.h"
#include "ctest_resource.h"

#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <inttypes.h>
#include "tslog.h"
#include "iqueue.h"

TS_ADD_TEST(test)
{
    tslevel = TSDIAG;

    char *dir = ts_test_resource_get_dir("iqueue_reopen");
    char *iq_file = NULL;
    if (asprintf(&iq_file, "%s/reopen.iqx", dir) == -1)
        TSABORTX("failed to asprintf");

    iqueue_t * const iq = iqueue_create(iq_file, 0, NULL, 0);
    TS_TEST_ASSERT(iq);

    const size_t msg_len = 16;
    uint64_t id;

    {
        iqueue_msg_t iqmsg;
        uint64_t * const msg = iqueue_allocate_raw(iq, msg_len, &iqmsg);
        TS_TEST_ASSERT(msg);

        msg[0] = getpid();

        TS_TEST_ASSERT(iqueue_update(iq, iqmsg, &id) == 0);
        TS_TEST_EQUALS(id, 0);

        TSLOGXL(TSINFO, "%s: posted %"PRIu64" at %p", iq_file, id, msg);
    }

    const uint64_t creation = iqueue_creation(iq);
    TS_TEST_ASSERT(iqueue_reopen(iq) == 0);
    TS_TEST_EQUALS(iqueue_creation(iq), creation);

    {
        size_t len;
        const uint64_t * const msg = iqueue_data(iq, id, &len);
        TS_TEST_ASSERT(msg);

        TS_TEST_EQUALS(len, msg_len);

        TS_TEST_EQUALS(msg[0], (uint64_t) getpid());

        TSLOGXL(TSINFO, "%s: read %"PRIu64" at %p", iq_file, id, msg);
    }

    TSLOGXL(TSINFO, "%s: reopen worked", iq_file);

    const iqueue_id_t seal_id = iqueue_entries(iq);
    TS_TEST_ASSERT(iqueue_archive(iq, seal_id) == 0);

    int rc = iqueue_reopen(iq);
    TS_TEST_EQUALS(rc, -1);
    TS_TEST_EQUALS(errno, ENOENT);

    // Success (in that we failed to reopen)
    iqueue_t * const new_iq = iqueue_create(iq_file, 0, NULL, 0);
    TS_TEST_ASSERT(new_iq);

    rc = iqueue_reopen(iq);
    TS_TEST_EQUALS(rc, 0);

    TS_TEST_ASSERT(iqueue_creation(iq) != creation);
    TS_TEST_EQUALS(iqueue_creation(iq), iqueue_creation(new_iq));

    TS_TEST_EQUALS(iqueue_entries(iq), 0);

    const size_t namelen = strlen(iq_file) + 32;
    char * archive_file = calloc(1, namelen);
    TS_TEST_ASSERT(archive_file);

    snprintf(archive_file, namelen,
        "%s.%"PRIu64,
        iq_file,
        creation
    );

    iqueue_close(iq);
    iqueue_close(new_iq);
    unlink(iq_file);
    unlink(archive_file);

    TSLOGXL(TSINFO, "Test passed");

    return true;
}
