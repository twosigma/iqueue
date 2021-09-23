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
/*
 * Test that symlink resolution for iqueue create works.
 */
#include "twosigma.h"
#include "tslog.h"

#include <unistd.h>

#include "ctest.h"
#include "ctest_resource.h"

#include "iqueue.h"

TS_ADD_TEST(test)
{
    tslevel = TSDIAG;
    char *dir = ts_test_resource_get_dir("iqueue_symlink");

    char *symlink0 = NULL;
    if (asprintf(&symlink0, "%s/symlink0", dir) == -1)
        TSABORTX("failed to asprintf");

    char *symlink1 = NULL;
    if (asprintf(&symlink1, "%s/symlink1", dir) == -1)
        TSABORTX("failed to asprintf");

    char *dest_iqx = NULL;
    if (asprintf(&dest_iqx, "%s/dest.iqx", dir) == -1)
        TSABORTX("failed to asprintf");

    // Case 1: symlink1 -> symlink2 -> dest_iqx.
    // This should create a new iqueue in dest_iqx.
    if (symlink(symlink1, symlink0) == -1)
	TSABORT("failed to symlink");

    if (symlink(dest_iqx, symlink1) == -1)
	TSABORT("failed to symlink");

    iqueue_t * const iq1 = iqueue_create(symlink0, 0, NULL, 0);
    TS_TEST_ASSERT(iq1);
    TS_TEST_ASSERT(access(dest_iqx, F_OK) == 0);

    iqueue_append(iq1, "test", 5); // Add an entry for the check later.
    iqueue_close(iq1);

    // Case 1: symlink1 -> symlink2 -> dest_iqx.
    // This should reopen the iqueue created in the previous step.
    iqueue_t * const iq2 = iqueue_create(symlink0, 0, NULL, 0);
    TS_TEST_ASSERT(iq2);
    TS_TEST_ASSERT(iqueue_entries(iq2) == 1);
    iqueue_close(iq2);

    unlink(dest_iqx);
    unlink(symlink1);
    unlink(symlink0);

    // Case 3: symlink1 -> symlink2 -> symlink1.
    // This should detect the loop and fail.
    if (symlink(symlink1, symlink0) == -1)
	TSABORT("failed to symlink");

    if (symlink(symlink0, symlink1) == -1)
	TSABORT("failed to symlink");

    iqueue_t * const iq3 = iqueue_create(symlink0, 0, NULL, 0);
    TS_TEST_ASSERT(!iq3);
    unlink(symlink1);
    unlink(symlink0);

    TSLOGX(TSINFO, "Test passed");
    return true;
}
