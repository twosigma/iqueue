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
#include <errno.h>
#include "sys/wait.h"

static inline bool
exec(char *cmd, const char *expected_output)
{
    FILE *proc = popen(cmd, "re");
    TS_TEST_ASSERT(proc);
    char* output = NULL;
    size_t n = 0;
    errno = 0;
    TS_TEST_ASSERT(getdelim(&output, &n, 0, proc) < 0 || errno == 0);
    pclose(proc);

    if (expected_output) {
        TS_TEST_ASSERT(strstr(output, expected_output) != NULL);
    }
    free(output);
    return true;
}

TS_ADD_TEST(test)
{
    char *dir = ts_test_resource_get_dir("iqmod");

    char *test_iqx = NULL;
    TS_TEST_ASSERT(asprintf(&test_iqx, "%s/test.iqx", dir) != -1);

    char cmd[2000];

    // create empty iqueue
    TS_TEST_ASSERT(sprintf(cmd, "./iqueue -C -f %s", test_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, NULL));

    // mod creation_time
    TS_TEST_ASSERT(sprintf(cmd, "./iqmod_inplace -c 12345 %s", test_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, NULL));
    TS_TEST_ASSERT(sprintf(cmd, "./iqueue -s -f %s", test_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, "12345"));

    // mod table in place
    TS_TEST_ASSERT(sprintf(cmd, "./iqmod_inplace -s 0:987654321:1500000000000000000 %s", test_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, NULL));
    TS_TEST_ASSERT(sprintf(cmd, "./iqueue -s -f %s", test_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, "3ade68b1: 1500000000000000000"));
    TS_TEST_ASSERT(sprintf(cmd, "./iqmod_inplace -s 0:987654321:1500000000000000001 %s", test_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, NULL));
    TS_TEST_ASSERT(sprintf(cmd, "./iqueue -s -f %s", test_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, "3ade68b1: 1500000000000000001"));

    // mod table copy
    char *mod_iqx = NULL;
    TS_TEST_ASSERT(asprintf(&mod_iqx, "%s/mod.iqx", dir) != -1);

    TS_TEST_ASSERT(sprintf(cmd, "./iqmod_copy -s 0:987654322:1500000000000000000 %s %s", test_iqx, mod_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, NULL));
    TS_TEST_ASSERT(sprintf(cmd, "./iqueue -s -f %s", mod_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, "3ade68b2: 1500000000000000000"));
    TS_TEST_ASSERT(sprintf(cmd, "rm -rf %s", mod_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, NULL));

    TS_TEST_ASSERT(sprintf(cmd, "./iqmod_copy -x 0:987654322 %s %s", test_iqx, mod_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, NULL));
    TS_TEST_ASSERT(sprintf(cmd, "./iqueue -s -f %s", mod_iqx) > 0);
    TS_TEST_ASSERT(exec(cmd, "3ade68b1: 1500000000000000001"));

    return true;
}
