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
 * Test what happens when an unlinked file is fstated.
 */
#include "twosigma.h"

#include "tslog.h"
#include "ctest.h"
#include "ctest_resource.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

TS_ADD_TEST(test)
{
    char *dir = ts_test_resource_get_dir("unlink");
    char *file = NULL;
    if (asprintf(&file, "%s/test.dat", dir) == -1)
        TSABORTX("failed to asprintf");

    const int fd = open(file, O_RDWR | O_CREAT, 0666);
    TS_TEST_ASSERT(fd >= 0);

    struct stat st;

    TS_TEST_ASSERT(fstat(fd, &st) == 0);

    printf("%s: len=%zu\n", file, (size_t) st.st_size);
    TS_TEST_EQUALS(st.st_size, 0);

    TS_TEST_ASSERT(ftruncate(fd, 1 << 20) == 0);

    TS_TEST_ASSERT(fstat(fd, &st) == 0);

    printf("%s: len=%zu\n", file, (size_t) st.st_size);
    TS_TEST_EQUALS(st.st_size, 1 << 20);

    unlink(file);

    TS_TEST_ASSERT(fstat(fd, &st) == 0);

    printf("%s: len=%zu\n", file, (size_t) st.st_size);
    TS_TEST_EQUALS(st.st_size, 1 << 20);

    TS_TEST_ASSERT(ftruncate(fd, 2 << 20) == 0);

    TS_TEST_ASSERT(fstat(fd, &st) == 0);

    printf("%s: len=%zu\n", file, (size_t) st.st_size);
    TS_TEST_EQUALS(st.st_size, 2 << 20);

    close(fd);
    return true;
}
