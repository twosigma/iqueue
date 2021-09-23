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

#include "tslog.h"
#include "ctest.h"

#include "shash.h"
#include "tsclock.h"
#include <inttypes.h>
#include <pthread.h>

static volatile int go = 0;
static const uint64_t max_iters = 1 << 24;
static const size_t buf_size = 4096;

static void *
thread_test(
    void * const sh_ptr
)
{
    shash_t * const sh = sh_ptr;
    uint64_t now = tsclock_getnanos(0);
    if (shash_insert(sh, now, now) == NULL)
        TSABORTX("unable to insert key");

    while (!go)
        ;

    shash_entry_t * e = shash_insert(sh, 1234, 0);
    if (e)
    {
        TSLOGXL(TSINFO, "I won the entry");
    } else {
        e = shash_get(sh, 1234);
        if (!e)
            TSABORTX("no entry?");
    }

    uint64_t tries = 0;
    const uint64_t start_time = tsclock_getnanos(0);

    for (unsigned i = 0 ; i < max_iters ; i++)
    {
        uint64_t val;
        do {
            val = e->value;
            tries++;
        } while (shash_update(sh, e, val, val+1) == 0);
    }

    const uint64_t end_time = tsclock_getnanos(0);
    const uint64_t delta_time = end_time - start_time;

    TSLOGXL(TSINFO, "%"PRIu64" tries, %.2f avg tries/iter, %"PRIu64" ns/try",
        tries,
        tries / (double) max_iters,
        delta_time / tries
    );

    return NULL;
}


static void
test_threaded(
    void * const buf,
    const int shared,
    const unsigned max_threads
)
{
    pthread_t threads[max_threads];
    shash_t * hashes[max_threads];
    if (shared)
        hashes[0] = shash_create(buf, buf_size, 0);

    for (unsigned i = 0 ; i < max_threads; i++)
    {
        shash_t * sh;
        if (!shared)
            sh = hashes[i] = shash_create(buf, buf_size, 0);
        else
            sh = hashes[0];

        if (pthread_create(&threads[i], NULL, thread_test, sh) < 0)
            TSABORT("thread create");
    }

    go = 1;
    for (unsigned i = 0 ; i < max_threads; i++)
        pthread_join(threads[i], NULL);

    shash_entry_t * const e = shash_get(hashes[0], 1234);
    if (!e)
        TSABORTX("no entry?");
    const uint64_t value = e->value;
    if (value != max_threads * max_iters)
        TSABORTX("value %"PRIu64" != expected %"PRIu64,
            value,
            max_threads * max_iters
        );

    if (!shash_update(hashes[0], e, value, 0))
        TSABORTX("value changed during check?");

    TSLOGXL(TSINFO, "%s threaded test passed", shared ? "shared" : "private");
}


static void
test_simple(
    void * const buf
)
{
    shash_t * const sh = shash_create(buf, buf_size, 0);
    if (!sh)
        TSABORTX("shash_create");

    if (shash_get(sh, 9) != NULL)
        TSABORTX("get should have failed");
    shash_entry_t * e1 = shash_insert(sh, 9, 2345);
    if (!e1)
        TSABORTX("insert should have passed");
    if (e1->key != 9)
        TSABORTX("bad key: %"PRIu64, e1->key);
    if (e1->value != 2345)
        TSABORTX("bad value: %"PRIu64, e1->value);
    if (shash_insert(sh, 9, 23498) != NULL)
        TSABORTX("insert should have failed");
    if (shash_update(sh, e1, 2344, 2346))
        TSABORTX("update should have failed");
    if (!shash_update(sh, e1, 2345, 2346))
        TSABORTX("update should have passed");

    if (shash_get(sh, 10) != NULL)
        TSABORTX("get should have failed");
    shash_entry_t * e2 = shash_insert(sh, 10, 9999);
    if (!e2)
        TSABORTX("insert should have passed");
    if (e2->key != 10)
        TSABORTX("bad key: %"PRIu64, e2->key);
    if (e2->value != 9999)
        TSABORTX("bad value: %"PRIu64, e2->value);
    if (!shash_update(sh, e2, 9999, 3))
        TSABORTX("update should have passed");
    if (shash_update(sh, e2, 9999, 3))
        TSABORTX("update should have failed");

    if (shash_get(sh, 9) != e1)
        TSABORTX("get did not return same entry");
    if (shash_get(sh, 10) != e2)
        TSABORTX("get did not return same entry");

    shash_destroy(sh);
}


TS_ADD_TEST(test)
{
    void * const buf = calloc(1, buf_size);

    test_simple(buf);
    test_threaded(buf, 0, 8); // private local hashes
    test_threaded(buf, 1, 8); // shared local hashes

    for (unsigned max_threads = 1 ; max_threads < 8 ; max_threads++)
        test_threaded(buf, 0, max_threads);

    TSLOGXL(TSINFO, "Ok");
    return true;
}
