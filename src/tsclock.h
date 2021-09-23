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
#include <time.h>
#include <stdint.h>

typedef int64_t tsclock_nanos_t;
#define TIMESPEC_TO_NANOS(ts) \
    ((ts)->tv_sec * NANOS_IN_SECOND + (ts)->tv_nsec)

#define NANOS_IN_MICRO 1000LL
#define MICROS_IN_MILLI 1000LL
#define MILLIS_IN_SECOND 1000LL
#define NANOS_IN_MILLI (NANOS_IN_MICRO * MICROS_IN_MILLI)
#define NANOS_IN_SECOND (NANOS_IN_MILLI * MILLIS_IN_SECOND)

static inline tsclock_nanos_t __attribute__((__always_inline__))
tsclock_getnanos(int clockid)
{
    struct timespec ts;
    if (clock_gettime(clockid, &ts) == -1)
	return -1;
    return TIMESPEC_TO_NANOS(&ts);
}
