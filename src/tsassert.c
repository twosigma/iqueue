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

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <execinfo.h>

#include "tsassert.h"
#include "tslog.h"

void
tsassert_fail(
    const char * const assertion,
    const char * const file,
    const unsigned int line,
    const char * const func,
    const char * fmt,
    ...
)
{
    tslog_info_t info = {
	.file	    = file,
	.line	    = line,
	.func	    = func,
	.level	    = TSFATAL,
	.do_perror  = 0,
    };

    if (fmt != NULL) {
        va_list ap;
        va_start(ap, fmt);
        tslog_info_vargs(&info, fmt, ap);
        va_end(ap);
    }

    tslog_info(&info, "Assertion `%s' failed. Backtrace:", assertion);

    void * buffer[100];
    int frames = backtrace(buffer, __arraycount(buffer));
    char ** strings = backtrace_symbols(buffer, frames);
    if (!strings)
	TSABORT("backtrace failed");

    // Start at 1 so that the tsassert does not show in the backtrace
    for (int i = 1 ; i < frames ; i++)
	tslog_info(&info, "%d: %s", i, strings[i]);

    abort();
}
