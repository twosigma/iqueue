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
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#ifdef _REENTRANT
#include <pthread.h>
#endif
#include <ctype.h>
#include <err.h>
#include <unistd.h>
#include <sys/syscall.h>

#include <bsd/stdlib.h>

#include "tslog.h"
#include "tstl.h"

#define BUF_SIZE 2048
static tstl_t threadlocal_buf = TSTL_BUF_INITIALIZER(BUF_SIZE);

static const char *oldlogstr = PREF_LONG;
static const char *newlogstr = DEFAULT_TSLOG_FORMAT;

/*
 * %%:  %
 * %D:	Datetime+Zone
 * %F:	File
 * %L:	Line
 * %N:	Program name
 * %P:	File with Path
 * %f:	Function
 * %T:	Type
 */
int
setlogstr(const char *fmt)
{
    const char *p;
    for (p = fmt; *p; p++)
	if (*p == '%' && strchr("%DFLNPfT", *++p) == NULL) {
	    if (*p)
		warnx("Unknown escape `%c'", *p);
	    else
		warnx("Missing character after `%%'");
	    return -1;
	}

    oldlogstr = newlogstr = fmt;
    return 0;
}

static const char *
getlogstr0(const char *file, int line, const char *func, int type, const char *logstr)
{
    char datetime[64];
    char scratch[64];
    struct timeval tv;
    struct tm tmb;
    struct tm *tmp;
    time_t t;
    int oerrno = errno;
    const char *p, *ptr;
    char *buffer = tstl_get(&threadlocal_buf);
    char *b = buffer, *eb = buffer + BUF_SIZE;

    assert(buffer != NULL);

    (void)gettimeofday(&tv, NULL);
    t = (time_t)tv.tv_sec;
    tmp = localtime_r(&t, &tmb);
    if (tmp == NULL)
	(void)snprintf(scratch, sizeof(scratch), "XXXX-XX-XX XX:XX:XX.XXX XXX");
    else
	(void)strftime(scratch, sizeof(scratch),
	"%Y-%m-%d %H:%M:%S.%%.3d %Z", tmp);
    (void)snprintf(datetime, sizeof(datetime), scratch, tv.tv_usec / 1000);

#define ADDC(c) \
    do { \
	if (b == eb) { \
	    b--; \
	    goto out; \
	} \
	*b++ = c; \
    } while (/*CONSTCOND*/0)

#define ADDS(s) \
    do { \
	const char *_s = s; \
	while (*_s) { \
	    if (b == eb) { \
		b--; \
		goto out; \
	    } \
	    *b++ = *_s++; \
	} \
    } while (/*CONSTCOND*/0)

    for (p = logstr; *p; p++) {
	if (*p != '%') {
	    ADDC(*p);
	    continue;
	}
	switch (*++p) {
	case '%':
	    ADDC(*p);
	    break;
	case 'D':
	    ADDS(datetime);
	    break;
	case 'F':
	    if ((ptr = strrchr(file, '/')) != NULL)
		ptr++;
	    else
		ptr = file;
	    ADDS(ptr);
	    break;
	case 'L':
	    (void)snprintf(scratch, sizeof(scratch), "%d", line);
	    ADDS(scratch);
	    break;
	case 'N':
	    ADDS(getprogname());
	    break;
	case 'P':
	    ADDS(file);
	    break;
	case 'f':
	    ADDS(func);
	    break;
	case 'T':
	    switch (type) {
	    case -1:
		if (isspace((unsigned char)p[1]))
		    p++;
		break;
	    case TSFATAL:
		ADDS("ABORT");
		break;
	    case TSERROR:
		ADDS("ERROR");
		break;
	    case TSWARN:
		ADDS("WARN");
		break;
	    case TSINFO:
		ADDS("INFO");
		break;
	    case TSDEBUG:
		ADDS("DEBUG");
		break;
	    case TSDIAG:
		ADDS("DIAG");
		break;
	    default:
		warnx("unknown error type %d", type);
		break;
	    }
	    break;
	case 't':
#ifdef __linux__
	    /* leave a noop case even if not __linux__ to avoid an abort on %t */
	    (void)snprintf(scratch, sizeof(scratch), "%ld", (long) syscall(__NR_gettid));
	    ADDS(scratch);
#endif
	    break;
	default:
	    warnx("unknown log formatting character `%c'", *p);
	    break;
	}
    }
out:
    ADDC('\0');
    errno = oerrno;
    return buffer;
}

const char *
getlogstr(const char *file, int line, const char *func, int type)
{
    return getlogstr0(file, line, func, type, oldlogstr);
}

const char *
getnewlogstr(const char *file, int line, const char *func, int type)
{
    return getlogstr0(file, line, func, type, newlogstr);
}
