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

#include <sys/param.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <err.h>
#include <time.h>
#include <pthread.h>
#include <ctype.h>

#include "tsdir.h"
#include "tslog.h"

static int logfd = STDERR_FILENO;
static FILE *logfp = NULL;

// YYYYMMDD-HHMMSS\0 = 15 characters
#define TIMESTAMP_SIZE 16

int tsdebug = 0;
int tsabort = TSFATAL;
int tslevel = TSINFO;

static const struct flock lock = {
    .l_type = F_WRLCK,
    .l_whence = SEEK_SET,
    .l_start = 0,
    .l_len = 0,
    .l_pid = 0,
};


typedef struct
{
    tslog_hookup_t handler;
    void * priv;
} tslog_hookup_entry_t;

static tslog_hookup_entry_t * hookup_entries;
static size_t hookup_count;


// write() has to be locked because calling it from multiple threads on same fp
// will result in lost/corrupt log entries.
static pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;

static size_t
tslog_strftime(char *buf, size_t len, const char *fmt, bool local)
{
    struct tm tm;
    time_t t = time(NULL);
    if (local)
	(void)localtime_r(&t, &tm);
    else
	(void)gmtime_r(&t, &tm);
    return strftime(buf, len, fmt, &tm);
}


int
tslogopen(const char *fname, int flag)
{
    char buf[MAXPATHLEN];

    (void)tslog_strftime(buf, sizeof(buf), fname, false);
    int fd = open(buf, O_CREAT|flag|O_RDWR, 0664);
    if (fd == -1) {
	warn("Cannot open `%s'", buf);
	return -1;
    }
    if (fcntl(fd, F_SETLK, &lock) == -1) {
	struct flock l;
	int oerrno = errno;
	l = lock;
	if (fcntl(fd, F_GETLK, &l) == -1)
	    warn("Can't get lock information");
	errno = oerrno;
	warn("Cannot obtain exclusive lock on `%s' (locked by %d)", fname,
	    l.l_pid);
	(void)close(fd);
	return -1;
    }
    FILE *fp;
    if ((fp = fdopen(fd, "w+")) == NULL) {
	warn("Cannot fdopen `%d'", fd);
	(void)close(fd);
	return -1;
    }
    logfp = fp;
    return logfd = fd;
}

int
tslogclose(void)
{
    int fd = logfd;
    FILE *fp = logfp;

    logfd = STDERR_FILENO;
    logfp = stderr;

    if (fp != stderr)
	(void)fclose(fp);

    if (fd != STDERR_FILENO)
	(void)close(fd);

    return 0;
}

FILE *
tslogfp(void)
{
    if (logfp == NULL)
	logfp = stderr;
    return logfp;
}


void
tslog_info(
    const tslog_info_t * info,
    const char *fmt,
    ...
)
{
    va_list ap;
    va_start(ap, fmt);
    tslog_info_vargs(info, fmt, ap);
    va_end(ap);
}


void
tslog_info_vargs(
    const tslog_info_t * info,
    const char * fmt,
    va_list ap
)
{
    const int serrno = errno;

    char buf[10240];
    size_t offset = 0;

    // In backwards compatability mode, the line# will be 0
    // and the file will contain the fully formatted prefix string.
    const char * logstr;
    if (info->line == 0)
	logstr = info->file;
    else
	logstr = getnewlogstr(
	    info->file,
	    info->line,
	    info->func,
	    info->level
	);

    const size_t hdr_len = strlen(logstr);
    memcpy(buf, logstr, hdr_len);
    offset += hdr_len;

    int msg_len = vsnprintf(
	buf + offset,
	sizeof(buf) - offset,
	fmt,
	ap
    );

    if (msg_len < 0)
    {
	static const char failure_msg[] = "tslog vsnprintf failed!";
	memcpy(buf + offset, failure_msg, sizeof(failure_msg));
	offset += sizeof(failure_msg);
    } else
    if (msg_len + offset > sizeof(buf))
	offset = sizeof(buf);
    else
	offset += msg_len;

    if (info->do_perror)
    {
	if (offset != sizeof(buf))
	    buf[offset++] = ':';

	int perror_len = snprintf(
	    buf + offset,
	    sizeof(buf) - offset,
	    " (%s)",
	    strerror(serrno)
	);
	if (perror_len < 0)
	{
	    static const char failure_msg[] = "strerr failed!";
	    memcpy(buf + offset, failure_msg, sizeof(failure_msg));
	    offset += sizeof(failure_msg);
	} else
	if (perror_len + offset > sizeof(buf))
	    offset = sizeof(buf);
	else
	    offset += perror_len;
    }

    // Add a newline if we have space
    if (offset != sizeof(buf))
	buf[offset++] = '\n';

    pthread_mutex_lock(&write_mutex);
    ssize_t wlen = write(logfd, buf, offset);
    pthread_mutex_unlock(&write_mutex);

    if (wlen != (ssize_t) offset)
	warn("Cannot append to log");

    for (unsigned i = 0 ; i < hookup_count ; i++)
    {
	tslog_hookup_entry_t * const entry = &hookup_entries[i];

	entry->handler(
	    entry->priv,
	    info,
	    buf + hdr_len, // skip the prefix
	    offset - hdr_len
	);
    }

    // Restore errno so that the caller does not see a change
    errno = serrno;
}


static const struct {
	const char *n;
	int v;
} nv[] = {
	{ "DIAG", TSDIAG },
	{ "DEBUG", TSDEBUG },
	{ "INFO", TSINFO },
	{ "WARN", TSWARN },
	{ "ERROR", TSERROR },
	{ "FATAL", TSFATAL },
	{ "COMPAT", TSCOMPAT }
};

const char *
tsloggetlevel(int level)
{
    size_t i;
    for (i = 0; i < __arraycount(nv); i++)
	if (nv[i].v == level)
	    return nv[i].n;
    return nv[i - 1].n;
}

int
tslogsetlevel(const char *l, int *level)
{
    for (size_t i = 0; i < __arraycount(nv); i++) {
	if (strcasecmp(l, nv[i].n) == 0) {
	    *level = nv[i].v;
	    return 0;
	}
    }
    TSLOGX(TSERROR, "Unknown level %s", l);
    return -1;
}

int
tslogsetabort(const char *l)
{
    for (size_t i = 0; i < __arraycount(nv); i++)
	if (strcasecmp(l, nv[i].n) == 0) {
	    tsabort = nv[i].v;
	    return 0;
	}
    TSLOGX(TSERROR, "Unknown level %s", l);
    return -1;
}

int tslog_mklogdir(const char *program, const char *root_dir,
    char *logdir, size_t logdir_size,
    char *logfile, size_t logfile_size)
{
    const char *suffix;
    if (root_dir != NULL) {
	// No suffix needed
	suffix = "";
    }
    else if ((root_dir = getenv("HOME")) == NULL) {
	warnx("No root directory specified "
	    "and unable to obtain the HOME environment "
	    "variable");
	return -1;
    }
    else {
	// Path is $HOME/ts/log
	suffix = "/ts/log";
    }

    char timestamp[TIMESTAMP_SIZE];
    size_t timestamp_result = tslog_strftime(timestamp, sizeof(timestamp),
	"%Y%m%d-%H%M%S", true);

    if (timestamp_result == 0) {
	warnx("Unable to construct the timestamp portion "
	    "of the log directory");
	return -1;
    }
    program = basename(program);
    int result = snprintf(logdir, logdir_size, "%s%s/%s-%s.log", root_dir,
	suffix, program, timestamp);
    //
    // Check that sprintf worked and then check to make sure that the
    // directory doesn't already exist
    //
    if (result < 0) {
	warn("Unable to snprintf the log directory");
	return -1;
    }
    else if (result >= (int)logdir_size) {
	warnx("Needed %d bytes for the log directory name but only "
	    "%zu were provided", result, logdir_size);
	return -1;
    }
    else if (tsdir_exists(logdir)) {
	warnx("Directory '%s' already exists", logdir);
	return -1;
    }

    //
    // Try to make the directory
    //
    if (tsdir_mkdir(logdir, 0775) != 0) {
	warnx("Unable to make directory '%s'", logdir);
	return -1;
    }

    //
    // Create the file name
    //
    result = snprintf(logfile, logfile_size, "%s/messages", logdir);
    if (result < 0) {
	warn("Unable to snprintf the log file");
	return -1;
    }
    else if (result >= (int)logfile_size) {
	warnx("Needed %d bytes for the log directory name but only "
	    "%zu were provided", result, logfile_size);
    }
    return tslogopen(logfile, 0);
}


int
tslog_set_hookup(
    tslog_hookup_t handler,
    void * priv
)
{
    tslog_hookup_entry_t * const new_entries = realloc(
	hookup_entries,
	(hookup_count + 1) * sizeof(*new_entries)
    );
    if (!new_entries)
	return -1;

    hookup_entries = new_entries;

    hookup_entries[hookup_count++] = (tslog_hookup_entry_t) {
	.handler    = handler,
	.priv	    = priv,
    };

    return 0;
}


int
tslog_unset_hookup(
    tslog_hookup_t handler,
    void * priv
)
{
    size_t i;
    for (i = 0; i < hookup_count; i++) {
	tslog_hookup_entry_t * entry = &hookup_entries[i];
	if (entry->handler == handler && entry->priv == priv)
	    break;
    }

    if (i == hookup_count)
	return -1;

    for (; i < hookup_count - 1; i++)
	hookup_entries[i] = hookup_entries[i + 1];
    hookup_count--;

    return 0;
}


/** Deprecated functions for libraries that have not updated to the new
 * version of base.
 */
void
tslog(
    const char * pref,
    const char * fmt,
    ...
) __attribute__((__format__(__printf__, 2, 3),__deprecated__));


void
tslogl(
    int level,
    const char * pref,
    const char * fmt,
    ...
) __attribute__((__format__(__printf__, 3, 4),__deprecated__));


void
tslogx(
    const char * pref,
    const char * fmt,
    ...
) __attribute__((__format__(__printf__, 2, 3),__deprecated__));


void
tsloglx(
    int level,
    const char * pref,
    const char * fmt,
    ...
) __attribute__((__format__(__printf__, 3, 4),__deprecated__));


void
tslog(
    const char * pref,
    const char * fmt,
    ...
)
{
    tslog_info_t info = {
	.file	    = pref,
	.func	    = __func__,
	.line	    = 0,
	.level	    = TSCOMPAT,
	.do_perror  = 1,
    };

    va_list ap;
    va_start(ap, fmt);
    tslog_info_vargs(&info, fmt, ap);
    va_end(ap);
}


void
tslogl(
    int level,
    const char * pref,
    const char * fmt,
    ...
)
{
    tslog_info_t info = {
	.file	    = pref,
	.func	    = __func__,
	.line	    = 0,
	.level	    = level,
	.do_perror  = 1,
    };

    va_list ap;
    va_start(ap, fmt);
    tslog_info_vargs(&info, fmt, ap);
    va_end(ap);
}


void
tslogx(
    const char * pref,
    const char * fmt,
    ...
)
{
    tslog_info_t info = {
	.file	    = pref,
	.func	    = __func__,
	.line	    = 0,
	.level	    = TSCOMPAT,
	.do_perror  = 0,
    };

    va_list ap;
    va_start(ap, fmt);
    tslog_info_vargs(&info, fmt, ap);
    va_end(ap);
}


void
tsloglx(
    int level,
    const char * pref,
    const char * fmt,
    ...
)
{
    tslog_info_t info = {
	.file	    = pref,
	.func	    = __func__,
	.line	    = 0,
	.level	    = level,
	.do_perror  = 0,
    };

    va_list ap;
    va_start(ap, fmt);
    tslog_info_vargs(&info, fmt, ap);
    va_end(ap);
}



void
tshdump_info(
    const tslog_info_t * info,
    const void * const buf,
    const size_t len
)
{
    const int serrno = errno;

    const unsigned bytes = _TSHDUMP_BYTES_PER_LINE;
    char hex[bytes*3 + 1];
    char txt[bytes + 1];
    memset(hex, 0, sizeof(hex));
    memset(txt, 0, sizeof(txt));

    const uint8_t * const bp = buf;
    size_t offset = 0;
    size_t i = 0;

    while (offset < len)
    {
	const uint8_t c = bp[offset++];
	snprintf(hex + i*3, 4, "%02x ", c);
	snprintf(txt + i, 2, "%c", isprint(c) ? c : '.');

	if ((++i % bytes) != 0 && offset != len)
	    continue;

	tslog_info(info, "%04zx: %-*s%s",
	    offset - i,
	    (int) sizeof(hex),
	    hex,
	    txt
	);

	memset(hex, 0, sizeof(hex));
	memset(txt, 0, sizeof(txt));
	i = 0;
    }

    errno = serrno;
}
