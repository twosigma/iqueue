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
#ifndef TSLOG_H
#define TSLOG_H

#include <stdint.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdbool.h>

__BEGIN_DECLS

typedef struct {
    const char * file;
    int line;
    const char * func;
    int level;
    int do_perror;
} tslog_info_t;

/**
 * Creates a log directory located below the specified root directory
 * that has the pattern "program-timestamp.log" and creates a log file
 * names "messages" in that directory which will be used for tslog
 * messages. If the root_dir is NULL, then the HOME environment
 * variable will be used and "/ts/log" will be appended to it. The
 * timestamp portion of it will be in the format YYYYMMDD-HHMMSS.
 *
 * @param program the name of the program that is calling this
 * function. This should be the value of argv[0] and cannot be NULL
 * @param root_dir the directory under which the log directory should
 * be created. If NULL, $HOME/ts/log will be used
 * @param logdir the resulting log directory.
 * @param logdir_size the length of the logdir argument
 * @param logfile the resulting messages file.
 * @param logfile_size the length of the logfile_size argument
 * @return the file descriptor of the created logfile, or -1 for
 * failure
 */
int
tslog_mklogdir(const char *program, const char *root_dir,
    char *logdir, size_t logdir_size,
    char *logfile, size_t logfile_size);


typedef void (*tslog_hookup_t)(
    void *cookie,
    const tslog_info_t * info,
    const char *message,
    size_t message_len
);

/**
 * Set up a hook up function on log events
 * @param hookup the hook up function
 * @param cookie cookie passed to the function call
 */
int
tslog_set_hookup(
    tslog_hookup_t hookup,
    void * cookie
);

/**
 * Uninstalls a hook up function
 *
 * @return 0 on success, non-zero otherwise
 */
int
tslog_unset_hookup(
    tslog_hookup_t handler,
    void * priv
);

const char *getlogstr(const char *, int, const char *, int);
const char *getnewlogstr(const char *, int, const char *, int);
int setlogstr(const char *);

/**
 * Logging to a file
 *
 * param #1: filename
 * param #2: open flags
 */
int tslogopen(const char *, int);
int tslogclose(void);
const char *tsloggetlevel(int);
int tslogsetlevel(const char *, int *);
int tslogsetabort(const char *);
FILE *tslogfp(void);

/**
 * Logging to a file [the x version does not append (%s), strerror(errno)
 *
 * param #1: prefix [NULL for none]
 * param #2: format
 * param #3: ...
 */
void
tslog_info(
    const tslog_info_t *,
    const char *fmt,
    ...
) __attribute__((__format__(__printf__, 2, 3)));

void
tslog_info_vargs(
    const tslog_info_t *,
    const char *fmt,
    va_list args
);


/**
 * Hexdump to the logfile.
 */
void
tshdump_info(
    const tslog_info_t *,
    const void * buf,
    size_t len
);

extern int tsdebug;
extern int tsabort;
extern int tslevel;

#define PREF	getlogstr(__FILE__, __LINE__, __func__, TSCOMPAT)

#define	TSDIAG		 5
#define	TSDEBUG		 4
#define	TSINFO		 3
#define	TSWARN		 2
#define	TSERROR		 1
#define	TSFATAL		 0
#define	TSCOMPAT	-1

#ifndef TSLOG_DIAG_ENABLED
# if defined(DIAGNOSTIC)
#  define TSLOG_DIAG_ENABLED true
# else
#  define TSLOG_DIAG_ENABLED false
# endif
#endif

#define TS_IS_LOGGING(level) (((level) != TSDIAG || TSLOG_DIAG_ENABLED) && level <= tslevel)

#define _TSLOG(level, do_perror, fmt, ...) \
    do { \
	if ((level) != TSDIAG || TSLOG_DIAG_ENABLED) { \
	    if ((level) > tslevel && (level) != tsabort) \
		break; \
	    tslog_info_t __tslog_info = { \
		__FILE__, \
		__LINE__, \
		__func__, /* NOLINT */ \
		(level), \
		(do_perror) \
	    }; \
	    tslog_info(&__tslog_info, fmt, ## __VA_ARGS__); /* NOLINT */ \
	    if ((level) <= tsabort) \
		abort(); \
	} \
    } while (/*CONSTCOND*/false)

#define TSLOG(level, fmt, ...) _TSLOG(level, 1, fmt, ## __VA_ARGS__)
#define TSLOGX(level, fmt, ...) _TSLOG(level, 0, fmt, ## __VA_ARGS__)

/* Force an abort */
#define TSABORT(fmt, ...) \
    do { \
	TSLOG(TSFATAL, fmt, ## __VA_ARGS__); \
	abort(); \
    } while (false)

#define TSABORTX(fmt, ...) \
    do { \
	TSLOGX(TSFATAL, fmt, ## __VA_ARGS__); \
	abort(); \
    } while (false)


/* hexdump to log */
#define _TSHDUMP(level, do_perror, buffer, size) \
    do { \
	if ((level) == TSDIAG && !TSLOG_DIAG_ENABLED) \
	    break; \
	if ((level) > tslevel \
	&& (level) != tsabort) \
	    break; \
	tslog_info_t __tslog_info = { \
	    __FILE__, \
	    __LINE__, \
	    __func__, /* NOLINT */ \
	    (level), \
	    (0) /* no perror */ \
	}; \
	tshdump_info(&__tslog_info, (buffer), (size)); /* NOLINT */ \
	if ((level) <= tsabort) \
	    abort(); \
    } while (/*CONSTCOND*/false)

#define TSHDUMP(level, buf, size) _TSHDUMP(level, 1, buf, size)

/*
 * %%:	%
 * %D:	Datetime+Zone
 * %F:	File
 * %L:	Line
 * %N:	Program name
 * %P:	File with Path
 * %f:	Function
 * %T:  Type
 * %t:  Thread id
 */
#define PREF_LONG "%T %N::%f@%P,%L: %D: "
#define PREF_SHORT "%T %D %F,%L: "

// this is a new, more readable format for logging. it will be used by the TSLOG
// and TSLOGX functions. PREF_LONG and PREF_SHORT are being left as-is because
// we have existing code that parses logs and expects this format. New code
// should use the new functions and the new format.
#define DEFAULT_TSLOG_FORMAT "%D %T [%t] %N::%f (%F:%L): "


/* Counter limited log macros */
#define _DEFAULT_LOG_LIMIT 10000
// Duplicate the level checks because that potentially saves us from
// loading the 2 static uint32's, which can results in a huge
// performance penalty especially if cross-bus.
#define _TSLOGN(log_macro, limit, level, ...)                           \
    do {                                                                \
        if ((level) != TSDIAG || TSLOG_DIAG_ENABLED) {                  \
            if ((level) > tslevel && (level) != tsabort)                \
                break;                                                  \
            static uint32_t _count = 0;                                 \
            static uint32_t _threshold = (limit);                       \
            if (++_count < (limit))                                     \
                log_macro((level), __VA_ARGS__);                        \
            else if (_count == _threshold) {                            \
                log_macro((level), __VA_ARGS__);                        \
                TSLOGX((level), "Previous message has been logged %" PRIu32 \
                    " times and is being suppressed", _count);          \
                _threshold *= 2;                                        \
            }                                                           \
        }                                                               \
    } while (/*CONSTCOND*/false)

#define TSLOGN(limit, ...) _TSLOGN(TSLOG, limit, __VA_ARGS__)
#define TSLOGXN(limit, ...) _TSLOGN(TSLOGX, limit, __VA_ARGS__)
#define TSLOGL(...) TSLOGN(_DEFAULT_LOG_LIMIT, __VA_ARGS__)
#define TSLOGXL(...) TSLOGXN(_DEFAULT_LOG_LIMIT, __VA_ARGS__)

#define _TSHDUMP_BYTES_PER_LINE 16
#define _TSHDUMP_LINE_LIMIT 100
#define _TSHDUMP_SIZE_LIMIT (_TSHDUMP_LINE_LIMIT*_TSHDUMP_BYTES_PER_LINE)
#define TSHDUMPN(limit, level, buffer, size)                            \
    do {                                                                \
        uint32_t _size = (size);                                        \
        if (_size <= _TSHDUMP_SIZE_LIMIT) {                             \
            _TSLOGN(TSHDUMP, (limit), (level), (buffer), _size);        \
        } else {                                                        \
            _TSLOGN(TSHDUMP, (limit), (level), (buffer), _TSHDUMP_SIZE_LIMIT); \
            TSLOGXN((limit), (level), "Previous hex dump had a size of %" PRIu32  \
                    " bytes and is being truncated", _size);            \
        }                                                               \
    } while (/*CONSTCOND*/false)
#define TSHDUMPL(...) TSHDUMPN(_DEFAULT_LOG_LIMIT/_TSHDUMP_LINE_LIMIT, __VA_ARGS__)

__END_DECLS

#endif
