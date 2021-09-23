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
#ifndef _CTEST_H
#define _CTEST_H

#ifdef __cplusplus
extern "C++" {
#  include <cstdarg>
#  include <cstdio>
#  include <cstdlib>
}
#else
#  include <stdarg.h>
#  include <stdio.h>
#  include <stdlib.h>
#  include <stdbool.h>
#endif

__BEGIN_DECLS

typedef bool (*ts_run_test_func_t)(void);

typedef struct ts_test_case {
    const char *name;
    ts_run_test_func_t run;
    bool enabled;
    struct ts_test_case *next;
} ts_test_case_t;

// we only define this global pointer in a single translation unit, all others
// pull it in with an extern declaration.
#ifndef CTEST_DEFINE_MAIN
extern
#endif
ts_test_case_t *ts_test_global_test_suite;

static inline void
ts_add_test_case(
    const char *test_name,
    ts_run_test_func_t run_test_func,
    bool is_enabled)
{
#ifdef __cplusplus
    ts_test_case_t *test_case = reinterpret_cast<ts_test_case_t*>(malloc(sizeof(*test_case)));
#else
    ts_test_case_t *test_case = malloc(sizeof(*test_case));
#endif
    *test_case = (ts_test_case_t) {
	test_name,
	run_test_func,
	is_enabled,
	ts_test_global_test_suite
    };

    ts_test_global_test_suite = test_case;
}

#define TS_TEST_ASSERT_FORMAT(expr, format, ...) \
    do { \
        if (expr) {} else { \
	    fprintf(global_std_out_fd, "\tassert(\"%s\") from %s:%d\n", #expr, __FILE__, __LINE__);  /* NOLINT */ \
	    fprintf(global_std_out_fd, format, ##__VA_ARGS__); /* NOLINT */ \
	    fprintf(global_std_out_fd, "\n"); /* NOLINT */ \
            \
	    fprintf(global_test_output_fd, "\t\t<failure type=\"assert\" src=\"%s:%d\">\"%s\": \n\t\t", __FILE__, __LINE__, #expr); /* NOLINT */ \
	    fprintf(global_test_output_fd, format, ##__VA_ARGS__); /* NOLINT */ \
	    fprintf(global_test_output_fd, "\n\t\t</failure>\n"); /* NOLINT */ \
            \
            return false; \
        } \
    } while (false)

#define TS_TEST_EQUALS_FORMAT(a, b, format, ...) \
    do { \
	bool _result = ((a) == (b)); \
        if (!_result) { \
	    fprintf(global_std_out_fd, "\tequals(\"%s\", \"%s\") from %s:%d\n\t", #a, #b, __FILE__, __LINE__); /* NOLINT */ \
	    fprintf(global_std_out_fd, format, ##__VA_ARGS__); /* NOLINT */ \
	    fprintf(global_std_out_fd, "\n"); /* NOLINT */ \
	    \
	    fprintf(global_test_output_fd, "\t\t<failure type=\"equals\" src=\"%s:%d\"/>\"%s\"!=\"%s\"\n\t\t", __FILE__, __LINE__, #a, #b);  /* NOLINT */ \
	    fprintf(global_test_output_fd, format, ##__VA_ARGS__); /* NOLINT */ \
	    fprintf(global_test_output_fd, "\n\t\t</failure>\n"); /* NOLINT */ \
            \
            return false; \
        } \
    } while (false)

// using 'expr' without protective parentheses in the condition here is
// deliberate. This is to enable the compiler to warn about accidental
// assignments instead of comparisons in the test
#define TS_TEST_ASSERT(expr)  \
    do {  \
        if (expr) {} else { \
	    fprintf(global_std_out_fd, "\tassert(\"%s\") from %s:%d\n", #expr, __FILE__, __LINE__);  /* NOLINT */ \
	    \
	    fprintf(global_test_output_fd, "\t\t<failure type=\"assert\" src=\"%s:%d\">\"%s\"</failure>\n", __FILE__, __LINE__, #expr); /* NOLINT */ \
            return false; \
        } \
    } while (false)

#define TS_TEST_EQUALS(a, b) \
    do { \
	bool _result = ((a) == (b)); \
        if (!_result) { \
	    fprintf(global_std_out_fd, "\tequals(\"%s\", \"%s\") from %s:%d\n\t", #a, #b, __FILE__, __LINE__); /* NOLINT */ \
            \
	    fprintf(global_test_output_fd, "\t\t<failure type=\"equals\" src=\"%s:%d\">\"%s\"!=\"%s\"</failure>\n", __FILE__, __LINE__, #a, #b);  /* NOLINT */ \
            return false; \
        } \
    } while (false)

#define TS_ADD_TEST(_test_name) \
    static inline bool run_##_test_name(void);  \
    __attribute__((constructor)) \
    static void add_test_##_test_name(void) {  \
        ts_add_test_case(#_test_name, run_##_test_name, true); \
    } \
    static inline bool run_##_test_name()

#define TS_ADD_DISABLED_TEST(_test_name) \
    static inline bool run_##_test_name(void);  \
    __attribute__((constructor)) \
    static inline void disable_test_##_test_name(void) { \
        ts_add_test_case(#_test_name, run_##_test_name, false); \
    } \
    static inline bool run_##_test_name()

#ifdef CTEST_DEFINE_MAIN

#include "ctest_main.h"

#else

extern FILE *global_test_output_fd;
extern FILE *global_std_out_fd;
extern bool global_ts_test_cleanup_on_exit;

#endif

__END_DECLS

#endif
