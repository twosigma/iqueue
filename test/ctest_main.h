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
#ifndef _CTEST_MAIN_H
#define _CTEST_MAIN_H

#ifdef __cplusplus
extern "C++" {
#  include <cstdio>
#  include <cstring>
}
#else
#  include <stdio.h>
#  include <string.h>
#endif
                                                                               
#include <getopt.h> 

/************************************************************
 * global 
 ************************************************************/
FILE *global_test_output_fd = NULL;
FILE *global_std_out_fd = NULL;
bool global_ts_test_cleanup_on_exit = true;

/************************************************************
 * options
 ************************************************************/
const char *option_string = "r:o:lqkd:h";

#define DEFAULT_USAGE "DEFAULT_USAGE" // icc complains if we use simply usage(NULL)

static struct option long_options[] = { 
    { "run",        required_argument,  NULL, 'r' }, 
    { "output",     required_argument,  NULL, 'o' },    
    { "list",       no_argument,        NULL, 'l' }, 
    { "quiet",      no_argument,        NULL, 'q' }, 
    { "keep-log",   no_argument,        NULL, 'k' },
    { "help",       no_argument,        NULL, 'h' }, 
    { 0, 0, 0, 0}, 
}; 

static void 
__attribute__((noreturn)) 
__attribute__((__format__(__printf__, 1, 2))) 
usage(const char * error_fmt, ...) 
{ 
    static const char usage_str[] = 
        "usage: <test_bin> [<options>]\n" 
        "\n" 
        "  --run        | -r <test name>           Run test\n"
        "  --output     | -o <xml file name>       Output xml file name\n"
        "  --list       | -l                       List all tests\n"
        "  --quiet      | -q                       Disable print\n"
        "  --keep-log   | -k                       Keep log when test finishes\n"
        "  --help       | -h                       help\n";
    if (strcmp(error_fmt, DEFAULT_USAGE) != 0) { 
        va_list ap; 
        va_start(ap, error_fmt); 
        vfprintf(stderr, error_fmt, ap); 
        va_end(ap); 
        fprintf(stderr, "\n%s", usage_str); 

        exit(EXIT_FAILURE); 
    } 
    else { 
        fprintf(stdout, "%s", usage_str); 
        exit(EXIT_SUCCESS); 
    } 
}

/************************************************************
 * sub functions
 ************************************************************/

static bool ts_test_harness_run(const char *name)
{   
    if (ts_test_global_test_suite == NULL) {
        fprintf(global_std_out_fd, "no test case to run\n");
        return 0;
    }

    bool suite_succeeded = true;
    int fail_count = 0;
    int skip_count = 0;
    int success_count = 0;

    fprintf(global_test_output_fd, "<testsuite>\n");

    for (ts_test_case_t *test_case = ts_test_global_test_suite;
        test_case != NULL; test_case = test_case->next)
    {
        if (name && strcmp(test_case->name, name) != 0) {            
            continue;
        }        

        // Only skip the test if it is not asked for specificaly
        if (!name && !test_case->enabled) {
            fprintf(global_test_output_fd,
                "\t<testcase name=\"%s\" enabled=\"false\"/>\n",
                test_case->name);

            fprintf(global_std_out_fd, "SKIPPED Test :%s\n", test_case->name);

            skip_count++;
            continue;
        }

        fprintf(global_test_output_fd,
            "\t<testcase name=\"%s\" enabled=\"true\">\n",
            test_case->name);
        fprintf(global_std_out_fd, "Starting test: '%s'\n", test_case->name);

        bool ret = test_case->run();

        fprintf(global_std_out_fd,
            "Finished test: '%s', Result: %s\n",
            test_case->name, ret ? "SUCCEED":"FAILED");

        fprintf(global_test_output_fd, "\t</testcase>\n");

        if (!ret) {
            suite_succeeded = false;
            fail_count++;
        } else {
            success_count++;
        }

        if (name) {
            break;
        }
    }

    fprintf(global_std_out_fd,
        "Total %d tests: %d succeed, %d failed, %d skipped.\n",
        success_count + skip_count + fail_count,
        success_count,
        fail_count,
        skip_count);

    fprintf(global_test_output_fd, "</testsuite>\n");

    if (name && success_count == 0 && fail_count == 0) {
        fprintf(global_std_out_fd, "The test case %s was not found\n", name);
    }

    return suite_succeeded;
}

static void ts_test_harness_list_all_xml(void) 
{
    for (ts_test_case_t *test_case = ts_test_global_test_suite;
        test_case != NULL; test_case = test_case->next)
    {
        fprintf(global_test_output_fd,
            "\t<testcase name=\"%s\" enabled=\"%s\"/>\n",
            test_case->name, test_case->enabled ? "true":"false");
    }
}

static void ts_test_harness_list_all(void) 
{
    for (ts_test_case_t *test_case = ts_test_global_test_suite;
        test_case != NULL; test_case = test_case->next)
    {
        fprintf(global_std_out_fd,
            "%s,%s\n", test_case->name,
            test_case->enabled ? "enabled" : "disabled");
    }
}

// Utility functions for a /dev/null FILE stream
static ssize_t dev_null_read(void *cookie, char *buf, size_t size) {
    // It's an error to read from this stream
    (void)cookie;
    (void)buf;
    (void)size;
    return -1;
}

static ssize_t dev_null_write(void *cookie, const char *buf, size_t size) {
    // Write always succeeds!
    (void)cookie;
    (void)buf;
    return size;
}

static int dev_null_seek(void *cookie, off64_t *offset, int whence) {
    // Cannot seek
    (void)cookie;
    (void)offset;
    (void)whence;
    return -1;
}

static int dev_null_close(void *cookie) {
    // Close does nothing
    (void)cookie;
    return 0;
}

static FILE *open_dev_null(void) {
    return fopencookie(NULL, "w",
        (cookie_io_functions_t) {
        dev_null_read,
        dev_null_write,
        dev_null_seek,
        dev_null_close
        });
}

/************************************************************
 * main function
 ************************************************************/
int main(int argc, char **argv) 
{
    int option_index = 0; 
    char *output_file_name = NULL;
    bool is_quiet = false;
    char *run_test_name = NULL;
    bool list_only = false;
    bool keep_log = false;
    while (1)
    { 
        int c = getopt_long( 
            argc, 
            argv, 
            option_string,
            long_options,
            &option_index 
            ); 
        if (c == -1)  
            break;
        switch (c) { 
        case 'r':
            run_test_name = optarg;
            break;
        case 'o':
            output_file_name = optarg;
            break;
        case 'l':
            list_only = true;
            break;
        case 'q':
            is_quiet = true;
            break;
        case 'k':
            keep_log = true;
            break;
        case 'h': 
            usage(DEFAULT_USAGE); 
        default: 
            usage("Invalid option '%c'", c); 
        }
    } 
    if (output_file_name) {
        global_test_output_fd = fopen(output_file_name, "w"); 
        if (!global_test_output_fd) {
            fprintf(stderr, "Unable to open output file %s\n", output_file_name); 
            exit(EXIT_FAILURE); 
        }
    } else {
        global_test_output_fd = open_dev_null();
    }

    if (!is_quiet) {
        global_std_out_fd = stdout;
    } else {
        global_std_out_fd = open_dev_null();
    }

    if (keep_log) {
        global_ts_test_cleanup_on_exit = false;
        fprintf(global_test_output_fd, "Keep test log for debugging!");
    } else {
        global_ts_test_cleanup_on_exit = true;
        fprintf(global_test_output_fd, "Will cleanup test data");
    }

    if (list_only) {
        ts_test_harness_list_all_xml();
        ts_test_harness_list_all();
        return 0;
    }

    if (!ts_test_harness_run(run_test_name)) {
        return -1;
    }

    return 0;
}

#endif // _CTEST_MAIN_H
