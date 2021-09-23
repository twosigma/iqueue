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
#include "err.h"
#include "tslog.h"

#define CTEST_DEFINE_MAIN

#include "ctest.h"
#include "ctest_resource.h"
#include "tsclock.h"

#include <stdio.h>
#include <stdlib.h>

#include <unistd.h> 

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <fcntl.h>

/************************************************************
 * global 
 *************************************************************/

ts_test_resource_t *test_resources = NULL;
ts_test_resource_t *test_resource = NULL;

static void ts_test_resource_dir_cleanup(void *data)
{
    if (!data)
	return;
	TSLOGX(TSDEBUG, "clean up dir %s", (char *)data);
	char *buf;
	if (asprintf(&buf, "rm -rf %s", (char *)data) <= 1) {
            err(1, NULL);
        };
	int ret = system(buf);
	__USE(ret);
    free(data);
}
char * ts_test_resource_get_dir(const char *testcase_name)
{
    char *temp_dir = getenv("TMPDIR");

    // Make a template for unique directory name
    char *template;
    if (asprintf(&template, "%s/%s-%d-XXXXXX", (temp_dir == NULL) ? "/tmp" : temp_dir,
	    testcase_name, (int)getpid()) <= 0)  
	return NULL;

    // create a unique temp directory using the template
    if (mkdtemp(template) != NULL) {
	// set the temp directory permission to 755
	chmod(template, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
	// clean up temp directory when exit
	TS_ON_DESTROY(template, ts_test_resource_dir_cleanup);
	return template;
    }
    else
	return NULL;
}

static void ts_test_resource_clean_up(void)
{
    if (!global_ts_test_cleanup_on_exit) {
        return;
    }

    test_resource = test_resources;
    while (test_resource != NULL) {
        test_resource->cleanup(test_resource->data);
        ts_test_resource_t *last = test_resource;
        test_resource = test_resource->next;
        free(last);
    }
    if (global_test_output_fd) {
        fclose(global_test_output_fd);
        global_test_output_fd = NULL;
    }
    test_resources = NULL;
}

void ts_test_local_resource_clean_up(void)
{
    test_resource = test_resources;
    while (test_resource != NULL) {
        test_resource->cleanup(test_resource->data);
        ts_test_resource_t *last = test_resource;
        test_resource = test_resource->next;
        free(last);
    }
    test_resources = NULL;
}

static void
signal_handler(
    int signum,
    siginfo_t * const siginfo,
    void * const context
)
{
    __USE(signum);
    __USE(siginfo);
    __USE(context);

    ts_test_resource_clean_up();

    abort();
}

void signal_handler_install(void) {
    static int installed;
    if(installed)
        return;

    static const struct sigaction sigact = {
        .sa_sigaction	= signal_handler,
        .sa_flags	= SA_SIGINFO,
    };

    /*
     * JVM uses signal for normal process and the old way (always install the handler
     * is incompatible with it. So we only install the handler when there is no previous
     * handler installed
     */

    int signals[] = { SIGSEGV, SIGBUS, SIGILL, SIGPIPE, SIGABRT, SIGTERM };
    struct sigaction prev;

    for (size_t i = 0; i < sizeof(signals)/sizeof(signals[0]); ++i) {
        memset(&prev, 0, sizeof(prev));

        int signo = signals[i];

        sigaction(signo, &sigact, &prev);

        if (prev.sa_handler != SIG_DFL && prev.sa_handler != SIG_IGN) {
            sigaction(signo, &prev, NULL);
        }
    }
    atexit(ts_test_resource_clean_up);
    installed = 1;
}
