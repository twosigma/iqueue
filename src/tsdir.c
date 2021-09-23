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

#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <sys/stat.h>
#include <unistd.h>

#include "tsdir.h"

int tsdir_mkdir(const char *path, mode_t mode)
{
    const char *path_p;
    char *tmp;
    int retval = 0;
    int result;
    mode_t old_umask;

    tmp = malloc(strlen(path) + 1);

    if (tmp == NULL)
	return -1;

    /* skip first / */
    path_p = path+1;
    old_umask = umask(0);

    for (;;) {
	while (*path_p != '/') {
	    if (*path_p == 0) {
		break;
	    }
	    ++path_p;
	}

	if (*path_p == '/') {
	    strncpy(tmp, path, path_p - path);
	    tmp[path_p-path] = '\0';
	    if (tmp[path_p - path - 1] != '/') {
		result = mkdir(tmp, mode);
		if (result == -1) {
		    if (!(errno == EEXIST || errno == EACCES || errno == EROFS)) {
			/* Then this is a real error */
			TSLOG(TSERROR, "Error calling mkdir()");
			retval = -1;
			break;
		    }
		}
	    }

	    /* pass / */
	    path_p++;

	} else {
	    /* last component */
	    result = mkdir(path, mode);

	    if (result == -1) {
		if (errno == EEXIST) {
		    int result2;

		    /* If it exists, make sure it really is a directory. */
		    result2 = tsdir_exists(path);
		    if (result2 == -1) {
			TSLOGX(TSERROR, "Error calling tsdir_exists()");
			retval = -1;
		    }
		    else if (result2 != 1) {
			TSLOGX(TSERROR, "Something with this name exists, but it is not a directory.");
			retval = -1;
		    }
		}
		else {
		    TSLOG(TSERROR, "Error calling mkdir()");
		    retval = -1;
		}
	    }

	    break;
	}
    }

    free(tmp);
    umask(old_umask);

    return retval;
}

int tsdir_exists(const char *dir)
{
    int result;
    struct stat st;

    result = stat(dir, &st);
    if (result == -1 && errno == ENOENT) {
	return 0;
    }
    else if (result == -1) {
	TSLOG(TSERROR, "Error calling stat()");
	return -1;
    }

    if (!S_ISDIR(st.st_mode)) {
	return 0;
    }

    return 1;
}
