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
#include "proc_utils.h"
#include "stdint.h"
#include "tslog.h"

pid_t
tsio_open3(
    int fd[],
    unsigned redirect_mask,
    const char * file,
    const char * const argv[]
)
{
    int fd_to_child[2];
    int fd_stdout_from_child[2];
    int fd_stderr_from_child[2];

    if ((redirect_mask & TSIO_STDIN_MASK) && pipe(fd_to_child) < 0) {
	TSLOG(TSERROR, "Unable to create pipe");
        return -1;
    }
    if ((redirect_mask & TSIO_STDOUT_MASK) && pipe(fd_stdout_from_child) < 0) {
	TSLOG(TSERROR, "Unable to create pipe");
        return -1;
    }
    if ((redirect_mask & TSIO_STDERR_MASK) && pipe(fd_stderr_from_child) < 0) {
	TSLOG(TSERROR, "Unable to create pipe");
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) {
	TSLOG(TSERROR, "Unable to fork child!");
        return -1;
    }

    if (pid == 0)
    {
	// Child; copy the pipe descriptors to stdin/stdout
	if (redirect_mask & TSIO_STDIN_MASK)
	{
	    if (dup2(fd_to_child[0], STDIN_FILENO) < 0)
		TSABORT("Unable to dup stdin fd");
	    close(fd_to_child[0]);
	    close(fd_to_child[1]);
	}

	if (redirect_mask & TSIO_STDOUT_MASK)
	{
	    if (dup2(fd_stdout_from_child[1], STDOUT_FILENO) < 0)
		TSABORT("Unable to dup stdout fd");
	    close(fd_stdout_from_child[0]);
	    close(fd_stdout_from_child[1]);
	}

	if (redirect_mask & TSIO_STDERR_MASK)
	{
	    if (dup2(fd_stderr_from_child[1], STDERR_FILENO) < 0)
		TSABORT("Unable to dup stderr fd");
	    close(fd_stderr_from_child[0]);
	    close(fd_stderr_from_child[1]);
	}

	execvp(file, (void*)(uintptr_t) argv);
	TSABORT("Unable to exec %s", file);
    }

    // Parent process continues here; clean up the dangling fds
    if (redirect_mask & TSIO_STDIN_MASK)
    {
	fd[0] = fd_to_child[1];
	close(fd_to_child[0]); // read end
    } else
	fd[0] = -1;

    if (redirect_mask & TSIO_STDOUT_MASK)
    {
	fd[1] = fd_stdout_from_child[0];
	close(fd_stdout_from_child[1]); // write end
    } else
	fd[1] = -1;

    if (redirect_mask & TSIO_STDERR_MASK)
    {
	fd[2] = fd_stderr_from_child[0];
	close(fd_stderr_from_child[1]); // write end
    } else
	fd[2] = -1;

    return pid;
}
