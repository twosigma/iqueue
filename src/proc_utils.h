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
/* The need for this header can be removed by removing SSH connection
 * setup support from iqsync-main, and replacing it with a Python
 * script to do the setup. */
#include <unistd.h>

#define TSIO_STDIN_MASK	1
#define TSIO_STDOUT_MASK 2
#define TSIO_STDERR_MASK 4

/**
 * Spawn a process with stdin/sdout/stderr captured by the
 * calling process.
 *
 * \return pid on success, -1 on failure.
 * \param redirect_mask is a bitmask of which fds to redirect (0, 1 and 2)
 * \param fd_out[] will be the child's stdin, stdout and stderr.
 */
pid_t
tsio_open3(
    int fd_out[3],
    unsigned redirect_mask,
    const char * file,
    const char * const argv[]
);
