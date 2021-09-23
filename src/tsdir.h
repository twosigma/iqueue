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

#ifndef _TSDIR_H_
#define _TSDIR_H_

#include <sys/types.h>

/* Like the mkdir() system call, but make all parent directories
 * if they don't exist. Also, don't fail if the directory already
 * exists. This is like the "mkdir -p" shell command.
 *
 * @path: the absolute or relative path of the directory to create
 * @mode: the mode of the created directories, identical to the
 *       mode argument of the mkdir() system call
 *
 * Return value:
 *    -1: Error (other than the directory already existed)
 *     0: Success
 */

int tsdir_mkdir(const char *path, mode_t mode);


/* Check if a directory exists
 *
 * @dir: the directory to test
 *
 * Return value:
 *    -1: Error
 *     0: Directory does not exist
 *     1: Directory exists
 */

int tsdir_exists(const char *dir);

#endif /* TSDIR_H */
