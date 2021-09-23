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
#pragma once
#include <sys/uio.h>

/**
 * Write len bytes to the fd, looping until all of them are written.
 *
 * \return -1 on error or the number of bytes actually written.
 * If the file descriptor is closed during the write the number of
 * byte will be less than len, otherwise the return should be
 * the same as len.
 */
ssize_t
write_all(
    int fd,
    const void * buf_ptr,
    size_t len
);


/** Advance an iov by adjust_len bytes.
 * \return 0 on success, -1 on failure (insufficient data)
 */
int
iovadjust(
    size_t adjust_len,
    struct iovec ** iov_ptr,
    int *entries_ptr
);


/**
 * Write total_len bytes from the io vector, looping until all of
 * them are written.
 * \note iov might be modified if there are partial writes.
 * \return -1 on error or the number of bytes actually written.
 * If the file descriptor is closed during the writev the number of
 * byte will be less than len, otherwise the return should be
 * the same as len.
 */
ssize_t
writev_all(
    int fd,
    size_t total_len,
    struct iovec * iov,
    int entries
);
