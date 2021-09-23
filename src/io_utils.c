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
#include "io_utils.h"
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

ssize_t
write_all(
    int fd,
    const void * const buf_ptr,
    size_t len
)
{
    const uint8_t * buf = buf_ptr;
    size_t offset = 0;

    while (1)
    {
	ssize_t wlen = write(fd, buf, len);
	if (wlen == (ssize_t) len || wlen == 0)
	    return offset + wlen;

	if (wlen < 0) return -1;

	offset += wlen;
	buf += wlen;
	len -= wlen;
    }
}

int
iovadjust(
    size_t adjust_len,
    struct iovec ** iov_ptr,
    int *entries_ptr
)
{
    int entries = *entries_ptr;
    struct iovec * iov = *iov_ptr;

    while (1)
    {
	// If the adjustment is less than the entry, we're done
	if (iov->iov_len > adjust_len)
	{
	    iov->iov_len -= adjust_len;
	    iov->iov_base = adjust_len + (uint8_t *) iov->iov_base;
	    break;
	}

	// This entry is entirely consumed; move to the next one
	adjust_len -= iov->iov_len;
	iov++;

	if (--entries > 0)
	    continue;

	return -1;
    }

    // Write the new entry count and first iov pointer
    *entries_ptr = entries;
    *iov_ptr = iov;
    return 0;
}

ssize_t
writev_all(
    int fd,
    size_t total_len,
    struct iovec * iov,
    int entries
)
{
    size_t offset = 0;

    while (1)
    {
	ssize_t wlen = writev(fd, iov, entries);
	if (wlen == (ssize_t) total_len || wlen == 0)
	    return offset + wlen;

	if (wlen < 0) return -1;

	// Adjust the iov to find where to start again
	total_len -= wlen;
	offset += wlen;

	if (iovadjust(wlen, &iov, &entries) < 0)
	    return -1;
    }
}
