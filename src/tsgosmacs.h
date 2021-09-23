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

#ifndef _TS_GOSMACS_H
#define _TS_GOSMACS_H

__BEGIN_DECLS

/*
 * Gosling's Emacs algorithm
 * Leaves STL one in the dust in speed and quality.
 */
static inline __attribute__((__always_inline__))
size_t gosmacs_hash(const char *s)
{
    size_t h = 0;
    while (*s)
	h = (h << 5) - h + (unsigned char) *s++;
    return h;
}
static inline __attribute__((__always_inline__))
size_t gosmacsn_hash(const char *s, size_t n)
{
    size_t h = 0;
    for (size_t i = 0; i < n; ++i)
	h = (h << 5) - h + s[i];
    return h;
}

__END_DECLS

#endif
