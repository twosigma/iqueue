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

#include <err.h>
#include <system_error>
#include <initializer_list>

//// Utility functions
// a poor imitation of Rust try!()
// aborts if retval < 0, otherwise returns retval
namespace ts { namespace mmia { namespace cpputils { namespace try_unix {
inline int try_func(int retval, const char *func, int line) {
    if (retval < 0) {
        // err(1, "%s: %d", func, line);
        warn("%s: %d", func, line);
        throw std::system_error(std::error_code(errno, std::system_category()));
    }
    return retval;
}

// like try_func, but doesn't abort if errno is in exclude_errors
inline int try_exclude_func(int retval, std::initializer_list<int> exclude_errors, const char *func, int line) {
    if (retval < 0) {
        for (int err : exclude_errors) {
            if (errno == err) return retval;
        }
        // err(1, "%s: %d", func, line);
        warn("%s: %d", func, line);
        throw std::system_error(std::error_code(errno, std::system_category()));
    }
    return retval;
}
}}}}

#define try_(x) ts::mmia::cpputils::try_unix::try_func(x, __FUNCTION__, __LINE__)
#define try_exclude(x, ...) ts::mmia::cpputils::try_unix::try_exclude_func(x, __VA_ARGS__, __FUNCTION__, __LINE__)
