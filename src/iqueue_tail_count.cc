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
#include <stdint.h>
#include <err.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <unistd.h>
#include "try_unix.hh"
#include "iqueue.hh"

struct options {
    char *iqueue_path;
};

struct options get_options(int argc, char **argv)
try {
    if (argc != 2) throw std::runtime_error("Incorrect number of arguments");
    struct options opt = {
        .iqueue_path = argv[1],
    };
    return opt;
} catch (std::exception const& e) {
    warnx("Usage: %s <iqueue_path>",
          (argc > 0 ? argv[0] : "iqueue_tail_count"));
    warnx("%s", e.what());
    exit(1);
}

uint64_t nanos() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

int main(int argc, char **argv) {
    struct options opt = get_options(argc, argv);
    ts::mmia::cpputils::iqueue iq(opt.iqueue_path, ts::mmia::cpputils::access_mode::read_only);
    uint64_t id = 0;
    for (;;) {
        uint64_t const end = iqueue_end((iqueue_t*)iq);
        if (id != end) {
            uint64_t const delta = end - id;
            try_(write(1, &delta, sizeof(delta)));
            id = end;
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    }
}
