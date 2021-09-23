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
#include "iqueue.hh"
#include <iostream>
#include <chrono>
#include <thread>
#include <string>
#include <experimental/optional>

namespace iqueue = ts::mmia::cpputils;
struct options {
    char *iqueue_path;
    uint64_t shash_key;
    std::experimental::optional<uint64_t> latency_bound_ns;
};

struct options get_options(int argc, char **argv)
try {
    if (argc == 3) {
        struct options opt = {
            .iqueue_path = argv[1],
            .shash_key = std::stoul(argv[2], nullptr, 0),
            .latency_bound_ns = std::experimental::nullopt,
        };
        return opt;
    } else if (argc == 4) {
        struct options opt = {
            .iqueue_path = argv[1],
            .shash_key = std::stoul(argv[2], nullptr, 0),
            .latency_bound_ns = std::stoul(argv[3], nullptr, 0),
        };
        return opt;
    } else {
        throw std::runtime_error("Incorrect number of arguments");
    }
} catch (std::exception const& e) {
    warnx("Usage: %s <iqueue_path> <shash_key> [latency_tolerance]",
          (argc > 0 ? argv[0] : "wait_for_heartbeat"));
    warnx("%s", e.what());
    exit(1);
}

uint64_t nanos() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

int main(int argc, char **argv) {
    struct options opt = get_options(argc, argv);
    iqueue::iqueue iq(opt.iqueue_path, iqueue::access_mode::read_only);
    // spin until we can get table 0
    shash_t* table = nullptr;
    while (!table) {
        table = iqueue_writer_table(static_cast<iqueue_t*>(iq), 0, false);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    // spin until we can get the entry for this shash_key
    shash_entry_t* entry = nullptr;
    while (!entry) {
        entry = shash_get(table, opt.shash_key);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    if (opt.latency_bound_ns) {
        uint64_t latency_bound_ns = *opt.latency_bound_ns;
        // spin until the shash key value meets latency bounds
        while (entry->value < (nanos() - latency_bound_ns)) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    return 0;
}
