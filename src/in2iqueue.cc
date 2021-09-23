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
#include <gsl/gsl>
#include <unistd.h>
#include <fcntl.h>
#include <err.h>
#include "try_unix.hh"
#include "iqueue.hh"
#include <string>
#include <chrono>
#include <thread>
#include <experimental/optional>
using std::experimental::optional;
using std::experimental::nullopt;

struct options {
    char *iqueue_path;
    int infd;
    optional<uint64_t> shash_key;
};

struct options get_options(int argc, char **argv)
try {
    if ((argc != 2) && (argc != 3)) throw std::runtime_error("Incorrect number of arguments");
    int infd = 0;
    try_(fcntl(infd, F_SETFL, try_(fcntl(infd, F_GETFL, 0)) & ~O_NONBLOCK));
    struct options opt = {
        .iqueue_path = argv[1],
        .infd = infd,
        .shash_key = nullopt,
    };
    if (argc == 3) {
        opt.shash_key = std::stoul(argv[2], nullptr, 0);
    }
    return opt;
} catch (std::exception const& e) {
    warnx("Usage: %s <iqueue_path> [shash_key]",
          (argc > 0 ? argv[0] : "in2iqueue"));
    warnx("%s", e.what());
    exit(1);
}

gsl::span<gsl::byte> read_fd(int fd) {
    static gsl::byte buf[4096];
    int ret = try_(read(fd, buf, sizeof(buf)));
    if (ret == 0) exit(0);
    return { buf, static_cast<ssize_t>(ret) };
}

void fd2iqueue(int fd, ts::mmia::cpputils::iqueue& iq) {
    for (;;) {
        auto bytes = read_fd(fd);
        iq.append(bytes);
    }
}

void update_heartbeat_loop(ts::mmia::cpputils::shash_heartbeat_entry entry) {
    for (;;) {
        entry.update(std::chrono::high_resolution_clock::now());
        std::this_thread::sleep_for(std::chrono::nanoseconds(100));
    }
}

int main(int argc, char **argv) {
    struct options opt = get_options(argc, argv);
    auto iq = ts::mmia::cpputils::iqueue(opt.iqueue_path, ts::mmia::cpputils::access_mode::write);
    optional<std::thread> heartbeat_thread = nullopt;
    if (opt.shash_key) {
        auto entry = iq.create_heartbeat_entry(*opt.shash_key);
        entry.update(std::chrono::high_resolution_clock::now());
        heartbeat_thread = std::thread(update_heartbeat_loop, entry);
    }
    fd2iqueue(opt.infd, iq);
}
