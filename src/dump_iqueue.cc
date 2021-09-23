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

struct options {
    char *iqueue_path;
    int outfd;
};

struct options get_options(int argc, char **argv)
try {
    if (argc != 2) throw std::runtime_error("Incorrect number of arguments");
    int outfd = 1;
    return {
        .iqueue_path = argv[1],
        .outfd = outfd,
    };
} catch (std::exception const& e) {
    warnx("Usage: %s <iqueue_path>",
          (argc > 0 ? argv[0] : "dump_iqueue"));
    warnx("%s", e.what());
    exit(1);
}

int main(int argc, char **argv) {
    struct options opt = get_options(argc, argv);
    ts::mmia::cpputils::iqueue iq(opt.iqueue_path, ts::mmia::cpputils::access_mode::read_only);
    auto it = iq.begin();
    for (;;) {
        if (!iq.wait(it, std::chrono::nanoseconds(0))) {
            exit(0);
        }
        gsl::span<gsl::byte const> msg = *it;
        if (msg.size() > 4096) {
            warnx("Message is unrealistically huge. Truncating to 4096.");
            msg = { msg.data(), 4096 };
        }
        size_t written = try_(write(opt.outfd, reinterpret_cast<char const*>(msg.data()), msg.size()));
        if (written != msg.size()) {
            warnx("Partial write? Did I get called with a non-packetized pipe?");
            exit(1);
        }
        ++it;
    }
}
