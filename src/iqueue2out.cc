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
#include <chrono>
#include <experimental/optional>
using std::experimental::optional;
using std::experimental::nullopt;
#include "stringer.hh"
using stringer::str;
#include <thread>

struct options {
    char *iqueue_path;
    int outfd;
    optional<uint64_t> shash_key;
    optional<uint64_t> latency_tolerance;
};

struct options get_options(int argc, char **argv)
try {
    if ((argc != 2) && (argc != 4)) throw std::runtime_error("Incorrect number of arguments");
    struct options opt = {
        .iqueue_path = argv[1],
        .outfd = 1,
        .shash_key = nullopt,
        .latency_tolerance = nullopt,
    };
    try_(fcntl(opt.outfd, F_SETFL, try_(fcntl(opt.outfd, F_GETFL, 0)) & ~O_NONBLOCK));
    if (argc == 4) {
        opt.shash_key = std::stoul(argv[2], nullptr, 0);
        opt.latency_tolerance = std::stoul(argv[3], nullptr, 0);
    }
    return opt;
} catch (std::exception const& e) {
    warnx("Usage: %s <iqueue_path> [shash_key] [latency_tolerance]",
          (argc > 0 ? argv[0] : "iqueue2out"));
    warnx("%s", e.what());
    exit(1);
}

void iqueue2fd(ts::mmia::cpputils::iqueue& iq, int fd) {
    auto it = iq.begin();
    for (;;) {
        iq.wait(it);
        gsl::span<gsl::byte const> msg = *it;
        if (msg.size() > 4096) {
            warnx("Message is unrealistically huge. Truncating to 4096.");
            msg = { msg.data(), 4096 };
        }
        size_t written = try_(write(fd, reinterpret_cast<char const*>(msg.data()), msg.size()));
        if (written != msg.size()) {
            warnx("Partial write? Did I get called with a non-packetized pipe?");
            exit(1);
        }
        ++it;
    }
}

void check_heartbeat(ts::mmia::cpputils::shash_heartbeat_entry& entry, std::chrono::nanoseconds latency_tolerance) {
    using namespace std::chrono;
    time_point<high_resolution_clock> const heartbeat_nanos = entry.value();
    time_point<high_resolution_clock> const now_nanos = high_resolution_clock::now();
    nanoseconds const time_since_last_heartbeat = now_nanos - heartbeat_nanos;
    if (time_since_last_heartbeat > latency_tolerance) {
        errx(1, "%s", str("This iqueue is too out-of-date! Exiting: "
                          "now (", duration_cast<nanoseconds>(now_nanos.time_since_epoch()).count(), ") ",
                          "- heartbeat (",
                          duration_cast<nanoseconds>(heartbeat_nanos.time_since_epoch()).count(), ") ",
                          "> latency_tolerance (", latency_tolerance.count(), ")").c_str());
    }
}

void check_heartbeat_loop(ts::mmia::cpputils::shash_heartbeat_entry entry, std::chrono::nanoseconds latency_tolerance) {
    for (;;) {
        using namespace std::chrono;
        time_point<high_resolution_clock> const heartbeat_nanos = entry.value();
        time_point<high_resolution_clock> const now_nanos = high_resolution_clock::now();
        nanoseconds const time_since_last_heartbeat = now_nanos - heartbeat_nanos;
        if (time_since_last_heartbeat > latency_tolerance) {
            errx(1, "%s", str("We failed our timing requirements, "
                              "now (", duration_cast<nanoseconds>(now_nanos.time_since_epoch()).count(), ") ",
                              "- heartbeat (",
                              duration_cast<nanoseconds>(heartbeat_nanos.time_since_epoch()).count(), ") ",
                              "> latency_tolerance (", latency_tolerance.count(), ")").c_str());
        }
        std::this_thread::sleep_for((latency_tolerance - time_since_last_heartbeat)/2);
    }
}

int main(int argc, char **argv) {
    struct options opt = get_options(argc, argv);
    ts::mmia::cpputils::iqueue iq(opt.iqueue_path, ts::mmia::cpputils::access_mode::read_only);
    optional<std::thread> heartbeat_thread = nullopt;
    if (opt.shash_key) {
        auto entry = iq.get_heartbeat_entry(*opt.shash_key);
        auto const latency_tolerance = std::chrono::nanoseconds(*opt.latency_tolerance);
        check_heartbeat(entry, latency_tolerance);
        heartbeat_thread = std::thread(check_heartbeat_loop, entry, latency_tolerance);
    }
    iqueue2fd(iq, opt.outfd);
}
