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
#include "twosigma.h"
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <getopt.h>
#include <ctype.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include "tslog.h"
#include "proc_utils.h"
#include "tsclock.h"
#include "net_utils.h"
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "math_utils.h"
#include "iqueue.h"
#include "iqsync.h"
#include "tslock.h"


/** \file
 * Push or pull changes from an iqueue with another over stdin/stdout
 * tunneled through ssh, or over a TCP socket.
 */
static struct option long_options[] = {
    { "help",           no_argument,        0, '?' },
    { "iqueue",         required_argument,  0, 'f' },
    { "tail",           no_argument,        0, 't' },
    { "server",         required_argument,  0, 'R' },
    { "sleep",          required_argument,  0, 's' },
    { "rate-limit",     required_argument,  0, 'M' },
    { "report-interval", required_argument, 0, 'r' },
    { "type",           required_argument,  0, 'T' },
    { "push",           no_argument,        0, 'p' },
    { "pull",           no_argument,        0, 'P' },
    { "nop",            no_argument,        0, 'Z' },
    { "validate",       no_argument,        0, 'V' },
    { "verbose",        no_argument,        0, 'v' },
    { "quiet",          no_argument,        0, 'q' },
    { "clone",          no_argument,        0, 'C' },
    { "remote-cpu",     required_argument,  0, 'K' },
    { "cpu",            required_argument,  0, 'c' },
    { "prefetch",       no_argument,        0, 'e' },
    { "syncbehind",     no_argument,        0, 'b' },
    { "filter", 	required_argument,  0, 'F' },
    { "clone-push",     no_argument,        0, 'O' },
    { "launch-server",  no_argument,        0, 1   },
    { "connection-timeout",
                        required_argument,  0, 2   },
    { "launched-by-client",
                        no_argument,        0, 3   },
    { "send-buffer",    no_argument,        0, 4   },
    { "send-buffer-size",
                        required_argument,  0, 5   },
    { "recv-buffer",    no_argument,        0, 6   },
    { "recv-buffer-size",
                        required_argument,  0, 7   },
    { 0, 0, 0, 0},
};

static void
__attribute__((noreturn))
usage(
    FILE * stream,
    const char * msg
)
{
    static const char usage_str[] =
"Options:\n"
"   -h | -? | --help            This help\n"
"   -f | --iqueue /path/file    Local iqueue (required)\n"
"   -V | --validate             Validate that the iqueue headers match\n"
"   -M | --rate-limit N         Read no more than N MB/s\n"
"   -p | --push                 Push local changes\n"
"   -P | --pull                 Pull remote changes\n"
"   -s | --sleep N              Sleep N useconds when there is no data\n"
"   -r | --report-interval N    Report TX/RX stats every N seconds\n"
"   -t | --tail                 Keep tracking entries as they are added\n"
"   -v | --verbose              Report every TX/RX message\n"
"   -q | --quiet                Sets the log level to TSWARN\n"
"   -T | --type {ssh|tcp}       Transport type (default ssh)\n"
"   -c | --cpu N                Bind local push thread to CPU N\n"
"   -K | --remote-cpu N         Bind remote push thread to CPU N"
"(only with ssh)\n"
"   -e | --prefetch             Create prefetch thread\n"
"   -b | --syncbehind           Create syncbehind thread\n"
"   -F | --filter               Use filter in format <lib>:<func>\n"
"\n"
"   --send-buffer               Messages will be aggregated into buffer\n"
"                               until the buffer gets full or no more messages available\n"
"   --send-buffer-size          Use send buffer of N bytes instead of default size(4KB)\n"
"   --recv-buffer               Reduce amount of IO on receiver side by using a buffer\n"
"   --recv-buffer-size          Use recv buffer of N bytes instead of default size(1MB)\n"
"\n"
"Cloning options:\n"
"   Cloning implies --pull and --validate.  If the local sizes are not\n"
"   specified the remote sizes will be used.  Bi-directional cloning\n"
"   is supported. clone-push implies --push and --validate.\n"
"\n"
"   -C | --clone                Clone a remote iqueue\n"
"   -O | --clone-push           Clone local iqueue to remote machine\n"
"\n"
"Options for TCP:\n"
"   By default, client and server have to be launched separately. \n"
"   'iqsync --launch-server' will launch server from the client.\n"
"\n"
"   --launch-server             Start tcp server from client\n"
"   --connection-timeout N      Wait for server to start for N seconds\n"
"\n"
"\n"
"Push/pull:\n"
"   The direction of the operation is from the view of the local process:\n"
"\n"
"   'iqsync --pull' or 'iqsync --clone' will open the remote queue read-only,\n"
"   read entries from it and write them to the local queue.\n"
"\n"
"   'iqsync --push' will open the local queue read-only, read entries from\n"
"   it and write to the remote queue.  It will not create a set of remote\n"
"   files; they must already exist or iqsync will exit with an error.\n"
"\n"
"   'iqsync --push --pull' will open both files read/write and merge\n"
"   their entries.  Keep in mind that the order of the entries that already\n"
"   exist in each queue will never change, so the queues will not be exactly\n"
"   identical, but all entries in the remote queue will appear in the same\n"
"   order relative to each other.\n"
"\n"
"\n"
"SSH Usage:\n"
"   iqsync [options...] [user@]host:/path/to/remote.iqx\n"
"\n"
"   Environment variables in SSH mode:\n"
"       IQSYNC_CMD              The command to be run on the remote side\n"
"       IQSYNC_RSH              The local rsh/ssh command to be run\n"
"\n"
"\n"
"TCP Server Usage:\n"
"   iqsync --type tcp --server [IPADDR:]PORT [options...]\n"
"\n"
"TCP Client Usage:\n"
"   iqsync --type tcp [options...] IPADDR:PORT\n"
"   iqsync --type tcp [options...] --launch-server user@host:/path/to/remote.iqx\n"
"\n"
"To invoke from netcat, create a shell script to invoke iqsync and\n"
"invoke it with \"--server unused --type ssh\".  Netcat will wait for\n"
"a connection and then iqsync will read/write from stdin/stdout over\n"
"the socket.\n"
"\n"
;

    fprintf(stream, "%s%s", msg, usage_str);
    exit(EXIT_FAILURE);
}

static int
iqsync_setup_filter(
    iqsync_t * const iqsync,
    const char *arg)
{
    char *filter_arg = strdup(arg);
    char *colon = strchr(filter_arg, ':');
    if (!colon)
        TSABORTX("badly formatted filter argument '%s', "
            "MUST be in <lib>:<func>\n", optarg);

    *colon = 0;
    const char *filename = filter_arg;
    const char *symbol = colon + 1;

    void *lib = dlopen(filename, RTLD_LAZY);
    if (!lib)
        TSABORT("could not open library '%s': %s", filename, dlerror());

    iqsync_filter_setup_fn_t setup = dlsym(lib, symbol);
    if (!setup)
        TSABORT("could not find filter setup function '%s'"
            "in library '%s'",
            symbol, filename);

    iqsync->filter_count++;
    iqsync->filters = realloc(iqsync->filters, sizeof(*iqsync->filters) * iqsync->filter_count);
    iqsync->filters[iqsync->filter_count - 1] = (iqsync_filter_t) {
        .filter_setup = setup,
    };

    free(filter_arg);

    return 0;
}

static int
iqsync_start_remote(
    iqsync_t * const iqsync,
    const char * remote_name,
    bool is_tcp,
    int fds[],
    pid_t *pchild
)
{
    // Get remote host:file from argv
    iqsync->remote.name = remote_name;
    if (!iqsync->remote.name)
        TSABORTX("Remote iqueue must be specified");

    char * remote_host = strdup(iqsync->remote.name);
    if (!remote_host) abort();
    char * remote_file = index(remote_host, ':');
    if (!remote_file)
        TSABORTX("Unable to parse remote iqueue name '%s'", remote_host);
    *(remote_file++) = '\0';

    // Check for an env variable for the iqsync command
    const char * remote_cmd = getenv("IQSYNC_CMD");
    if (!remote_cmd)
        remote_cmd = "iqsync";
    const char * ssh_cmd = getenv("IQSYNC_RSH");
    if (!ssh_cmd)
        ssh_cmd = "/usr/bin/ssh";

    // Determine my name to pass as the remote hostname
    char my_name[1024];
    if (is_tcp) {
        strncpy(my_name, "0", sizeof(my_name));
    } else {
        if (gethostname(my_name, sizeof(my_name)) < 0)
            TSABORT("Unable to get my hostname");
        int name_len = strlen(my_name);
        my_name[name_len++] = ':';
        strncpy(my_name + name_len, iqsync->local.name, sizeof(my_name) - name_len);
    }

    char usleep_str[16];
    snprintf(usleep_str, sizeof(usleep_str), "%d", iqsync->usleep_time);

    char rate_limit_str[16];
    snprintf(rate_limit_str, sizeof(rate_limit_str),
	"%"PRIu64,
	iqsync->rate_limit);

    char connection_timeout_str[16];
    snprintf(connection_timeout_str, sizeof(connection_timeout_str),
        "%"PRIu64,
        iqsync->connection_timeout_sec);

    char sendbuffer_size_str[16];
    snprintf(sendbuffer_size_str, sizeof(sendbuffer_size_str),
        "%"PRIu32,
        (iqsync->sendbuffer_len ? iqsync->sendbuffer_len : DEFAULT_SENDBUFFER_SIZE));
    char recvbuffer_size_str[16];
    snprintf(recvbuffer_size_str, sizeof(recvbuffer_size_str),
        "%"PRIu32,
        (iqsync->recvbuffer_len ? iqsync->recvbuffer_len : DEFAULT_RECVBUFFER_SIZE));

    // Redirect stdin/stdout, but let it write to our stderr
    pid_t child = tsio_open3(
        fds,
        TSIO_STDIN_MASK | TSIO_STDOUT_MASK,
        ssh_cmd,
        (const char *[]) {
            ssh_cmd,
            remote_host,
            remote_cmd,
            "--server", my_name,
            "--type", is_tcp ? "tcp" : "ssh",
            "-f",
            remote_file,
            "--sleep", usleep_str,
            "--rate-limit", rate_limit_str,
            "--connection-timeout", connection_timeout_str,
            "--send-buffer-size", sendbuffer_size_str,
            "--recv-buffer-size", recvbuffer_size_str,
            iqsync->verbose ? "--verbose" : "--nop",
            iqsync->quiet ? "--quiet" : "--nop",
            iqsync->do_push ? ( iqsync->do_clone_push ? "--clone" : "--pull" ) : "--nop", // note reversed sense
            iqsync->do_pull ? "--push" : "--nop", // note reversed sense
            iqsync->do_tail ? "--tail" : "--nop",
            iqsync->do_hdr_validate ? "--validate" : "--nop",
            iqsync->remote_cpu ? "--cpu" : "--nop",
            iqsync->remote_cpu ? iqsync->remote_cpu : "--nop",
            is_tcp ? "--launched-by-client" : "--nop",
            iqsync->use_sendbuffer ? "--send-buffer" : "--nop",
            iqsync->use_recvbuffer ? "--recv-buffer" : "--nop",
            0
        }
    );
    if (child < 0)
        TSABORTX("Unable to fork %s", ssh_cmd);

    if (pchild)
        *pchild = child;

    free(remote_host);
    return 0;
}

static int
iqsync_setup_ssh(
    iqsync_t * const iqsync,
    const char * remote_name
)
{
    // If we are in srever mode, everything is setup
    if (iqsync->do_server)
        return 0;

    int fds[3];
    if (iqsync_start_remote(iqsync, remote_name, false, fds, NULL) < 0) {
        return -1;
    }

    iqsync->read_fd = fds[1];
    iqsync->write_fd = fds[0];

    return 0;
}

static int
iqsync_setup_tcp_both_side(
    iqsync_t * const iqsync,
    const char * const remote_name)
{
    pid_t child = -1;
    int fds[3];
    if (iqsync_start_remote(iqsync, remote_name, true, fds, &child) < 0) {
        return -1;
    }

    uint16_t nport = 0;
    char * ptr = (char *)&nport;
    size_t offset = 0;
    int64_t timeout_nanos = 0;
    if (iqsync->connection_timeout_sec > 0)
        timeout_nanos = tsclock_getnanos(0) + iqsync->connection_timeout_sec * NANOS_IN_SECOND;

    int flags = fcntl(fds[1], F_GETFL, 0);
    if (fcntl(fds[1], F_SETFL, flags | O_NONBLOCK))
        TSABORTX("Unable to set nonblocking flag");

    while (offset < sizeof(nport)) {

        if (timeout_nanos > 0 && tsclock_getnanos(0) > timeout_nanos) {
            TSLOGX(TSERROR, "Timed out while waiting for port number from the server");
            return -1;
        }

        ssize_t rlen = read(fds[1], ptr + offset, sizeof(nport) - offset);
        if (rlen < 0) {
            if (errno == EAGAIN)
                continue;

            TSLOGL(TSERROR, "failed to receive port number from the server");
            waitpid(child, NULL, 0);
            return -1;
        }

        offset += rlen;
    }

    uint16_t server_port = be16toh(nport);
    char server_port_str[8];
    snprintf(server_port_str, sizeof(server_port_str), "%"PRIu16, server_port);

    char * remote_host = strdup(remote_name);
    if (!remote_host) abort();
    char * remote_file = index(remote_host, ':');
    if (!remote_file)
        TSABORTX("Unable to parse remote iqueue name '%s'", remote_host);
    *(remote_file++) = '\0';

    const char *host_name = remote_host;
    if (index(remote_host, '@') != NULL) 
        host_name = index(remote_host, '@') + 1;

    if (iqsync->connection_timeout_sec > 0)
        timeout_nanos = tsclock_getnanos(0) + iqsync->connection_timeout_sec * NANOS_IN_SECOND;

    while (1)
    {
        int fd = tsnet_tcp_client_socket(host_name, server_port_str, 0);
        if (fd < 0) {
            if (timeout_nanos > 0 && tsclock_getnanos(0) > timeout_nanos)
            {
                TSLOGL(TSERROR,
                    "Timed out while trying to connect to %s:%s",
                    host_name, server_port_str);
                waitpid(child, NULL, 0);
                return -1;
            }
            continue;
        }

        iqsync->read_fd = fd;
        iqsync->write_fd = fd;

        free(remote_host);
        return 0;
    }
}

static int
iqsync_setup_tcp(
    iqsync_t * const iqsync,
    const char * const remote_name,
    const bool launched_by_client
)
{
    static char default_port[] = "20809";
    static char default_bind[] = "0.0.0.0";

    if (!iqsync->do_server)
    {
        // client connects make a TCP socket and are done.
        char * name = strdup(remote_name);
        char * port = strchr(name, ':');
        if (port)
            *port++ = '\0';
        else
            port = default_port;

        TSLOGXL(TSINFO, "%s: Connecting to %s:%s",
            iqsync->local.name,
            name,
            port
        );

        int fd = tsnet_tcp_client_socket(name, port, 0);
        if (fd < 0)
        {
            TSLOGL(TSERROR, "Unable to connect to %s:%s", name, port);
            return -1;
        }

        iqsync->read_fd = fd;
        iqsync->write_fd = fd;
        iqsync->remote.name = name;
        return 0;
    }

    TSLOGX(TSINFO, "opening iqueue %s", iqsync->local.name);
    // Make sure the parameters are correct; can not do a clone into
    // a non-existant iqueue
    // only get write access if we are pulling i
    const bool writable = iqsync->do_pull ? true : false;
    iqsync->iq = iqueue_open(iqsync->local.name, writable);
    if (!iqsync->iq)
    {
        TSLOGXL(TSERROR, "%s: Unable to open", iqsync->local.name);
        return -1;
    }

    // Use the remote server name for [[IP:]PORT] to bind to
    char * server_name = default_bind;
    char * port_name = default_port;

    char * orig_name = strdup(iqsync->remote.name);
    if (iqsync->remote.name[0] != '\0')
    {
        char * colon = strchr(orig_name, ':');
        if (!colon)
            port_name = orig_name;
        else {
            *port_name++ = '\0';
            server_name = orig_name;
        }
    }

    int server_fd = tsnet_tcp_server_socket(server_name, port_name, 0);
    if (server_fd < 0)
        TSABORT("tcp bind to %s:%s", server_name, port_name);

    struct sockaddr_in server_addr;
    socklen_t server_len = sizeof(server_addr);
    if (getsockname(server_fd, &server_addr, &server_len) < 0)
        TSABORT("unable to getsockname");

    uint16_t server_port = be16toh(server_addr.sin_port);

    TSLOGXL(TSINFO,
        "%s: Waiting for inbound connections on TCP port %s:%"PRIu16,
        iqsync->local.name,
        server_name,
        server_port
    );

    if (launched_by_client) {
        // send port number to client
        uint16_t nport = htobe16(server_port);
        ssize_t wlen = write(STDOUT_FILENO, &nport, sizeof(nport));
        if (wlen < 0 || wlen != (ssize_t)sizeof(nport))
            TSABORT("failed to write to stdout");

        fsync(STDOUT_FILENO);
    }

    while (1)
    {
        if (launched_by_client && iqsync->connection_timeout_sec) {
            struct timeval timeout = {
                .tv_sec = iqsync->connection_timeout_sec,
                .tv_usec = 0
            };

            fd_set readfds;
            FD_ZERO(&readfds);
            FD_SET(server_fd, &readfds);
            int rc = select(server_fd + 1, &readfds, NULL, NULL, &timeout);
            if (rc < 0) {
                TSABORT("%s: Select failed", iqsync->local.name);
            } else if (rc == 0) {
                TSABORTX("%s: Timed out while waiting for connection", iqsync->local.name);
            }
        }

        struct sockaddr_in remote_addr;
        socklen_t remote_len = sizeof(remote_addr);
        int fd = accept(server_fd, &remote_addr, &remote_len);
        if (fd < 0)
            TSABORT("accept %s:%"PRIu16, server_name, server_port);

        // Duplicate the iqsync_t for the new connection
        iqsync_t * const new_iqsync = calloc(1, sizeof(*new_iqsync));
        if (!new_iqsync)
            TSABORT("unable to allocate iqsync object");
        *new_iqsync = *iqsync;

        char client_name[256];
        snprintf(client_name, sizeof(client_name), "%s:%d",
            inet_ntoa(remote_addr.sin_addr),
            ntohs(remote_addr.sin_port)
        );
        new_iqsync->remote.name = strdup(client_name);

        TSLOGXL(TSINFO, "%s: Connected to %s",
            new_iqsync->local.name,
            new_iqsync->remote.name
        );

        new_iqsync->read_fd = fd;
        new_iqsync->write_fd = fd;

        if (iqsync_start(new_iqsync) < 0)
            return -1;

        if (launched_by_client) {
            if (iqsync_wait(new_iqsync) < 0)
                TSABORTX("iqsync_wait failed");

            break;
        } else {
            // Detatch from the threads so that they will exit cleanly
            // This will leak the new_iqsync object, but that is ok for now.
            pthread_detach(new_iqsync->push_thread);
            pthread_detach(new_iqsync->pull_thread);
            pthread_detach(new_iqsync->stat_thread);
        }
    }

    TSLOGXL(TSINFO, "exiting success");
    free(orig_name);
    close(server_fd);
    exit(EXIT_SUCCESS);
}

int
main(
    int argc,
    char **argv
)
{
    signal(SIGPIPE, SIG_IGN);

    iqsync_t * const iqsync = calloc(1, sizeof(*iqsync));
    if (!iqsync)
	TSABORT("alloc failed");

    *iqsync = (iqsync_t) {
        .report_interval        = 600,
        .usleep_time            = 100,
        .heartbeats_lock        = tslock_alloc(),
        .read_fd                = STDIN_FILENO,
        .write_fd               = STDOUT_FILENO,
        
        .connection_timeout_sec     = 120,
    };
    if (!iqsync->heartbeats_lock)
	TSABORT("lock alloc failed");

    int option_index = 0;
    const char * transport_type = "ssh";
    bool prefetch = false;
    bool syncbehind = false;
    bool launched_by_client = false;
    bool launch_server = false;

    while (1)
    {
        int c = getopt_long(
            argc,
            argv,
            "h?f:ts:pvVZR:Cr:T:m:c:K:ebOq",
            long_options,
            &option_index
        );

        if (c == -1)
            break;

        switch (c)
        {
        case 0: break;
        default: usage(stderr, ""); break;
        case 'h': case '?': usage(stdout, ""); break;

        case 'Z': break; // nop
        case 'C':
            iqsync->do_clone = 1;
            iqsync->do_hdr_validate = 1;
            iqsync->do_pull = 1;
            break;
        case 'O':
            iqsync->do_clone_push = 1;
            iqsync->do_hdr_validate = 1;
            iqsync->do_push = 1;
            break;
        case 'f': iqsync->local.name = optarg; break;
        case 't': iqsync->do_tail = 1; break;
        case 'c': iqsync->local_cpu = optarg; break;
        case 'K': iqsync->remote_cpu = optarg; break;
        case 'M': iqsync->rate_limit = strtoul(optarg, NULL, 0); break;
        case 'T': transport_type = optarg; break;
        case 'p': iqsync->do_push = 1; break;
        case 'P': iqsync->do_pull = 1; break;
        case 'V': iqsync->do_hdr_validate = 1; break;
        case 'v': iqsync->verbose++; tslevel = TSDEBUG; break;
        case 'q': iqsync->quiet++; tslevel = TSWARN; break;
        case 'r': iqsync->report_interval = strtoul(optarg, NULL, 0); break;
        case 'R':
            iqsync->do_server = 1;
            iqsync->remote.name = optarg;
            break;
        case 's':
            iqsync->usleep_time = strtoul(optarg, NULL, 0);
            break;
        case 'e':
            prefetch = true;
            break;
        case 'b':
            syncbehind = true;
            break;
	case 'F': {
            if (iqsync_setup_filter(iqsync, optarg) < 0)
                usage(stderr, "failed to parse filter argument");
	    break;
	}
        case 1:
            launch_server = true;
            break;
        case 2:
            iqsync->connection_timeout_sec = strtoul(optarg, NULL, 0);
            break;
        case 3:
            launched_by_client = true;
            break;
        case 4:
            iqsync->use_sendbuffer = 1;
            break;
        case 5:
            iqsync->sendbuffer_len = strtoul(optarg, NULL, 0);
            if (iqsync->sendbuffer_len == 0)
                usage(stderr, "Invalid sendbuffer length");
            else if (ceilintpow2(iqsync->sendbuffer_len) != iqsync->sendbuffer_len)
                usage(stderr, "sendbuffer length must be power of 2");
            break;
        case 6:
            iqsync->use_recvbuffer = 1;
            break;
        case 7:
            iqsync->recvbuffer_len = strtoul(optarg, NULL, 0);
            if (iqsync->recvbuffer_len == 0)
                usage(stderr, "Invalid recvbuffer length");
            else if (ceilintpow2(iqsync->recvbuffer_len) != iqsync->recvbuffer_len)
                usage(stderr, "recvbuffer length must be power of 2");
            break;
        }
    }

    if (iqsync->verbose && iqsync->quiet)
        usage(stderr, "Quiet and verbose modes cannot be set concurrently!\n");

    if (!iqsync->local.name)
        usage(stderr, "iqueue file must be specified!\n");

    if (!iqsync->do_push && !iqsync->do_pull)
        usage(stderr, "At least one of --push / --pull must be specified!\n");

    if (strcmp(transport_type, "ssh") == 0)
    {
        if (iqsync_setup_ssh(iqsync, argv[optind]) < 0)
            return -1;
    } else
    if (strcmp(transport_type, "tcp") == 0)
    {
        if (!iqsync->do_server && launch_server) {
            if (iqsync_setup_tcp_both_side(iqsync, argv[optind]) < 0)
                return -1;
        } else {
            if (iqsync_setup_tcp(
                    iqsync, argv[optind], launched_by_client) < 0)
                return -1;
        }
    } else
        usage(stderr, "Unknown --type option!\n");

    iqsync->do_prefetch = prefetch;
    iqsync->do_syncbehind = syncbehind;

    // All configured.  Start the threads
    if (iqsync_start(iqsync) < 0)
	TSABORTX("iqsync_start failed");

    if (iqsync_wait(iqsync) < 0)
	TSABORTX("iqsync_wait failed");

    TSLOGXL(TSINFO, "exiting success");
    return 0;
}
