/*      $TwoSigma: iqsync-main.c,v 1.6 2012/02/02 20:53:59 thudson Exp $       */

/*
 *      Copyright (c) 2010 Two Sigma Investments, LLC
 *      All Rights Reserved
 *
 *      THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
 *      Two Sigma Investments, LLC.
 *
 *      The copyright notice above does not evidence any
 *      actual or intended publication of such source code.
 */
#include "twosigma.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <getopt.h>
#include <ctype.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "tsutil.h"
#include "tsio.h"
#include "tsclock.h"
#include "tsnet.h"
#include "iqueue.h"
#include "iqsync.h"
#include "segfault.h"

__RCSID("$TwoSigma: iqsync-main.c,v 1.6 2012/02/02 20:53:59 thudson Exp $");



/** \file
 * Push or pull changes from an iqueue with another over stdin/stdout
 * tunneled through ssh, or over a TCP socket.
 */

static struct option long_options[] = {
    { "help",           no_argument,        0, '?' },
    { "iqueue",         required_argument,  0, 'f' },
    { "tail",           no_argument,        0, 't' },
    { "server",		required_argument,  0, 'R' },
    { "sleep",          required_argument,  0, 's' },
    { "rate-limit",	required_argument,  0, 'M' },
    { "report-interval", required_argument, 0, 'r' },
    { "type",           required_argument,  0, 'T' },
    { "push",           no_argument,        0, 'p' },
    { "pull",           no_argument,        0, 'P' },
    { "nop",            no_argument,        0, 'Z' },
    { "validate",	no_argument,	    0, 'V' },
    { "verbose",	no_argument,	    0, 'v' },
    { "clone",          no_argument,        0, 'C' },
    { "remote-cpu",     required_argument,  0, 'K' },
    { "cpu",            required_argument,  0, 'c' },
    { "prefetch",       no_argument,        0, 'e' },
    { "syncbehind",     no_argument,        0, 'b' },
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
"   -T | --type {ssh|tcp}       Transport type (default ssh)\n"
"   -c | --cpu N                Bind local push thread to CPU N\n"
"   -K | --remote-cpu N         Bind remote push thread to CPU N (only with ssh)\n"
"   -e | --prefetch             Create prefetch thread\n"
"   -b | --syncbehind           Create syncbehind thread\n"
"\n"
"Cloning options:\n"
"   Cloning implies --pull and --validate.  If the local sizes are not\n"
"   specified the remote sizes will be used.  Bi-directional cloning\n"
"   is supported.\n"
"\n"
"   -C | --clone                Clone a remote iqueue\n"
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
iqsync_setup_ssh(
    iqsync_t * const iqsync,
    const char * remote_name
)
{
    // If we are in srever mode, everything is setup
    if (iqsync->do_server)
	return 0;

    // Get remote host:file from argv
    iqsync->remote.name = remote_name;
    if (!iqsync->remote.name)
	TSABORTX("Remote iqueue must be specified");

    char * remote_host = strdup(iqsync->remote.name);
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
    if (gethostname(my_name, sizeof(my_name)) < 0)
	TSABORT("Unable to get my hostname");
    int name_len = strlen(my_name);
    my_name[name_len++] = ':';
    strncpy(my_name + name_len, iqsync->local.name, sizeof(my_name) - name_len);

    char usleep_str[16];
    snprintf(usleep_str, sizeof(usleep_str), "%d", iqsync->usleep_time);

    char rate_limit_str[16];
    snprintf(rate_limit_str, sizeof(rate_limit_str), "%"PRIu64, iqsync->rate_limit);

    // Redirect stdin/stdout, but let it write to our stderr
    int fds[3];
    pid_t child = tsio_open3(
	fds,
	TSIO_STDIN_MASK | TSIO_STDOUT_MASK,
	ssh_cmd,
	(const char *[]) {
	    ssh_cmd,
	    remote_host,
	    remote_cmd,
	    "--server", my_name,
	    "--type", "ssh",
	    "-f",
	    remote_file,
	    "--sleep", usleep_str,
	    "--rate-limit", rate_limit_str,
	    iqsync->verbose ? "--verbose" : "--nop",
	    iqsync->do_push ? "--pull" : "--nop", // note reversed sense
	    iqsync->do_pull ? "--push" : "--nop", // note reversed sense
	    iqsync->do_tail ? "--tail" : "--nop",
	    iqsync->remote_cpu ? "--cpu" : "--nop",
	    iqsync->remote_cpu ? iqsync->remote_cpu : "--nop",
	    0
	}
    );
    if (child < 0)
	TSABORTX("Unable to fork %s", ssh_cmd);

    iqsync->read_fd = fds[1];
    iqsync->write_fd = fds[0];

    return 0;
}


static int
iqsync_setup_tcp(
    iqsync_t * const iqsync,
    const char * const remote_name
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

	TSLOGX(TSINFO, "%s: Connecting to %s:%s",
	    iqsync->local.name,
	    name,
	    port
	);

	int fd = tsnet_tcp_client_socket(name, port, 0);
	if (fd < 0)
	{
	    TSLOG(TSERROR, "Unable to connect to %s:%s", name, port);
	    return -1;
	}

	iqsync->read_fd = fd;
	iqsync->write_fd = fd;
	iqsync->remote.name = name;
	return 0;
    }

    // Make sure the parameters are correct; can not do a clone into
    // a non-existant iqueue
    // only get write access if we are pulling i
    const bool writable = iqsync->do_pull ? true : false;
    iqsync->iq = iqueue_open(iqsync->local.name, writable);
    if (!iqsync->iq)
    {
	TSLOGX(TSERROR, "%s: Unbale to open", iqsync->local.name);
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

    TSLOGX(TSINFO,
	"%s: Waiting for inbound connections on TCP port %s:%s",
	iqsync->local.name,
	server_name,
	port_name
    );

    while (1)
    {
	struct sockaddr_in remote_addr;
	socklen_t remote_len = sizeof(remote_addr);
	int fd = accept(server_fd, &remote_addr, &remote_len);
	if (fd < 0)
	    TSABORT("accept %s:%s", server_name, port_name);

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

	TSLOGX(TSINFO, "%s: Connected to %s",
	    new_iqsync->local.name,
	    new_iqsync->remote.name
	);

	new_iqsync->read_fd = fd;
	new_iqsync->write_fd = fd;

	if (iqsync_start(new_iqsync) < 0)
	    return -1;

	// Detatch from the threads so that they will exit cleanly
	// This will leak the new_iqsync object, but that is ok for now.
	pthread_detach(new_iqsync->push_thread);
	pthread_detach(new_iqsync->pull_thread);
	pthread_detach(new_iqsync->stat_thread);
    }

    // Unreachable
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
    //segfault_handler_install();
    iqsync_t * const iqsync = calloc(1, sizeof(*iqsync));

    *iqsync = (iqsync_t) {
	.report_interval	= 600,
	.usleep_time		= 100,
	.heartbeats_lock	= tslock_alloc(),
	.read_fd		= STDIN_FILENO,
	.write_fd		= STDOUT_FILENO,
    };

    int option_index = 0;
    const char * usleep_time_str = "0";
    const char * transport_type = "ssh";
    bool prefetch = false;
    bool syncbehind = false;

    while (1)
    {
	int c = getopt_long(
	    argc,
	    argv,
	    "h?f:ts:pvVZR:Cr:T:m:c:K:eb",
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
	case 'r': iqsync->report_interval = strtoul(optarg, NULL, 0); break;
	case 'R':
	    iqsync->do_server = 1;
	    iqsync->remote.name = optarg;
	    break;
	case 's':
	    usleep_time_str = optarg;
	    iqsync->usleep_time = strtoul(optarg, 0, 0);
	    break;
	case 'e':
	    prefetch = true;
	    break;
	case 'b':
	    syncbehind = true;
	    break;
	}
    }

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
	if (iqsync_setup_tcp(iqsync, argv[optind]) < 0)
	    return -1;
    } else
	usage(stderr, "Unknown --type option!\n");

    iqsync->do_prefetch = prefetch;
    iqsync->do_syncbehind = syncbehind;

    // All configured.  Start the threads
    if (iqsync_start(iqsync) < 0)
	return -1;

    if (iqsync_wait(iqsync) < 0)
	return -1;

    return 0;
}
