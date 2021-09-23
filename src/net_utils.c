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
#include "tslog.h"
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#define BACKLOG 250

/**
 * Sets up a socket
 *
 * param #1: node
 * param #2: service
 * param #3: socket type, e.g. SOCK_STREAM, SOCK_DGRAM
 * param #4: setup function, e.g. connect(), bind()
 *
 * Returns non-negative file descriptor on success, -1 on failure
 */
static int
setup_socket(const char *node, const char *service,
    int (*action)(int, const struct sockaddr *, socklen_t))
{
    struct addrinfo *aai, *ai;
    int fd, ret;

    struct addrinfo hints = {};

    hints.ai_family = PF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    ret = getaddrinfo(node, service, &hints, &aai);
    if (ret != 0) {
        TSLOGX(TSWARN, "cannot resolve %s:%s (%s)", node, service,
               gai_strerror(ret));
        errno = ENOENT;
        return -1;
    }
    if (aai == NULL) {
        TSLOGX(TSWARN, "no addresses for %s:%s", node, service);
        errno = ENOENT;
        return -1;
    }

    for (ai = aai; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, SOCK_STREAM, ai->ai_protocol);
        if (fd == -1)
            continue;

        int sockoptvalue = 1;
        if (action == bind) {
            ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sockoptvalue, sizeof(sockoptvalue));
            if (ret == -1) {
                TSLOG(TSWARN, "cannot set reuseaddr");
                (void)close(fd);
                fd = -1;
                continue;
            }
        }

        ret = (*action)(fd, ai->ai_addr, ai->ai_addrlen);
        if (ret == -1) {
            (void)close(fd);
            fd = -1;
            continue;
        }
        break;
    }
    freeaddrinfo(aai);
    if (fd == -1) {
        TSLOG(TSWARN, "could not setup socket %s:%s of type %d",
              node, service, SOCK_STREAM);
        return -1;
    }

    int sockopt = 1;
    ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &sockopt, sizeof(sockopt));
    if (ret == -1) {
        TSLOG(TSWARN, "cannot set tcp nodelay to %d", sockopt);
        close(fd);
        return -1;
    }
    return fd;
}

/**
 * Creates TCP client socket
 *
 * param #1: node
 * param #2: service
 *
 * Returns non-negative file descriptor on success, -1 on failure
 */
int
tsnet_tcp_client_socket(const char *node, const char *service)
{
    assert(node != NULL);
    assert(service != NULL);
    return setup_socket(node, service, connect);
}

/**
 * Creates TCP server socket
 *
 * param #1: node
 * param #2: service
 *
 * Returns non-negative file descriptor on success, -1 on failure
 */
int
tsnet_tcp_server_socket(const char *node, const char *service)
{
    assert(node != NULL);
    assert(service != NULL);
    int fd = setup_socket(node, service, bind);
    if (fd == -1)
        return -1;
    if (listen(fd, BACKLOG) == -1) {
        TSLOG(TSWARN, "cannot listen on socket %s:%s", node, service);
        close(fd);
        return -1;
    }
    return fd;
}
