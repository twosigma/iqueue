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
#ifndef _dma_transport_iqsync_h_
#define _dma_transport_iqsync_h_

/** \file
 * iqsync magic constants for both the ssh and udp versions.
 */
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include "iqueue.h"
#define IQSYNC_HANDSHAKE_MAGIC  0x495148414E440005

/** Send at the start of TCP connection or every few seconds by
 * the multicast version.
 */
struct iqsync_handshake
{
    uint64_t magic;
    uint64_t creation;
    uint64_t entries;
    uint64_t hdr_len;
    uint8_t hdr[];
} __attribute__((packed));


/** Sent as a resend request to please replay from the
 * starting index.  The replay will start from the specified
 * index in the remote iqueue and will be sequenced with the
 * desired sequence number.
 *
 * This is never sent to the multicast group; unicast only.
 */
#define IQSYNC_START_MAGIC 0x4951535452540003

struct iqsync_start
{
    uint64_t magic;
    uint64_t start_seq;
    uint64_t start_index;
    uint64_t flags;
} __attribute__((packed));


/** Sent at the front of each packet, either unicast or multicast */
#define IQSYNC_DATA_MAGIC 0x4951444154410004

struct iqsync_data
{
    uint64_t magic;
    uint64_t src; // sending source
    uint64_t iq_index; // sending index
    uint64_t orig_src; // original source (for cycle detection)
    uint64_t orig_index; // original source
    uint32_t len;
    uint8_t data[];
} __attribute__((packed));


/** Update of heartbeats that have changed since the last heartbeat */
#define IQSYNC_HEARTBEAT_MAGIC 0x4951444154410005
struct iqsync_heartbeat
{
    uint64_t magic_be64;
    uint64_t count_be64;
    shash_entry_t writers[];
} __attribute__((packed));


/** Shadow the actual values for an iqueue into a temporary for
 * safe keeping.
 */
typedef struct
{
    const char * name;
    uint64_t index;
    uint64_t creation;
    uint64_t entries;
    uint64_t hdr_len;
    uint64_t count; // msgs received
    uint64_t len; // bytes received
    void * hdr;
} iqsync_shadow_t;


/**
 * iqsync filtering function, which will be called on every outbound
 * local iqueue message before it is sent to the remote side (push)
 * Valid return codes are:
 *   1: Forward the message
 *   0: Skip the message
 *   -1: Stop processing futher messages and exit
 */
typedef int (*iqsync_filter_fn_t)(void *handle, const void *buf, size_t len);
typedef int (*iqsync_filter_setup_fn_t)(
        const void *buf,
        size_t len,
	void **filter_fn_priv,
        iqsync_filter_fn_t *filter_fn);

typedef struct
{
    iqsync_filter_setup_fn_t filter_setup;
    iqsync_filter_fn_t filter_fn;

    void *filter_fn_priv;
} iqsync_filter_t;

/** Book keeping and options for the iqsync process.
 * This is not the easiest structure to use for outside processes;
 * it will likely have some significant rework if any other
 * applications want to use the iqsync algorithm.
 */
#define DEFAULT_RECVBUFFER_SIZE (1 << 20)       // 1MB
#define DEFAULT_SENDBUFFER_SIZE (1 << 12)       // 4KB

struct extremely_dangerous_internal_tslock_s;
typedef struct extremely_dangerous_internal_tslock_s tslock_t;
typedef struct
{
    int read_fd;
    int write_fd;

    int do_clone;
    int do_clone_push;
    int do_tail;
    int do_push;
    int do_pull;
    int do_hdr_validate;
    int do_server;
    int do_prefetch;
    int do_syncbehind;
    int use_sendbuffer;
    int use_recvbuffer;

    volatile int do_shutdown;
    int usleep_time;
    int verbose;
    int quiet;
    uint64_t report_interval;
    uint64_t rate_limit; // in MB/s
    uint64_t avg_msg_len; // in bytes
    uint64_t connection_timeout_sec;

    iqueue_t * iq;
    bool close_iq_on_shutdown;

    shash_t * sources;
    shash_entry_t * scan_index;

    tslock_t * heartbeats_lock;
    shash_t * heartbeats_hash;
    shash_entry_t * heartbeats;
    shash_entry_t * heartbeats_copy;
    struct iqsync_heartbeat * heartbeat_msg;
    unsigned heartbeats_max;

    uint64_t start_time;
    uint64_t report_time;
    uint64_t report_tx_count;
    uint64_t report_rx_count;
    uint64_t report_rx_len;
    int warned_cycle;

    const char * local_cpu;
    const char * remote_cpu;
    pthread_t push_thread;
    pthread_t pull_thread;
    pthread_t stat_thread;
    pthread_t prefetch_thread;
    pthread_t syncbehind_thread;

    pthread_mutex_t stats_shutdown_mutex;
    pthread_cond_t stats_shutdown_cond;

    iqsync_shadow_t remote;
    iqsync_shadow_t local;

    unsigned filter_count;
    iqsync_filter_t *filters;

    int64_t initialization_rc;
    int wait_complete;

    uint32_t recvbuffer_len;
    uint8_t recvbuffer_block_shift;
    uint64_t recvbuffer_offset_mask;
    uint32_t sendbuffer_len;
    uint8_t sendbuffer_block_shift;
    uint64_t sendbuffer_offset_mask;

    /**
     * internal buffer used to store packets
     */
    volatile uint64_t recvbuffer_read_idx;
    volatile uint64_t recvbuffer_write_idx;
    char *recvbuffer;

    uint32_t sendbuffer_data_len;
    char *sendbuffer;

} iqsync_t;


int
iqsync_start_async(
    iqsync_t * iqsync
);

int
iqsync_start_async_wait(
    iqsync_t * iqsync
);

int
iqsync_start(
    iqsync_t * iqsync
);


/** Wait for the push/pull threads to exit.
 * \note Does not close the iqsync->iq iqueue.
 */
int
iqsync_wait(
    iqsync_t * iqsync
);


void
iqsync_stats(
    iqsync_t * iqsync
);

const struct iqsync_data *
iqsync_data_msg(
    iqueue_t * const iq,
    const uint64_t offset
);

#endif
