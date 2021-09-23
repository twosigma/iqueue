/*      $TwoSigma: iqsync.c,v 1.24 2012/02/07 13:37:40 thudson Exp $       */

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
#include "bswap.h"
#include "tsutil.h"
#include "tslock.h"
#include "tsio.h"
#include "tsclock.h"
#include "tssched.h"
#include "iqueue.h"
#include "iqsync.h"
#include "segfault.h"

__RCSID("$TwoSigma: iqsync.c,v 1.24 2012/02/07 13:37:40 thudson Exp $");

/** \file
 * Core iqsync algorithm and threads.
 *
 * See iqsync-main.c for an example of how to create an iqsync_t object
 * and start them.
 */

void
iqsync_stats(
    iqsync_t * const iqsync
)
{
    const uint64_t now = tsclock_getnanos(0);

    const uint64_t rx_len = iqsync->remote.len;
    const uint64_t rx_count = iqsync->remote.count;
    const uint64_t tx_count = iqsync->local.count;
    const uint64_t report_delta = now - iqsync->report_time;

    if (iqsync->do_pull)
    TSLOGX(TSINFO, "%s: RX index %"PRId64": %"PRIu64" messages %.3f kpps, %.2f MB/s",
	iqsync->remote.name,
	iqsync->remote.index,
	rx_count,
	(rx_count - iqsync->report_rx_count) * 1.0e6 / report_delta,
	(rx_len - iqsync->report_rx_len) * 1.0e3 / report_delta
    );

    if (iqsync->do_push)
    TSLOGX(TSINFO, "%s: TX index %"PRId64": %"PRIu64" messages %.3f kpps, avg size %"PRIu64" bytes",
	iqsync->remote.name,
	iqsync->local.index,
	tx_count,
	(tx_count - iqsync->report_tx_count) * 1.0e6 / report_delta,
	iqsync->avg_msg_len
    );

    iqsync->report_rx_count = rx_count;
    iqsync->report_tx_count = tx_count;
    iqsync->report_rx_len = rx_len;
    iqsync->report_time = now;
}


static void *
iqsync_stat_thread(
    void * const iqsync_ptr
)
{
    iqsync_t * const iqsync = iqsync_ptr;
    if (iqsync->report_interval == 0)
	return NULL;

    while (!iqsync->do_shutdown)
    {
	sleep(iqsync->report_interval);
	iqsync_stats(iqsync);
    }

    return NULL;
}



/** Return a pointer to the containing iqsync_msg_t if the
 * message in the iqueue is of type iqsync_msg.  Returns NULL
 * otherwise.
 */
const struct iqsync_data *
iqsync_data_msg(
    iqueue_t * const iq,
    const uint64_t offset
)
{
    const void * const data = iqueue_get_data(iq, offset, 1);
    if (!data)
	return NULL;

    const struct iqsync_data * const msg = container_of(
	(void*)(uintptr_t) data,
	const struct iqsync_data,
	data
    );

    // Make sure that the offset does not cross the front of
    // a block boundary, which would indicate that this is not
    // a iqsync message, and also check the magic value
    // to be sure that the msg header is intact.
    if ((offset & IQUEUE_BLOCK_MASK) < offsetof(struct iqsync_data, data)
    || msg->magic != htobe64(IQSYNC_DATA_MAGIC))
	return NULL;

    // It appears to be a valid iqsync message.
    return msg;
}


/** Setup the local source table, initializing the persistent source table
 * if it does not yet exist.
 */
static int
iqsync_sources_setup(
    iqsync_t * const iqsync
)
{
    iqueue_t * const iq = iqsync->iq;
    shash_t * const sh = iqueue_writer_table(iq, 1, 1);

    if (!sh)
    {
	TSLOGX(TSERROR, "%s: Unable to create iqsync sources table",
	    iqueue_name(iq)
	);
	return -1;
    }

    iqsync->sources = shash_copy(sh);
    iqsync->scan_index = shash_insert_or_get(iqsync->sources, -1, 0);

    TSLOGX(TSINFO, "%s: Sources table skipping to index %"PRIu64,
	iqueue_name(iq),
	iqsync->scan_index->value
    );

    return 0;
}


static int
iqsync_hash_update(
    shash_t * const sh,
    const uint64_t src_id,
    const uint64_t src_index
)
{
    shash_entry_t * source = shash_get(sh, src_id);

    if (!source)
    {
	// Writer does not yet exist; create a new one.
	// If this succeeds, the source id not yet exist and the newly
	// created one will have the value that we provided.
	source = shash_insert(sh, src_id, src_index);

	// If it did not succeed, then we raced with another thread to
	// create this entry in the hash and must follow the update
	// protocol by retrieving the existing one
	if (!source)
	    source = shash_get(sh, src_id);

	// If there is still no source, the iqueue is corrupted
	if (!source)
	    TSABORTX("corrupt iqueue? bad source behaviour");
    }

    return iqueue_writer_update(sh, source, src_index);
}


/** Retrieve a source from the shared hash, or insert it with
 * a zero-value if it does not yet exist.
 */
static uint64_t
iqsync_sources_get(
    iqsync_t * const iqsync,
    const uint64_t src_id
)
{
    if (src_id == 0)
	return 0;

    shash_entry_t * const source = shash_insert_or_get(
	iqsync->sources,
	src_id,
	0
    );

    return source->value;
}


/** Read all messages newer than the scan index shared variable
 * and update the shared sources hash table through the end of the
 * iqueue.
 *
 * \param src_index_out will be set to point to the latest src index
 * for the src_id, if it is non-zero.
 *
 * \return The local index slot id into which the message should be stored.
 */
static uint64_t
iqsync_sources_scan_all(
    iqsync_t * const iqsync,
    const uint64_t src_id,
    uint64_t * const src_index_out
)
{
    iqueue_t * const iq = iqsync->iq;

    // Read the current shared scan index.  The shared hash table cache
    // is guaranteed to be current at least up to this value.  It might
    // be newer if a writer has crashed between updating the hash
    // but before it could update the scan index.  In either event,
    // this process must scan from that point to the end of the iqueue
    // and bring the shared hash table up to date.
    //
    // It is very important that this value is read before the expected
    // value for src_id.  The race is if the hash[src_id] is read, and then
    // some values from src_id are added, and another process moves scan_index
    // to the end of the iqueue, then this reader will not see the messages
    // that would have updated hash[src_id] and will write duplicate messages.
    const uint64_t orig_scan_index = iqsync->scan_index->value;

    // Retrieve the current value of the cached src index for src id.
    // This might be newer than the value of scan_index, but that is ok
    // since the linear scan will pass by the message that would have
    // advanced hash[src_id].
    uint64_t src_index = iqsync_sources_get(iqsync, src_id);

    uint64_t scan_index = orig_scan_index;

    while (1)
    {
	size_t len;
	const uint64_t offset = iqueue_offset(iq, scan_index, &len);

	// At the end of the queue?  We're done scanning.
	if (offset == (uint64_t) -1)
	    break;

	// There is a new message; update our iterator
	scan_index++;

	// If the scanned slot does not contain an iqsync message,
	// we can ignore it and move on to the next slot
	const struct iqsync_data * const old_msg = iqsync_data_msg(iq, offset);
	if (!old_msg)
	    continue;

	// Update the hash table for this original source and the iqueue
	// through which it might have been routed.
	// If this races with other iqsyncs, the highest value will win.
	// The update does not need to do an atomic in the case were another
	// iqsync has already updated the table.
	const uint64_t orig_src = be64toh(old_msg->orig_src);
	const uint64_t orig_index = be64toh(old_msg->orig_index);
	const uint64_t route_src = be64toh(old_msg->src);
	const uint64_t route_index = be64toh(old_msg->iq_index);

	// If the source and routed sources are the same, the indices
	// had better agree.  Otherwise something has gone horribly wrong
	// in the protocol.
	if (orig_src == route_src
	&&  orig_index != route_index)
	    TSABORTX("%s: iqsync protocol error!"
		" original source %"PRIu64".%"PRIu64
		" != routed %"PRIu64".%"PRIu64
		"!",
		iqsync->remote.name,
		orig_src,
		orig_index,
		route_src,
		route_index
	    );

	// Update the original and routing iqueue indices in the shared table
	// The value recorded is the next expected index, not the most
	// recently seen index.  This allows the value to start at 0,
	// rather than -1.
	iqsync_hash_update(iqsync->sources, orig_src, orig_index + 1);
	if (orig_src != route_src)
	    iqsync_hash_update(iqsync->sources, route_src, route_index + 1);

	// If the caller has specified a source that matches either id,
	// update the tell them the index for that source
	if (src_id == orig_src)
	    src_index = orig_index + 1;
	else
	if (src_id == route_src)
	    src_index = route_index + 1;
    }

    // We've hit the end of the iqueue.  Update the tail pointer in
    // in the shared hash table with the location to begin searching the
    // next scan; this might fail if other threads have
    // advanced it since this process read the scan index, but that is ok
    // since the scan_index is only maintained with loose consistency.
    const uint64_t cur_scan_index = iqsync->scan_index->value;
    if (cur_scan_index < scan_index)
	shash_update(
	    iqsync->sources,
	    iqsync->scan_index,
	    cur_scan_index,
	    scan_index
	);

    // Indicate to the caller the src_index of the last message from
    // src_id.
    if (src_index_out)
	*src_index_out = src_index;

    // And return how far we have scanned through the iqueue.
    // If there is no race, this will be the first empty element of
    // the iqueue, which is where it will append the incoming message.

    if (orig_scan_index != scan_index)
	TSLOGX(TSDEBUG, "%s: Scanned from %"PRIu64" to %"PRIu64,
	    iqsync->local.name,
	    orig_scan_index,
	    scan_index
	);

    return scan_index;
}


/** Read all the messages that have not yet been processed to update
 * the latest sequence number from each of the sources.
 *
 * \param cur_src The incoming message source to check against, or NULL to
 * examine the entire queue without concern about sources.
 *
 * \return The slot that the current message should be written to,
 * or IQUEUE_MSG_BAD_ID if the message should be discarded,.
 */
static inline uint64_t
iqsync_sources_scan(
    iqsync_t * const iqsync,
    const uint64_t cur_src,
    const uint64_t cur_index
)
{
    // One of our own?
    if (cur_src == iqsync->local.creation)
	goto discard;

    // Refresh to the end of the iqueue and retrieve the
    // latest message index from the current source.
    uint64_t src_index;
    const uint64_t scan_index = iqsync_sources_scan_all(
	iqsync,
	cur_src,
	&src_index
    );

    // So far, so good.  If the incoming message is the next expected
    // one, then we're done.
    if (cur_index >= src_index)
	return scan_index;

    // The incoming message has already been seen.
    // Signal that it should be discarded.
discard:
    if (iqsync->verbose || iqsync->warned_cycle == 0)
    {
	iqsync->warned_cycle = 1;
	TSLOGX(TSINFO, "%s: Discarding %"PRIu64".%"PRIu64,
	    iqsync->remote.name,
	    cur_src,
	    cur_index
	);
    }

    return IQUEUE_MSG_BAD_ID;
}


static int
iqsync_start_recv(
    iqsync_t * const iqsync
)
{
    iqueue_t * const iq = iqsync->iq;

    // Read where they want us to start
    struct iqsync_start start;
    if (tsio_read_all(
	iqsync->read_fd,
	&start,
	sizeof(start)
    ) != sizeof(start))
    {
	TSLOGX(TSERROR, "%s: Start message read failed", iqsync->remote.name);
	return -1;
    }

    const uint64_t remote_magic = be64toh(start.magic);
    if (remote_magic != IQSYNC_START_MAGIC)
    {
	TSLOGX(TSERROR, "%s: Start message bad magic %"PRIx64" != expected %"PRIx64,
	    iqsync->remote.name,
	    remote_magic,
	    IQSYNC_START_MAGIC
	);
	return -1;
    }

    iqsync->local.index = be64toh(start.start_index);
    //uint64_t flags = be64toh(start.flags);

    if (iqsync->local.index > iqueue_entries(iq))
	TSLOGX(TSWARN,
	    "%s: Starting at %"PRIu64", but only %"PRIu64" entries so far",
	    iqsync->local.name,
	    iqsync->local.index,
	    iqueue_entries(iq)
	);

    if (iqsync->verbose)
    TSLOGX(TSINFO, "%s: Starting at %"PRIu64"/%"PRIu64" and will %s when done",
	iqsync->local.name,
	iqsync->local.index,
	iqueue_entries(iq),
	iqsync->do_tail ? "tail" : "exit"
    );

    return 0;
}


static int
iqsync_push_one(
    iqsync_t * const iqsync,
    const uint64_t local_index,
    const uint64_t offset,
    size_t data_len
)
{
    struct iqsync_data msg = {
	.magic	    = htobe64(IQSYNC_DATA_MAGIC),
	.src	    = htobe64(iqsync->local.creation),
	.orig_src   = htobe64(iqsync->local.creation),
	.orig_index = htobe64(local_index),
	.iq_index   = htobe64(local_index),
	.len	    = htobe32((uint32_t) data_len),
    };

    // Check to see if this is one that we received from
    // the remote side.  If so, we do not send it on.
    const struct iqsync_data * const sync_msg = iqsync_data_msg(iqsync->iq, offset);
    if (sync_msg)
    {
	// Avoid the obvious cycle to our direct correspondent
	if (sync_msg->src == htobe64(iqsync->remote.creation))
	    return 1;

	// Flag the message with the original source and index
	// Note that the values in the sync_msg are already in
	// network byte order
	msg.orig_src	= sync_msg->orig_src;
	msg.orig_index	= sync_msg->orig_index;
    }

    const void * data = iqueue_get_data(iqsync->iq, offset, 1);

    struct iovec iov[] = {
	{ .iov_base = &msg, .iov_len = sizeof(msg) },
	{ .iov_base = (void*)(uintptr_t) data, .iov_len = data_len },
    };

    size_t total_len = iov[0].iov_len + iov[1].iov_len;

    if (iqsync->verbose)
	TSLOGX(TSINFO, "%s: sending index %"PRIu64": %zu bytes",
	    iqsync->local.name,
	    local_index,
	    data_len
	);

    ssize_t wlen = tsio_writev_all(iqsync->write_fd, total_len, iov, 2);
    if (wlen < 0)
    {
	TSLOG(TSERROR, "%s: write failed!", iqsync->remote.name);
	return -1;
    }

    if (wlen != (ssize_t) total_len)
    {
	TSLOGX(TSWARN, "%s: Connection closed", iqsync->remote.name);
	return 0;
    }

    iqsync->local.count++;
    return 1;
}


/** Prevent the aggregate read rate from exceeding the rate limit.
 * This makes iqsync on a large file less likely to blow out the caches.
 */
static void
iqsync_rate_limit(
    iqsync_t * const iqsync,
    uint64_t start_time,
    size_t len
)
{
    // Compute a moving, weighted average of the message len to determine
    // the appropriate sleep time
    const uint64_t avg_len = iqsync->avg_msg_len = (iqsync->avg_msg_len * 7 + len) / 8;
    const uint64_t limit = iqsync->rate_limit << 20; // scale MB/s to B/s

    if (limit == 0)
	return;

    uint64_t delta = tsclock_getnanos(0) - start_time;

    uint64_t ns_sleep_time = (avg_len * 1000000000ul) / limit;
    if (ns_sleep_time < delta)
	return;

    if (ns_sleep_time < 60000)
    {
	// Just busy wait until our time period has expired
	while ((uint64_t) tsclock_getnanos(0) < start_time + ns_sleep_time)
	    continue;
    } else {
	// The minimum sleep time seems to be about 60 usec,
	ns_sleep_time -= delta;
	nanosleep(&(struct timespec) {
	    .tv_sec	= ns_sleep_time / 1000000000ull,
	    .tv_nsec	= ns_sleep_time % 1000000000ull
	}, NULL);
    }
}


/** Setup the heartbeat table in the iqueue and local maps of it.
 *
 * If create_flag is not set, the function only checks to see if
 * there exists a heartbeat table.
 * Otherwise it will try to create one and allocate the local copies
 * for determining when heartbeat status has changed.
 */
static int
iqsync_setup_heartbeats(
    iqsync_t * const iqsync,
    const int create_flag
)
{
    if (iqsync->heartbeats_hash)
	return 1;

    tslock(iqsync->heartbeats_lock);
    if (iqsync->heartbeats_hash)
    {
	tsunlock(iqsync->heartbeats_lock);
	return 1;
    }

    shash_t * sh = iqueue_writer_table(
	iqsync->iq,
	0, // default writer table
	create_flag
    );

    // If it still doesn't exist, don't worry about it.  No heartbeats
    // will be pushed until it is created.
    if (!sh)
    {
	if (create_flag)
	    TSABORTX("%s: Unable to create heartbeat table",
		iqsync->local.name
	    );

	tsunlock(iqsync->heartbeats_lock);
	return 0;
    }

    // Create a thread-local version
    sh = shash_copy(sh);
    iqsync->heartbeats = shash_entries(sh, &iqsync->heartbeats_max);

    TSLOGX(TSDEBUG, "%s: Heartbeat table %p has %u entries",
	iqsync->local.name,
	iqsync->heartbeats,
	iqsync->heartbeats_max
    );

    iqsync->heartbeats_copy = calloc(1,
	iqsync->heartbeats_max * sizeof(*iqsync->heartbeats_copy)
    );
    if (!iqsync->heartbeats_copy)
	TSABORT("failed to allocate %u writers", iqsync->heartbeats_max);

    iqsync->heartbeat_msg = calloc(1,
	sizeof(*iqsync->heartbeat_msg)
	+ iqsync->heartbeats_max * sizeof(*iqsync->heartbeat_msg->writers)
    );
    if (!iqsync->heartbeat_msg)
	TSABORT("failed to allocate %u writers message", iqsync->heartbeats_max);

    iqsync->heartbeats_hash = sh;
    tsunlock(iqsync->heartbeats_lock);

    return 1;
}


/** Send a set of messages, from iqsync->local.index to end_index.
 * \return 0 on success, -1 on any failures.
 */
static int
iqsync_send_set(
    iqsync_t * const iqsync,
    const uint64_t end_index
)
{
    const uint64_t start_index = iqsync->local.index;

    while (iqsync->local.index < end_index)
    {
	const uint64_t i = iqsync->local.index++;
	size_t len;
	const uint64_t offset = iqueue_offset(iqsync->iq, i, &len);

	if (offset == (uint64_t) -1)
	{
	    TSLOGX(TSERROR, "%s: No data at index %"PRIu64"?",
		iqsync->local.name,
		i
	    );
	    return -1;
	}

	uint64_t start_time = tsclock_getnanos(0);
	if (iqsync_push_one(iqsync, i, offset, len) <= 0)
	    return -1;

	iqsync_rate_limit(iqsync, start_time, len);
    }

    if (start_index != end_index)
	TSLOGX(TSDEBUG, "%s: Send %"PRIu64" to %"PRIu64,
	    iqsync->local.name,
	    start_index,
	    end_index
	);

    return 0;
}



static int
iqsync_push_heartbeats(
    iqsync_t * const iqsync
)
{
    if (!iqsync_setup_heartbeats(iqsync, 0))
	return 0;
    struct iqsync_heartbeat * const msg = iqsync->heartbeat_msg;

    unsigned count = 0;

    for (unsigned i = 0 ; i < iqsync->heartbeats_max ; i++)
    {
	// This does not guarantee ordering of updates to different keys.
	const shash_entry_t heartbeat = iqsync->heartbeats[i];
	shash_entry_t * const copy = &iqsync->heartbeats_copy[i];
	if (heartbeat.key == 0)
	    break;
	if (heartbeat.key == copy->key && heartbeat.value == copy->value)
	    continue;

	// A new timestamp.  Update the cached copy
	memcpy(copy, &heartbeat, sizeof(*copy));
	memcpy(&msg->writers[count++], &heartbeat, sizeof(*copy));
    }

    if (count == 0)
	return 0;

    msg->magic_be64 = htobe64(IQSYNC_HEARTBEAT_MAGIC);
    msg->count_be64 = htobe64(count);

    // Make sure that all pending messages have been sent
    // to ensure that heartbeats do not arrive before any messages
    // that were written before the hearbeat.
    if (iqsync_send_set(iqsync, iqueue_entries(iqsync->iq)) < 0)
	return -1;

    // At this point the cached copy of the heartbeat table might be
    // old, but it meets the guarantee that all messages that were
    // present at the time it was duplicated have been sent to the
    // destination iqueue.  It is now safe to send the entire table
    // of updates.

    ssize_t wlen = tsio_write_all(
	iqsync->write_fd,
	msg,
	sizeof(*msg) + count * sizeof(msg->writers[0])
    );
    if (wlen <= 0)
	return -1;

    TSLOGX(TSDEBUG, "%s: Sent %u heartbeat updates", iqsync->local.name, count);

    return 0;
}


/** Send entries to the remote side.
 *
 * At this point everything is correctly configured and we have exclusive
 * access to the write file descriptor.
 */
static void *
iqsync_push_thread(
    void * const iqsync_ptr
)
{
    iqsync_t * const iqsync = iqsync_ptr;
    iqueue_t * const iq = iqsync->iq;

    while (!iqsync->do_shutdown)
    {
	// By only sending up to the end of the iqueue at the current
	// time, we ensure that progress is made on the heartbeat sending.
	if (iqsync_send_set(iqsync, iqueue_entries(iq)) < 0)
	{
	    TSLOGX(TSWARN, "%s: Send set failed", iqueue_name(iq));
	    break;
	}

	// Everytime there is no data, check for heartbeats
	if (iqsync_push_heartbeats(iqsync) < 0)
	{
	    TSLOGX(TSWARN, "%s: Heartbeat send failed", iqueue_name(iq));
	    break;
	}

	if (!iqsync->do_tail)
	{
	    TSLOGX(TSINFO, "%s: Reached end and not tailing", iqueue_name(iq));
	    break;
	}

	if (iqueue_is_sealed(iq))
	{
	    TSLOGX(TSINFO, "%s: Has been sealed", iqueue_name(iq));
	    break;
	}

	if (iqsync->usleep_time)
	    usleep(iqsync->usleep_time);
    }

    if (iqsync->verbose)
    TSLOGX(TSINFO, "%s: Done sending at index %"PRIu64,
	iqsync->remote.name,
	iqsync->local.index
    );

    close(iqsync->write_fd);
    if (!iqsync->do_pull)
	iqsync->do_shutdown = 1;

    return NULL;
}


/** Receive a new iqsync_data msg and determine if it is a duplicate of
 * one already received.
 *
 * \param msg should point to a message that has been received in the
 * iqueue that is being synchronized.
 *
 * \return 1 if committed, 0 if it is a duplicate and should be discarded
 * (with iqueue_realloc() to zero length) or -1 on error.
 */
static int
iqsync_recv(
    iqsync_t * const iqsync,
    const struct iqsync_data * const msg,
    iqueue_msg_t iqmsg
)
{
    const size_t data_len = be32toh(msg->len);
    const uint64_t orig_src = be64toh(msg->orig_src);
    const uint64_t orig_index = be64toh(msg->orig_index);
    const uint64_t remote_index = be64toh(msg->iq_index);

    // Adjust the message to skip the data at the head
    const size_t data_offset = offsetof(struct iqsync_data, data);
    const iqueue_msg_t new_iqmsg = iqueue_msg(
	iqueue_msg_offset(iqmsg) + data_offset,
	iqueue_msg_len(iqmsg) - data_offset
    );

    while (1)
    {
	const uint64_t local_index = iqsync_sources_scan(
	    iqsync,
	    orig_src,
	    orig_index
	);
	if (local_index == IQUEUE_MSG_BAD_ID)
	    return 0;

	// Try to store the new entry at the last slot scanned.
	// Note that the index entry points to the data section,
	// not the iqsync_msg header portion (which will be in the file for
	// future reference).
	int rc = iqueue_try_update(
	    iqsync->iq,
	    local_index,
	    new_iqmsg
	);

	// We were not successful; rescan the sources and try again
	if (rc == IQUEUE_STATUS_HAS_DATA)
	{
	    TSLOGX(TSDEBUG, "%s: Lost race at %"PRIu64" for %"PRIx64".%"PRIu64,
		iqsync->local.name,
		local_index,
		orig_src,
		orig_index
	    );
	    continue;
	}

	if (rc == IQUEUE_STATUS_SEALED)
	{
	    TSLOGX(TSWARN, "%s: File has been sealed.  Stopping sync.", iqueue_name(iqsync->iq));
	    return -1;
	}

	if (rc != 0)
	{
	    TSLOGX(TSERROR, "%s: Unable to store %zu bytes at %"PRIu64"! rc=%d",
		iqsync->local.name,
		data_len,
		local_index,
		rc
	    );
	    return -1;
	}

	// We have successfully written at the desired slot, which means
	// no new messages arrived while we were consulting the hash tables.
	if (iqsync->verbose)
	    TSLOGX(TSINFO, "%s: Stored remote %"PRIu64" as %"PRIu64,
		iqsync->local.name,
		remote_index,
		local_index
	    );

	iqsync->remote.count++;
	iqsync->remote.index = remote_index;

	return 1;
    }
}


static int
iqsync_pull_one_data(
    iqsync_t * const iqsync,
    iqueue_allocator_t * const allocator
)
{
    const size_t alloc_len = sizeof(struct iqsync_data) + IQUEUE_MSG_MAX;

    iqueue_msg_t iqmsg;
    struct iqsync_data * const msg = iqueue_allocate(
	allocator,
	alloc_len,
	&iqmsg
    );
    if (!msg)
    {
	TSLOGX(TSERROR, "%s: Unable to allocate message", iqsync->local.name);
	return -1;
    }

    ssize_t rlen = tsio_read_all(
	iqsync->read_fd,
	((uint8_t*) msg) + sizeof(msg->magic),
	sizeof(*msg) - sizeof(msg->magic)
    );
    if (rlen < 0)
	return -1; // error
    if (rlen != sizeof(*msg) - sizeof(msg->magic))
	return 0; // closed fd

    // Fill in the magic header
    msg->magic = htobe64(IQSYNC_DATA_MAGIC);

    // If this is just a keep-alive, we have nothing else to process
    if (msg->len == 0 && msg->src == 0 && msg->orig_src == 0)
    {
	iqueue_realloc_bulk(allocator, &iqmsg, alloc_len, 0);
	return 1;
    }

    const size_t data_len = be32toh(msg->len);
    const size_t msg_len = sizeof(*msg) + data_len;

    iqsync->remote.len += data_len;

    if (data_len > IQUEUE_MSG_MAX)
    {
	TSLOGX(TSERROR, "%s: Message %"PRIu64" len %zu greater than max %zu",
	    iqsync->remote.name,
	    be64toh(msg->iq_index),
	    data_len,
	    (size_t) IQUEUE_MSG_MAX
	);
	return -1;
    }

    rlen = tsio_read_all(iqsync->read_fd, msg->data, data_len);
    if (rlen < 0)
	return -1; // error
    if (rlen != (ssize_t) data_len)
	return 0; // closed fd

    if (iqueue_realloc_bulk(
	allocator,
	&iqmsg,
	alloc_len,
	msg_len
    ) < 0) {
	TSLOGX(TSERROR, "%s: Unable to resize from %zu to %zu?",
	    iqsync->local.name,
	    alloc_len,
	    msg_len
	);
	return -1;
    }

    // Now that the message has been fully received into the buffer,
    // try to post it to the iqueue.
    int rc = iqsync_recv(iqsync, msg, iqmsg);
    if (rc == 1)
	return 1;
    if (rc < 0)
	return -1;

    // Too old or from ourselves; discard it, but do not signal an error
    iqueue_realloc_bulk(allocator, &iqmsg, alloc_len, 0);
    return 1;
}


static int
iqsync_pull_one_heartbeat(
    iqsync_t * const iqsync
)
{
    iqsync_setup_heartbeats(iqsync, 1);

    struct iqsync_heartbeat msg;
    ssize_t rlen;

    rlen = tsio_read_all(
	iqsync->read_fd,
	((uint8_t*) &msg) + sizeof(msg.magic_be64),
	sizeof(msg) - sizeof(msg.magic_be64)
    );
    if (rlen <= 0)
	return (int) rlen;

    const uint64_t count = be64toh(msg.count_be64);
    if (count > iqsync->heartbeats_max)
	TSABORTX("%s: Sent %"PRIu64" heartbeats?  Max %u",
	    iqsync->remote.name,
	    count,
	    iqsync->heartbeats_max
	);

    shash_entry_t heartbeats[count];
    rlen = tsio_read_all(iqsync->read_fd, heartbeats, sizeof(heartbeats));
    if (rlen <= 0)
	return rlen;

    for (unsigned i = 0 ; i < count ; i++)
    {
	shash_entry_t * const heartbeat = &heartbeats[i];

	if (heartbeat->key == 0 || heartbeat->key == ~(uint64_t) 0)
	{
	    TSLOGX(TSWARN, "%s: Sent writer with invalid id/timestamp %"PRIx64":%"PRIx64"?",
		iqsync->remote.name,
		heartbeat->key,
		heartbeat->value
	    );
	    continue;
	}

	iqsync_hash_update(
	    iqsync->heartbeats_hash,
	    heartbeat->key,
	    heartbeat->value
	);
    }

    TSLOGX(TSDEBUG, "Received %"PRIu64" heartbeats", count);

    return 1;
}


/** Read a iqsync_msg and append the data to the local iqueue.
 * \return 1 on success, 0 on connection closed, -1 on error.
 */
static int
iqsync_pull_one(
    iqsync_t * const iqsync,
    iqueue_allocator_t * const allocator
)
{
    uint64_t magic_be64;
    ssize_t rlen = tsio_read_all(iqsync->read_fd, &magic_be64, sizeof(magic_be64));
    if (rlen < 0)
	return -1;
    if (rlen != sizeof(magic_be64))
	return 0;

    const uint64_t magic = be64toh(magic_be64);
    if (magic == IQSYNC_DATA_MAGIC)
	return iqsync_pull_one_data(iqsync, allocator);
    if (magic == IQSYNC_HEARTBEAT_MAGIC)
	return iqsync_pull_one_heartbeat(iqsync);

    TSLOGX(TSERROR, "%s: Bad magic %"PRIx64".  Unknown type!",
	iqsync->remote.name,
	magic
    );
    return -1;
}


/** Send the final part of the handshake to tell the remote side
 * to begin sending data.
 *
 * This requires that the source table be up to date with the sequence
 * number that is desired from the remote side; if it is not exactly right
 * there is no harm.  The remote side will send some messages that will be
 * discarded since they are already present in the local iqueue.
 */
static int
iqsync_start_send(
    iqsync_t * const iqsync
)
{
    // If there is a writer in the table, we know that we have heard from
    // at least that position and can resume there.  If there was a mistake
    // in computing the starting index, it might be too low and the first
    // few incoming packets will be dropped.
    iqsync->remote.index = iqsync_sources_get(
	iqsync,
	iqsync->remote.creation
    );

    // Send the ack asking to start at the next message, or 0
    // if there is no value already recorded for this source.
    struct iqsync_start start = {
	.magic		= htobe64(IQSYNC_START_MAGIC),
	.start_index	= htobe64(iqsync->remote.index),
	.flags		= htobe64(0),
    };

    if (tsio_write_all(
	iqsync->write_fd,
	&start,
	sizeof(start)
    ) != sizeof(start))
    {
	TSLOGX(TSERROR, "%s: Write error on start message", iqsync->remote.name);
	return -1;
    }

    if (iqsync->verbose)
	TSLOGX(TSINFO, "send RX request start at %"PRIu64, iqsync->remote.index);

    return 0;
}


/** Read entries from the remote iqueue and write them into our local one.
 *
 * At this point everything is correctly configured and handshaken, so
 * we have exclusive read access to the file descriptor.
 */
static void *
iqsync_pull_thread(
    void * const iqsync_ptr
)
{
    iqsync_t * const iqsync = iqsync_ptr;

    // Pre-allocate some space for incoming messages
    iqueue_allocator_t allocator;
    if (iqueue_allocator_init(
	iqsync->iq,
	&allocator,
	IQUEUE_MSG_MAX * 4, // try to avoid re-filling too often
	1
    ) < 0)
	TSABORTX("%s: Unable to create allocator", iqsync->local.name);

    // Read messages until we have an error or a closed connection
    int rc;
    while ((rc = iqsync_pull_one(iqsync, &allocator)) == 1)
    {
	// nop
	// \todo: check for sealed iqueue
    }

    if (rc == 0)
    {
	if (iqsync->verbose)
	TSLOGX(TSINFO, "%s: Connection closed: index %"PRIu64,
	    iqsync->remote.name,
	    iqsync->remote.index
	);
    } else {
	TSLOG(TSERROR, "%s: Read failed: index %"PRIu64,
	    iqsync->remote.name,
	    iqsync->remote.index
	);
    }

    iqsync->do_shutdown = 1;
    close(iqsync->read_fd);

    return NULL;
}



/** Send the handshake message to the remote side.
 * This describes the parameters of the iqueue on our side.
 */
static int
iqsync_handshake_send(
    iqsync_t * const iqsync
)
{
    iqueue_t * const iq = iqsync->iq;

    iqsync->local.hdr = iqueue_header(iq, &iqsync->local.hdr_len);
    iqsync->local.creation = iqueue_creation(iq);
    iqsync->local.entries = iqueue_entries(iq);

    if (iqsync->verbose)
    TSLOGX(TSINFO, "%s: Source creation=%"PRIu64" entries=%"PRIu64,
	iqsync->local.name,
	iqsync->local.creation,
	iqsync->local.entries
    );

    struct iqsync_handshake handshake = {
	.magic	    = htobe64(IQSYNC_HANDSHAKE_MAGIC),
	.creation   = htobe64(iqsync->local.creation),
	.entries    = htobe64(iqsync->local.entries),
	.hdr_len    = htobe64(iqsync->local.hdr_len),
    };

    // \todo Is this safe?  What if hdr is long?  Should there be
    // a split-phase handshake to ensure that we do not deadlock?
    struct iovec iov[] = {
	{ .iov_base = &handshake, .iov_len = sizeof(handshake) },
	{ .iov_base = (void*)(uintptr_t) iqsync->local.hdr, .iov_len = iqsync->local.hdr_len },
    };
    size_t total_len = iov[0].iov_len + iov[1].iov_len;

    ssize_t wlen = tsio_writev_all(iqsync->write_fd, total_len, iov, 2);
    if (wlen != (ssize_t) total_len)
    {
	TSLOGX(TSERROR,
	    "%s: handshake write failed: %zd != %zu",
	    iqsync->remote.name,
	    wlen,
	    total_len
	);
	return -1;
    }

    return 0;
}


/** Receive a remote handshake message and update our view of the
 * remote queue.
 */
static int
iqsync_handshake_recv(
    iqsync_t * const iqsync
)
{
    // Read the handshake from the remote side
    struct iqsync_handshake reply;
    ssize_t rlen = tsio_read_all(iqsync->read_fd, &reply, sizeof(reply));
    if (rlen != sizeof(reply))
    {
	TSLOGX(TSERROR, "%s: handshake read failed", iqsync->remote.name);
	return -1;
    }

    const uint64_t remote_magic = be64toh(reply.magic);
    if (remote_magic != IQSYNC_HANDSHAKE_MAGIC)
    {
	TSLOGX(TSERROR, "%s: bad handshake magic: %"PRIu64" != %"PRIu64,
	    iqsync->remote.name,
	    remote_magic,
	    IQSYNC_HANDSHAKE_MAGIC
	);
	return -1;
    }

    iqsync->remote.creation	= be64toh(reply.creation);
    iqsync->remote.entries	= be64toh(reply.entries);
    iqsync->remote.hdr_len	= be64toh(reply.hdr_len);

    if (iqsync->remote.hdr_len == 0)
	return 0;

    iqsync->remote.hdr = malloc(iqsync->remote.hdr_len);
    if (!iqsync->remote.hdr)
	TSABORT("hdr alloc failed: %"PRIu64" bytes", iqsync->remote.hdr_len);

    if (tsio_read_all(
	iqsync->read_fd,
	iqsync->remote.hdr,
	iqsync->remote.hdr_len
    ) != (ssize_t) iqsync->remote.hdr_len)
    {
	TSLOGX(TSERROR, "read of remote header failed");
	return -1;
    }

    return 0;
}



/** Receive the remote handshake and create the local iqueue based on the
 * remote parameters.
 */
static int
iqsync_handshake_clone(
    iqsync_t * const iqsync
)
{
    TSLOGX(TSINFO, "%s: Not present; cloning from remote %s",
	iqsync->local.name,
	iqsync->remote.name
    );

    // Receive the remote handshake message before sending ours
    if (iqsync_handshake_recv(iqsync) < 0)
	return -1;

    iqsync->local.hdr_len = iqsync->remote.hdr_len;

    if (iqsync->iq)
    {
	TSLOGX(TSINFO, "%s: Using existing local iqueue", iqueue_name(iqsync->iq));
    } else
    {
	const uint64_t local_creation = tsclock_getnanos(0);
	iqsync->iq = iqueue_create(
	    iqsync->local.name,
	    local_creation,
	    iqsync->remote.hdr,
	    iqsync->remote.hdr_len
	);

	if (!iqsync->iq)
	{
	    TSLOGX(TSERROR, "%s: Unable to open", iqsync->local.name);
	    return -1;
	}
	if (iqueue_creation(iqsync->iq) != local_creation)
	{
	    TSLOGX(TSWARN, "%s: already exists; verify header?", iqsync->local.name);
	}
    }

    // Exchange handshake messages now that we have an iqueue created
    if (iqsync_handshake_send(iqsync) < 0)
	return -1;

    return 0;
}


/** Both sides should exist; do the normal exchange */
static int
iqsync_handshake_normal(
    iqsync_t * const iqsync
)
{
    // only get write access if we are pulling i
    const bool writable = iqsync->do_pull ? true : false;

    if (iqsync->iq != NULL)
    {
	iqsync->local.name = iqueue_name(iqsync->iq);
    }
    else
    {
	iqsync->iq = iqueue_open(iqsync->local.name, writable);
	if (!iqsync->iq)
	{
	    TSLOGX(TSERROR, "%s: Unable to open", iqsync->local.name);
	    return -1;
	}
	iqsync->close_iq_on_shutdown = true;
    }

    // Exchange handshake messages
    if (iqsync_handshake_send(iqsync) < 0)
	return -1;
    if (iqsync_handshake_recv(iqsync) < 0)
	return -1;

    if (!iqsync->do_hdr_validate)
	return 0;

    if (iqsync->remote.hdr_len != iqsync->local.hdr_len)
    {
	TSLOGX(TSERROR, "%s: remote header %"PRIu64" bytes != local %"PRIu64,
	    iqsync->remote.name,
	    iqsync->remote.hdr_len,
	    iqsync->local.hdr_len
	);
	return -1;
    }

    if (memcmp(iqsync->remote.hdr, iqsync->local.hdr, iqsync->local.hdr_len) != 0)
    {
	TSLOGX(TSERROR, "Remote header");
	TSHDUMP(TSERROR, iqsync->remote.hdr, iqsync->remote.hdr_len);
	TSLOGX(TSERROR, "Local header");
	TSHDUMP(TSERROR, iqsync->local.hdr, iqsync->local.hdr_len);
	return -1;
    }

    return 0;
}


static void *
iqsync_send_hb_thread(
    void * const iqsync_ptr
)
{
    iqsync_t * const iqsync = iqsync_ptr;

    const struct iqsync_data msg = {
	.magic	    = htobe64(IQSYNC_DATA_MAGIC),
	.src	    = 0,
	.orig_src   = 0,
	.len	    = 0,
    };

    while (!iqsync->do_shutdown)
    {
	sleep(1);

	ssize_t wlen = write(iqsync->write_fd, &msg, sizeof(msg));
	if (wlen == sizeof(msg))
	    continue;

	TSLOG(TSERROR, "%s: Short write", iqsync->remote.name);
	break;
    }

    iqsync->do_shutdown = 1;
    close(iqsync->write_fd);

    return NULL;
}


static void *
iqsync_recv_hb_thread(
    void * const iqsync_ptr
)
{
    iqsync_t * const iqsync = iqsync_ptr;

    while (!iqsync->do_shutdown)
    {
	struct iqsync_data msg;
	ssize_t rlen = read(iqsync->read_fd, &msg, sizeof(msg));
	if (rlen != sizeof(msg))
	{
	    TSLOG(TSERROR, "%s: Short read?", iqsync->remote.name);
	    break;
	}

	if (msg.magic == htobe64(IQSYNC_DATA_MAGIC)
	&&  msg.len == 0
	&&  msg.src == 0
	)
	    continue;

	TSLOGX(TSWARN, "%s: Sent non-empty heartbeat?", iqsync->remote.name);
	break;
    }

    iqsync->do_shutdown = 1;
    close(iqsync->read_fd);

    return NULL;
}


static void *
iqsync_init_thread(
    void * const iqsync_ptr
)
{
    iqsync_t * const iqsync = iqsync_ptr;

    // Rescan to build our table of sources; we don't care about what
    // we find, so we ignore the result and don't look for anything in
    // particular.  We only do this if we are pulling; push only mode
    // does not need to scan since the remote side will tell us where
    // to start.  This saves walking the entire file for a read-only mode
    if (iqsync->do_pull)
    {
	// If the file has been iqsync'ed already, then the sources
	// will be updated into the writer table and not much should need to
	// be scanned.  Bring things up to date with the end of the file
	// just in case.
	iqsync_sources_setup(iqsync);
	iqsync_sources_scan_all(iqsync, 0, NULL);

	// Handshake and scan done, exchange start messages

	if (iqsync_start_send(iqsync) < 0) {
	    iqsync->do_shutdown = 1;
	    return NULL;
	}
    }

    // Handshake and scan done, exchange start messages
    if (iqsync->do_push
    && iqsync_start_recv(iqsync) < 0) {
	iqsync->do_shutdown = 1;
	return NULL;
    }

    // Start the clock
    iqsync->start_time = iqsync->report_time = tsclock_getnanos(0);

    // We create a pull thread no matter what; it will just monitor the
    // socket to detect a close.  The pull thread doesn't do any
    // CPU pinning since it is spending all of its time in a read
    if (pthread_create(
	&iqsync->pull_thread,
	NULL,
	iqsync->do_pull ? iqsync_pull_thread : iqsync_recv_hb_thread,
	iqsync
    ) < 0)
	TSABORTX("Unable to create pull thread");

    // Likewise, the report thread is sleeping most of the time so it does
    // not do any cpu pinning.
    if (iqsync->report_interval
    && pthread_create(&iqsync->stat_thread, NULL, iqsync_stat_thread, iqsync) < 0)
	TSABORTX("Unable to create stats thread");

    // TODO: It would be better to do the work above on the pinned cpu, but the
    // spawned threads would inherit the affinity mask. This could be
    // re-factored to workj better (hold the threads or saved the mask).
    if (iqsync->local_cpu)
    {
	char * end;
	int cpu = strtoul(iqsync->local_cpu, &end, 0);
	if (end == iqsync->local_cpu)
	    TSABORTX("Unable to parse local cpu '%s'", iqsync->local_cpu);
	if (tssched_set_thread_affinity(pthread_self(), cpu) < 0)
	    TSABORT("Unable to set cpu affinity to %d", cpu);
	TSLOGX(TSINFO, "Pinned push thread to cpu %d", cpu);
    }

    if (iqsync->do_push)
	return iqsync_push_thread(iqsync);
    else
	return iqsync_send_hb_thread(iqsync);
}


/** Start the iqsync handshake process and spin off the send/receive
 * threads that handle the exchange of data (depending on --push / --pull).
 * If --report-interval is specified a stat reporting thread will also
 * be created.
 *
 * \todo Take advantage of atomic creation code.
 */
int
iqsync_start(
    iqsync_t * const iqsync
)
{
    // Check for an existing file
    struct stat statbuf;
    if (iqsync->do_clone
    && stat(iqsync->local.name, &statbuf) < 0)
    {
	if (errno != ENOENT)
	{
	    TSLOG(TSERROR, "%s: Unable to stat", iqsync->local.name);
	    return -1;
	}

	if (iqsync_handshake_clone(iqsync) < 0)
	    return -1;
    } else {
	if (iqsync_handshake_normal(iqsync) < 0)
	    return -1;
    }

    if (iqsync->do_prefetch
    && iqueue_prefetch_thread(iqsync->iq,
		&iqsync->prefetch_thread) != 0)
	    return -1;

    if (iqsync->do_syncbehind
    && iqueue_syncbehind_thread(iqsync->iq,
		&iqsync->syncbehind_thread) != 0)
	    return -1;

    // And kick off the threads to do the real work (the init thread
    // will become the push thread)
    if (pthread_create(
	&iqsync->push_thread,
	NULL,
	iqsync_init_thread,
	iqsync
    ) < 0)
	TSABORTX("Unable to create push thread");

    return 0;
}


int
iqsync_wait(
    iqsync_t * const iqsync
)
{
    // Wait for the thread to exit
    pthread_join(iqsync->push_thread, NULL);
    pthread_join(iqsync->pull_thread, NULL);

    if (iqsync->report_interval)
	pthread_cancel(iqsync->stat_thread);

    if (!iqsync->do_server)
	iqsync_stats(iqsync);

    if (iqsync->verbose)
	TSLOGX(TSINFO, "Exiting");

    if (iqsync->close_iq_on_shutdown)
	iqueue_close(iqsync->iq);

    close(iqsync->read_fd);
    close(iqsync->write_fd);

    return 0;
}
