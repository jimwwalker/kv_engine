/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "range_scans/range_scan.h"

#include "bucket_logger.h"
#include "ep_bucket.h"
#include "failover-table.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan_callbacks.h"
#include "vbucket.h"

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/chrono.h>
#include <memcached/cookie_iface.h>
#include <statistics/cbstat_collector.h>
#include <utilities/logtags.h>

RangeScan::RangeScan(EPBucket& bucket,
                     const VBucket& vbucket,
                     DiskDocKey start,
                     DiskDocKey end,
                     RangeScanDataHandlerIFace& handler,
                     const CookieIface* cookie,
                     cb::rangescan::KeyOnly keyOnly)
    : start(std::move(start)),
      end(std::move(end)),
      vbUuid(vbucket.failovers->getLatestUUID()),
      handler(handler),
      cookie(cookie),
      vbid(vbucket.getId()),
      state(State::Idle),
      keyOnly(keyOnly) {
    // Don't init the uuid in the initialisation list as we may read various
    // members which must be initialised first
    uuid = createScan(bucket);

    EP_LOG_DEBUG("RangeScan {} created for {}", uuid, getVBucketId());
}

cb::rangescan::Id RangeScan::createScan(EPBucket& bucket) {
    auto valFilter =
            cookie->isDatatypeSupported(PROTOCOL_BINARY_DATATYPE_SNAPPY)
                    ? ValueFilter::VALUES_COMPRESSED
                    : ValueFilter::VALUES_DECOMPRESSED;

    scanCtx = bucket.getRWUnderlying(getVBucketId())
                      ->initByIdScanContext(
                              std::make_unique<RangeScanDiskCallback>(*this,
                                                                      handler),
                              std::make_unique<RangeScanCacheCallback>(
                                      *this, bucket, handler),
                              getVBucketId(),
                              {ByIdRange{start, end}},
                              DocumentFilter::NO_DELETES,
                              valFilter);

    if (!scanCtx) {
        // KVStore logs more details
        throw cb::engine_error(
                cb::engine_errc::failed,
                fmt::format("RangeScan::createScan {} initByIdScanContext "
                            "returned nullptr",
                            getVBucketId()));
    }

    // Generate the scan ID (which may also incur i/o)
    return boost::uuids::random_generator()();
}

cb::engine_errc RangeScan::continueScan(KVStoreIface& kvstore) {
    EP_LOG_DEBUG("RangeScan {} continue for {}", uuid, getVBucketId());

    scanContinueDeadline = now() + timeLimit;
    auto status = kvstore.scan(*scanCtx);

    switch (status) {
    case ScanStatus::Yield: {
        // Scan reached a limit and has yielded
        // For RangeScan we have already consumed the last key, which is
        // different to DCP scans where the last key is read and 'rejected', so
        // must be read again. We adjust the startKey so we continue from next
        scanCtx->ranges[0].startKey.append(0);
        return cb::engine_errc::too_busy;
    }
    case ScanStatus::Success:
        // Scan has reached the end
    case ScanStatus::Cancelled:
        // Scan cannot continue and has cancelled (e.g. vbucket gone)
    case ScanStatus::Failed:
        // Scan cannot continue due to failure
        break;
    }
    // for Success/Cancelled/Failed for now state success. The plan is the
    // client will have been told already as the I/O task writes to their
    // connection. The caller to this method though can see a difference between
    // Yield and other status - Yield means the scan must not be cleaned up but
    // all other states mean the scan is cleaned up and never ran again
    return cb::engine_errc::success;
}

void RangeScan::setStateIdle() {
    switch (state) {
    case State::Idle:
    case State::Cancelled:
        throw std::runtime_error(fmt::format(
                "RangeScan::setStateIdle invalid state:", int(state.load())));
    case State::Continuing:
        state = State::Idle;
        break;
    }
}

void RangeScan::setStateContinuing(size_t limit,
                                   std::chrono::milliseconds timeLimit) {
    switch (state) {
    case State::Continuing:
    case State::Cancelled:
        throw std::runtime_error(
                fmt::format("RangeScan::setStateContinuing invalid state:",
                            int(state.load())));
    case State::Idle:
        state = State::Continuing;
        itemLimit = limit;
        itemCount = 0;
        this->timeLimit = timeLimit;
        break;
    }
}

void RangeScan::setStateCancelled() {
    switch (state) {
    case State::Cancelled:
        throw std::runtime_error(
                fmt::format("RangeScan::setStateCancelled invalid state:",
                            int(state.load())));
    case State::Idle:
    case State::Continuing:
        state = State::Cancelled;
        break;
    }
}

void RangeScan::incrementItemCount() {
    ++itemCount;
    ++totalItems;
}

bool RangeScan::areLimitsExceeded() {
    if (itemLimit) {
        return itemCount >= itemLimit;
    } else if (timeLimit.count()) {
        return now() > scanContinueDeadline;
    }
    return false;
}

std::function<std::chrono::steady_clock::time_point()> RangeScan::now = []() {
    return std::chrono::steady_clock::now();
};

void RangeScan::addStats(const StatCollector& collector) const {
    fmt::memory_buffer prefix;
    fmt::format_to(prefix, "vb_{}:{}", vbid.get(), uuid);
    const auto addStat = [&prefix, &collector](const auto& statKey,
                                               auto statValue) {
        fmt::memory_buffer key;
        format_to(key,
                  "{}:{}",
                  std::string_view{prefix.data(), prefix.size()},
                  statKey);
        collector.addStat(std::string_view(key.data(), key.size()), statValue);
    };

    addStat("vbuuid", vbUuid);
    addStat("start", cb::UserDataView(start.to_string()).getRawValue());
    addStat("end", cb::UserDataView(end.to_string()).getRawValue());
    addStat("key_value",
            keyOnly == cb::rangescan::KeyOnly::Yes ? "key" : "value");
    addStat("state", fmt::format("{}", state.load()));
    addStat("queued", queued);
    addStat("item_limit", itemLimit);
    addStat("item_count", itemCount);
    addStat("total_items", totalItems);
    addStat("time_limit", timeLimit.count());
}

void RangeScan::dump(std::ostream& os) const {
    fmt::print(os,
               "RangeScan: uuid:{}, {}, vbuuid:{}, range:({},{}), kv:{}, {}, "
               "queued:{}, itemCount:{}, itemLimit:{}, timeLimit:{}, "
               "deadline:{}\n",
               uuid,
               vbid,
               vbUuid,
               cb::UserDataView(start.to_string()),
               cb::UserDataView(end.to_string()),
               (keyOnly == cb::rangescan::KeyOnly::Yes ? "key" : "value"),
               state.load(),
               queued,
               itemCount,
               itemLimit,
               timeLimit.count(),
               scanContinueDeadline.time_since_epoch());
}

std::ostream& operator<<(std::ostream& os, const RangeScan::State& state) {
    switch (state) {
    case RangeScan::State::Idle:
        return os << "State::Idle";
    case RangeScan::State::Continuing:
        return os << "State::Continuing";
    case RangeScan::State::Cancelled:
        return os << "State::Cancelled";
    }
    throw std::runtime_error(
            fmt::format("RangeScan::State ostream operator<< invalid state:{}",
                        int(state)));
}

std::ostream& operator<<(std::ostream& os, const RangeScan& scan) {
    scan.dump(os);
    return os;
}