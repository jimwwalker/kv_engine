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

#pragma once

#include "callbacks.h"
#include "range_scans/range_scan_types.h"
#include "storeddockey.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>

class ByIdScanContext;
class CookieIface;
class EPBucket;
class KVStoreIface;
class RangeScanDataHandlerIFace;
class StatCollector;
class VBucket;

/**
 * RangeScan class is the object create for each range-scan (created as part
 * of the range-scan-create command)
 */
class RangeScan {
public:
    /**
     * Create a RangeScan object for the given vbucket and with the given
     * callbacks. The constructed RangeScan will open the underlying KVStore.
     *
     * @throws std::runtime_error with details as to why construction failed
     * @param bucket The EPBucket to use to obtain the KVStore and pass to the
     *               RangeScanCacheCallback
     * @param vbucket vbucket to scan
     * @param start range start (inclusive)
     * @param end range end (inclusive)
     * @param handler key/item handler to process key/items of the scan
     * @param cookie connection cookie creating the RangeScan
     * @param keyOnly configure key or value scan
     */
    RangeScan(EPBucket& bucket,
              const VBucket& vbucket,
              const DocKey& start,
              const DocKey& end,
              RangeScanDataHandlerIFace& handler,
              const CookieIface* cookie,
              RangeScanKeyOnly keyOnly);

    /**
     * Continue the range scan by calling kvstore.scan()
     *
     * @param kvstore A KVStoreIface on which to call scan
     * @return success or too_busy
     */
    cb::engine_errc continueScan(KVStoreIface& kvstore);

    /// @return the universally unique id of this scan (exposed to the client)
    RangeScanId getUuid() const {
        return uuid;
    }

    /// @return true if the scan is currently idle
    bool isIdle() const {
        return state.load() == State::Idle;
    }

    /// @return true if the scan is currently continuing
    bool isContinuing() const {
        return state.load() == State::Continuing;
    }

    /// @return true if the scan is cancelled
    bool isCancelled() const {
        return state.load() == State::Cancelled;
    }

    /// change the state of the scan to Idle
    void setStateIdle();

    /**
     * Change the state of the scan to Continuing and set the limit for the
     * scan. A value of 0 means no limit
     * @param itemLimit how many items the scan can return
     * @param timeLimit how long the scan can run for (0 no limit)
     */
    void setStateContinuing(size_t itemLimit,
                            std::chrono::milliseconds timeLimit);

    /// change the state of the scan to Cancelled
    void setStateCancelled();

    /// @return the vbucket ID owning this scan
    Vbid getVBucketId() const {
        return vbid;
    }

    /// @return cookie associated with the current stage of the scan
    const CookieIface* getCookie() const {
        return cookie;
    }

    /// @return true if the scan is configured for keys only
    bool isKeyOnly() const {
        return keyOnly == RangeScanKeyOnly::Yes;
    }

    /// method for use by RangeScans to ensure we only queue a scan once
    bool isQueued() const {
        return queued;
    }
    /// method for use by RangeScans to ensure we only queue a scan once
    void setQueued(bool q) {
        queued = q;
    }

    /// Increment the scan's itemCount for an item read by the scan
    void incrementItemCount();

    /// @return true if limits have been reached
    bool areLimitsExceeded();

    /// Generate stats for this scan
    void addStats(const StatCollector& collector) const;

    /// dump the object to the ostream (default of cerr)
    void dump(std::ostream& os = std::cerr) const;
    /**
     * To facilitate testing, the now function, which returns a time point can
     * be replaced
     */
    static void setClockFunction(
            std::function<std::chrono::steady_clock::time_point()> func) {
        now = func;
    }

protected:
    /**
     * Function to create a scan by opening the underlying disk snapshot. The
     * KVStore is located from the EPBucket and the snapshot obtained by calling
     * initByIdScanContext. The EPBucket is also passed to the
     * RangeScanCacheCallback for use when the scan loads items.
     *
     * On failure throws a std::runtime_error
     *
     * @param bucket The EPBucket to use to obtain the KVStore and pass to the
     *               RangeScanCacheCallback
     * @return the RangeScanId to use for this scan
     */
    RangeScanId createScan(EPBucket& bucket);

    // member variables ordered by size large -> small
    RangeScanId uuid;
    StoredDocKey start;
    StoredDocKey end;
    // uuid of the vbucket to assist detection of a vbucket state change
    uint64_t vbUuid;
    std::unique_ptr<ByIdScanContext> scanCtx;
    RangeScanDataHandlerIFace& handler;
    const CookieIface* cookie;

    /// current limit for the continuation of this scan
    size_t itemLimit{0};
    /// current item count for the continuation of this scan
    size_t itemCount{0};
    /// item count for the life of this scan
    size_t totalItems{0};
    /// current time limit for the continuation of this scan
    std::chrono::milliseconds timeLimit{0};
    std::chrono::steady_clock::time_point scanContinueDeadline;
    Vbid vbid;

    /**
     * RangeScan has a state with the following legal transitions. Also
     * shown is the operation which makes that transition.
     *
     * Idle->Continuing  (via range-scan-continue)
     * Idle->Cancelled (via range-scan-cancel)
     * Continuing->Idle (via I/O task after a successful continue)
     * Continuing->Cancelled (via range-scan-cancel)
     */
    enum class State : char { Idle, Continuing, Cancelled };
    std::atomic<State> state;
    RangeScanKeyOnly keyOnly{RangeScanKeyOnly::No};
    /// is this scan in the run queue? This bool is read/written only by
    /// RangeScans under the queue lock
    bool queued{false};

    // To facilitate testing, the clock can be replaced with something else
    // this is static as there's no need for replacement on a scan by scan basis
    static std::function<std::chrono::steady_clock::time_point()> now;

    friend std::ostream& operator<<(std::ostream&, const RangeScan::State&);
    friend std::ostream& operator<<(std::ostream&, const RangeScan&);
};

std::ostream& operator<<(std::ostream&, const RangeScan::State&);
std::ostream& operator<<(std::ostream&, const RangeScan&);
