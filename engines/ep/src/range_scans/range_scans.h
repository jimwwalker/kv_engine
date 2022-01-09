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

#include "dcp/backfill-manager.h"
#include "dcp/backfill.h"
#include "range_scans/range_scan_context.h"

#include <memory>
#include <unordered_map>

class BackfillManager;
class EPVBucket;
class KVBucket;

using RangeScanID = uint32_t;

// class owns data and some logic for the buckets range scans
class RangeScans : public BackfillTrackingIface {
public:
    RangeScans(KVBucket& bucket);

    bool canAddBackfillToActiveQ() override;

    /**
     * Decrement by one the number of running (active/initializing/snoozing)
     * backfills. Does not include pending backfills.
     */
    void decrNumRunningBackfills() override;

    RangeScanID createAndSchedule(KVBucket& bucket,
                                  EPVBucket& vb,
                                  const DocKey& start,
                                  const DocKey& end);

private:
    std::shared_ptr<BackfillManager> bfm;
};

namespace VB {
class RangeScans {
public:
    /**
     * Generate a new scan ID and context
     * @return the scan ID so the context can be located
     */
    std::pair<RangeScanID, std::shared_ptr<RangeScanContext>>
    createRangeScanContext(BackfillManager& bfManager);

    bool exists(RangeScanID id) const;

    std::shared_ptr<RangeScanContext> getRangeScanContext(RangeScanID id) const;

private:
    RangeScanID nextScanID{0};

    folly::Synchronized<
            std::unordered_map<RangeScanID, std::shared_ptr<RangeScanContext>>>
            contexts;
};
} // namespace VB