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

class EPVBucket;
class KVBucket;

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

    void scheduleScan(KVBucket& bucket,
                      EPVBucket& vb,
                      const DocKey& start,
                      const DocKey& end);

    std::shared_ptr<BackfillManager> rangeScanTasks;
};

namespace VB {
class RangeScans {
public:
    // all scans against the vbucket
    std::unordered_map<int /*RangeScanId*/, std::shared_ptr<RangeScanContext>>
            rangeScans;
};
} // namespace VB