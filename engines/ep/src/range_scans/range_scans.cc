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

#include "range_scans/range_scans.h"
#include "ep_vb.h"
#include "range_scans/range_scan_task.h"

RangeScans::RangeScans(KVBucket& bucket)
    : bfm(std::make_shared<BackfillManager>(
              bucket, *this, "RangeScans", ~0, ~0, ~0)) {
}

RangeScanID RangeScans::createAndSchedule(KVBucket& bucket,
                                          EPVBucket& vb,
                                          const DocKey& start,
                                          const DocKey& end) {
    auto scanContext = vb.createRangeScanContext(*bfm);
    bfm->schedule(std::make_unique<RangeScanTask>(
            vb.getId(), bucket, scanContext.second, start, end));
    return scanContext.first;
}

bool RangeScans::canAddBackfillToActiveQ() {
    return true;
}

void RangeScans::decrNumRunningBackfills() {
}

namespace VB {
std::pair<RangeScanID, std::shared_ptr<RangeScanContext>>
RangeScans::createRangeScanContext(BackfillManager& bfManager) {
    return contexts.withWLock([this, &bfManager](auto& contexts) {
        nextScanID++;
        auto [itr, emplaced] = contexts.try_emplace(
                nextScanID, std::make_shared<RangeScanContext>(bfManager));
        Expects(emplaced);
        return std::make_pair(nextScanID, itr->second);
    });
}

bool RangeScans::exists(RangeScanID id) const {
    return contexts.rlock()->count(id) > 0;
}

std::shared_ptr<RangeScanContext> RangeScans::getRangeScanContext(
        RangeScanID id) const {
    return contexts.withRLock([id](const auto& contexts) {
        Expects(contexts.count(id));
        return contexts.find(id)->second;
    });
}
} // end namespace VB