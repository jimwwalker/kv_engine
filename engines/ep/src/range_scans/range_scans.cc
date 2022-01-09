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
    : rangeScanTasks(
              std::make_shared<BackfillManager>(bucket, *this, ~0, ~0, ~0)) {
}

void RangeScans::scheduleScan(KVBucket& bucket,
                              EPVBucket& vb,
                              const DocKey& start,
                              const DocKey& end) {
    rangeScanTasks->schedule(std::make_unique<RangeScanTask>(
            vb.getId(), bucket, vb.createRangeScan(), start, end));
}

bool RangeScans::canAddBackfillToActiveQ() {
    return true;
}

void RangeScans::decrNumRunningBackfills() {
}