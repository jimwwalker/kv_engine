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

#include "range_scans/range_scan_continue_task.h"

#include "ep_bucket.h"
#include "ep_engine.h"
#include "vbucket.h"

#include <phosphor/phosphor.h>

RangeScanContinueTask::RangeScanContinueTask(EPBucket& bucket)
    : GlobalTask(
              &bucket.getEPEngine(), TaskId::RangeScanContinueTask, 0, false),
      bucket(bucket) {
}

bool RangeScanContinueTask::run() {
    auto scan = bucket.takeNextRangeScan();

    // With more than one continue task scheduled, it's possible to run and find
    // the scans have all been taken/handled by other tasks
    if (scan) {
        TRACE_EVENT1("ep-engine/task",
                     "RangeScanContinueTask",
                     "vbid",
                     scan->getVBucketId().get());
        if (scan->isContinuing()) {
            continueScan(*scan);
        }
        // scan could be cancelled, in which case it will destruct here
    }

    // @todo: reschedule if more work exists, similar to compaction
    return false;
}

void RangeScanContinueTask::continueScan(RangeScan& scan) {
    auto status =
            scan.continueScan(*bucket.getRWUnderlying(scan.getVBucketId()));

    if (status == cb::engine_errc::success) {
        // Completed/Cancelled/Failed - all require the scan 'cancelling' so
        // it cannot be continued again
        auto vb = bucket.getVBucket(scan.getVBucketId());
        if (vb) {
            // Note this may return a failure. vbucket could of been removed
            // and re-added whilst scan was busy so has no knowledge
            vb->cancelRangeScan(scan.getUuid(), false /* no schedule*/);
        }
        return;
    }

    // This scan has been 'yielded' by a limit, set back to idle so it can be
    // continued again.
    // @todo: set this state before we could of told the client the status of
    // the continue - otherwise they could fire another continue that errors
    scan.setStateIdle();

    Expects(status == cb::engine_errc::too_busy);
}