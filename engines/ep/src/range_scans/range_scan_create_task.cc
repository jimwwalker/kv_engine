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

#include "range_scans/range_scan_create_task.h"

#include "bucket_logger.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "range_scans/range_scan_types.h"

#include <phosphor/phosphor.h>

RangeScanCreateTask::RangeScanCreateTask(
        EPBucket& bucket,
        Vbid vbid,
        CollectionID cid,
        std::string_view start,
        std::string_view end,
        RangeScanDataHandlerIFace& handler,
        const CookieIface* cookie,
        cb::rangescan::KeyOnly keyOnly,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::unique_ptr<RangeScanCreateData> scanData)
    : GlobalTask(&bucket.getEPEngine(), TaskId::RangeScanCreateTask, 0, false),
      bucket(bucket),
      vbid(vbid),
      start(start, cid),
      end(end, cid),
      handler(handler),
      cookie(cookie),
      keyOnly(keyOnly),
      snapshotReqs(snapshotReqs),
      scanData(std::move(scanData)) {
}

bool RangeScanCreateTask::run() {
    TRACE_EVENT1("ep-engine/task", "RangeScanCreateTask", "vbid", vbid.get());

    auto status = cb::engine_errc::failed;
    try {
        std::tie(status, scanData->uuid) = create();
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "RangeScanCreateTask::run() failed to create RangeScan "
                "exception:{}",
                e.what());
    }

    // On success, release the scanData. The frontend thread will retrieve and
    // handle destruction and free
    if (status == cb::engine_errc::success) {
        scanData.release();
    }

    engine->notifyIOComplete(cookie, status);
    return false; // done, no reschedule required
}

std::pair<cb::engine_errc, cb::rangescan::Id> RangeScanCreateTask::create()
        const {
    auto vb = bucket.getVBucket(vbid);
    if (!vb) {
        return {cb::engine_errc::not_my_vbucket, {}};
    }
    // RangeScan constructor will throw if the snapshot cannot be opened or is
    // not usable for the scan
    auto scan = std::make_shared<RangeScan>(
            bucket, *vb, start, end, handler, cookie, keyOnly, snapshotReqs);
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    return {epVb.addNewRangeScan(scan), scan->getUuid()};
}