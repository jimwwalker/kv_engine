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

#include "range_scans/range_scan_context.h"

#include "dcp/backfill-manager.h"
#include "item.h"
#include "range_scans/range_scan_result.h"

RangeScanContext::RangeScanContext(BackfillManager& bfManager)
    : bfManager(bfManager) {
}

bool RangeScanContext::store(std::unique_ptr<Item> item) {
    Expects(item);
    if (!bfManager.bytesCheckAndRead(item->size())) {
        return false;
    }

    queue.wlock()->emplace_back(
            std::make_unique<RangeScanResultValue>(std::move(item)));
    return true;
}

bool RangeScanContext::store(DocKey key) {
    if (!bfManager.bytesCheckAndRead(key.size())) {
        return false;
    }

    queue.wlock()->emplace_back(std::make_unique<RangeScanResultKey>(key));
    return true;
}

RangeScanContext::resultType RangeScanContext::popFront() {
    return queue.withWLock([](auto& queue) {
        auto result = std::move(queue.front());
        queue.pop_front();
        return result;
    });
}

void RangeScanContext::storeEndSentinel() {
    queue.wlock()->emplace_back(
            std::make_unique<RangeScanResultEnd>(cb::engine_errc::success));
}