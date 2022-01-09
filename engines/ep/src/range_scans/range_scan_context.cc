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

RangeScanContext::RangeScanContext(BackfillManager& bfManager)
    : bfManager(bfManager) {
}

bool RangeScanContext::store(std::unique_ptr<Item> item) {
    if (item && !bfManager.bytesCheckAndRead(item->size())) {
        return false;
    }

    queue.wlock()->emplace_back(std::move(item));
    return true;
}

void RangeScanContext::storeEndSentinel() {
    queue.wlock()->emplace_back(nullptr);
}