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

#include "range_scans/range_scan_callbacks.h"
#include "range_scans/range_scan_types.h"
#include "storeddockey.h"

#include <executor/globaltask.h>
#include <memcached/vbucket.h>

class CookieIface;
class EPBucket;

/**
 * RangeScanCreateTask performs the I/O required as part of creating a range
 * scan
 */
class RangeScanCreateTask : public GlobalTask {
public:
    RangeScanCreateTask(
            EPBucket& bucket,
            Vbid vbid,
            const DocKey& start,
            const DocKey& end,
            RangeScanDataHandlerIFace& handler,
            const CookieIface* cookie,
            RangeScanKeyOnly keyOnly,
            std::optional<RangeScanSnapshotRequirements> snapshotReqs,
            std::unique_ptr<RangeScanCreateData> scanData);

    bool run() override;

    std::string getDescription() const override {
        return "RangeScanCreateTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::seconds(1);
    }

protected:
    /// @return status and uuid. The uuid is only valid is status is success
    std::pair<cb::engine_errc, RangeScanId> create() const;

    EPBucket& bucket;
    Vbid vbid;
    StoredDocKey start;
    StoredDocKey end;
    RangeScanDataHandlerIFace& handler;
    const CookieIface* cookie;
    RangeScanKeyOnly keyOnly;
    std::optional<RangeScanSnapshotRequirements> snapshotReqs;
    std::unique_ptr<RangeScanCreateData> scanData;
};