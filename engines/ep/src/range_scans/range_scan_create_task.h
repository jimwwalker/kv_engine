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

#include "storeddockey.h"

#include <executor/globaltask.h>
#include <memcached/engine_error.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <memcached/range_scan_optional_configuration.h>
#include <memcached/vbucket.h>

class CookieIface;
class EPBucket;
struct RangeScanCreateData;
class RangeScanDataHandlerIFace;

/**
 * RangeScanCreateTask performs the I/O required as part of creating a range
 * scan
 */
class RangeScanCreateTask : public GlobalTask {
public:
    RangeScanCreateTask(
            EPBucket& bucket,
            Vbid vbid,
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            RangeScanDataHandlerIFace& handler,
            const CookieIface& cookie,
            cb::rangescan::KeyOnly keyOnly,
            std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig,
            std::unique_ptr<RangeScanCreateData> scanData);

    bool run() override;

    std::string getDescription() const override {
        return "RangeScanCreateTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::seconds(1);
    }

    static StoredDocKey makeStoredDocKey(CollectionID cid,
                                         cb::rangescan::KeyView key);

protected:
    /// @return status and uuid. The uuid is only valid is status is success
    std::pair<cb::engine_errc, cb::rangescan::Id> create() const;

    EPBucket& bucket;
    Vbid vbid;
    StoredDocKey start;
    StoredDocKey end;
    RangeScanDataHandlerIFace& handler;
    const CookieIface& cookie;
    cb::rangescan::KeyOnly keyOnly;
    std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs;
    std::optional<cb::rangescan::SamplingConfiguration> samplingConfig;
    std::unique_ptr<RangeScanCreateData> scanData;
};