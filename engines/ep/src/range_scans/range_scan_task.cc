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

#include "range_scans/range_scan_task.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"

class RangeScanCacheCallback : public StatusCallback<CacheLookup> {
public:
    RangeScanCacheCallback(KVBucket& bucket);

    void callback(CacheLookup& lookup) override;

    KVBucket& bucket;
};

/* Callback to get the items that are found to be in the disk */
class RangeScanDiskCallback : public StatusCallback<GetValue> {
public:
    void callback(GetValue& val) override;
};

RangeScanTask::RangeScanTask(Vbid vbid,
                             KVBucket& bucket,
                             const DocKey& start,
                             const DocKey& end)
    : DCPBackfill(vbid),
      DCPBackfillDisk(bucket),
      range(DiskDocKey{start}, DiskDocKey{end}) {
}

bool RangeScanTask::shouldCancel() const {
    return false;
}

backfill_status_t RangeScanTask::create() {
    const auto* kvstore = bucket.getROUnderlying(getVBucketId());

    // @todo: configure based on clients request
    auto valFilter = ValueFilter::VALUES_DECOMPRESSED;

    scanCtx = kvstore->initByIdScanContext(
            std::make_unique<RangeScanDiskCallback>(),
            std::make_unique<RangeScanCacheCallback>(bucket),
            getVBucketId(),
            {range},
            DocumentFilter::NO_DELETES,
            valFilter);

    // @todo notifyIOComplete so that the create command can return status
    if (!scanCtx) {
        auto vb = bucket.getVBucket(getVBucketId());
        std::stringstream log;
        log << "RangeScanTask::create(): (" << getVBucketId()
            << ") initByIdScanContext failed ";
        if (vb) {
            log << VBucket::toString(vb->getState());
        } else {
            log << "vb not found!!";
        }

        EP_LOG_WARN("{}", log.str());
        return backfill_finished;
    }

    return backfill_success;
}

backfill_status_t RangeScanTask::scan() {
    // @todo: cancelled and disconnected check
    const auto* kvstore = bucket.getROUnderlying(getVBucketId());

    scan_error_t error = kvstore->scan(static_cast<ByIdScanContext&>(*scanCtx));

    if (error == scan_again) {
        return backfill_success;
    }

    return backfill_finished;
}

RangeScanCacheCallback::RangeScanCacheCallback(KVBucket& bucket)
    : bucket(bucket) {
}

void RangeScanCacheCallback::callback(CacheLookup& lookup) {
}

void RangeScanDiskCallback::callback(GetValue& val) {
}
