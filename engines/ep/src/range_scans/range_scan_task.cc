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

#include "bucket_logger.h"
#include "collections/vbucket_manifest_handles.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan_context.h"
#include "vbucket.h"

class RangeScanCacheCallback : public StatusCallback<CacheLookup> {
public:
    RangeScanCacheCallback(KVBucket& bucket,
                           std::weak_ptr<RangeScanContext> context);
    void callback(CacheLookup& lookup) override;

private:
    GetValue get(VBucket& vb, CacheLookup& lookup, RangeScanContext& rsContext);
    KVBucket& bucket;
    std::weak_ptr<RangeScanContext> context;
};

/* Callback to get the items that are found to be in the disk */
class RangeScanDiskCallback : public StatusCallback<GetValue> {
public:
    RangeScanDiskCallback(std::weak_ptr<RangeScanContext> context);
    void callback(GetValue& gv) override;

private:
    std::weak_ptr<RangeScanContext> context;
};

RangeScanTask::RangeScanTask(Vbid vbid,
                             KVBucket& bucket,
                             const std::shared_ptr<RangeScanContext>& context,
                             const DocKey& start,
                             const DocKey& end)
    : DCPBackfill(vbid),
      DCPBackfillDisk(bucket),
      range(DiskDocKey{start}, DiskDocKey{end}),
      context(context) {
}

bool RangeScanTask::shouldCancel() const {
    return !context.lock();
}

backfill_status_t RangeScanTask::create() {
    auto locked = context.lock();
    if (!locked) {
        return backfill_finished;
    }

    const auto* kvstore = bucket.getROUnderlying(getVBucketId());

    // @todo: enable compressed return for snappy enabled clients
    ValueFilter valFilter = locked->isKeyOnly()
                                    ? ValueFilter::KEYS_ONLY
                                    : ValueFilter::VALUES_DECOMPRESSED;

    scanCtx = kvstore->initByIdScanContext(
            std::make_unique<RangeScanDiskCallback>(context),
            std::make_unique<RangeScanCacheCallback>(bucket, context),
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
    if (!context.lock()) {
        return backfill_finished;
    }

    const auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    switch (kvstore->scan(static_cast<ByIdScanContext&>(*scanCtx))) {
    case ScanStatus::Failed:
        EP_LOG_WARN("RangeScanTask::scan KVStore::scan failed {}",
                    getVBucketId());
    case ScanStatus::Cancelled:
        EP_LOG_INFO("RangeScanTask::scan KVStore::scan cancelled {}",
                    getVBucketId());
    case ScanStatus::Success:
        // @todo: get the fail/abort/success status into the end sentinel
        break;
    case ScanStatus::Yield:
        // Scan should run again (e.g. was paused by callback)
        return backfill_success;
    }

    auto locked = context.lock();
    if (locked) {
        // Store the sentinel value which indicates no more data
        locked->storeEndSentinel();
    } // else presume scan cancelled

    return backfill_finished;
}

RangeScanCacheCallback::RangeScanCacheCallback(
        KVBucket& bucket, std::weak_ptr<RangeScanContext> context)
    : bucket(bucket), context(context) {
}

GetValue RangeScanCacheCallback::get(VBucket& vb,
                                     CacheLookup& lookup,
                                     RangeScanContext& rsContext) {
    // getInternal may generate expired items and thus may for example need to
    // update a collection high-seqno, get a handle on the collection manifest
    auto cHandle = vb.lockCollections(lookup.getKey().getDocKey());
    if (!cHandle.valid()) {
        return GetValue{nullptr, cb::engine_errc::unknown_collection};
    }
    return vb.getInternal(nullptr,
                          bucket.getEPEngine(),
                          /*options*/ NONE,
                          VBucket::GetKeyOnly::No,
                          cHandle);
}

void RangeScanCacheCallback::callback(CacheLookup& lookup) {
    auto locked = context.lock();
    if (!locked) {
        // @todo correct status code
        setStatus(cb::engine_errc::stream_not_found);
        return;
    }

    // Check vbucket is valid and active, vb only needed for value scan, but
    // may as well check it for key only so we can stop if state changes
    VBucketPtr vb = bucket.getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(cb::engine_errc::not_my_vbucket);
        return;
    }
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        setStatus(cb::engine_errc::not_my_vbucket);
        return;
    }

    if (locked->isKeyOnly()) {
        // @todo consider memory reduction, the Item is mostly unused
        locked->store(std::make_unique<Item>(
                lookup.getKey().getDocKey(), 0, 0, nullptr, 0));
        setStatus(cb::engine_errc::key_already_exists);
        return;
    }

    auto gv = get(*vb, lookup, *locked);
    if (gv.getStatus() == cb::engine_errc::success &&
        gv.item->getBySeqno() == lookup.getBySeqno()) {
        if (locked->store(std::move(gv.item))) {
            setStatus(cb::engine_errc::key_already_exists);
        } else {
            // No space to store
            yield();
        }
    } else if (gv.getStatus() == cb::engine_errc::unknown_collection) {
        setStatus(cb::engine_errc::unknown_collection);
    } else {
        // Didn't find a matching value in-memory, continue to disk read
        setStatus(cb::engine_errc::success);
    }
}

RangeScanDiskCallback::RangeScanDiskCallback(
        std::weak_ptr<RangeScanContext> context)
    : context(context) {
}

void RangeScanDiskCallback::callback(GetValue& gv) {
    auto locked = context.lock();
    if (!locked) {
        setStatus(cb::engine_errc::success);
        return;
    }

    if (locked->store(std::move(gv.item))) {
        setStatus(cb::engine_errc::success);
    } else {
        yield();
    }
}
