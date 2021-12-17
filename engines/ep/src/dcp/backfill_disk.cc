/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill_disk.h"
#include "active_stream.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"

CacheCallback::CacheCallback(KVBucket& bucket, std::shared_ptr<ActiveStream> s)
    : bucket(bucket), streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("CacheCallback(): stream is NULL");
    }
}

// Do a get and restrict the collections lock scope to just these checks.
GetValue CacheCallback::get(VBucket& vb,
                            CacheLookup& lookup,
                            ActiveStream& stream) {
    // getInternal may generate expired items and thus may for example need to
    // update a collection high-seqno, get a handle on the collection manifest
    auto cHandle = vb.lockCollections(lookup.getKey().getDocKey());
    if (!cHandle.valid()) {
        return {};
    }
    return vb.getInternal(nullptr,
                          bucket.getEPEngine(),
                          /*options*/ NONE,
                          stream.isKeyOnly() ? VBucket::GetKeyOnly::Yes
                                             : VBucket::GetKeyOnly::No,
                          cHandle);
}

void CacheCallback::callback(CacheLookup& lookup) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(cb::engine_errc::success);
        return;
    }

    VBucketPtr vb = bucket.getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(cb::engine_errc::success);
        return;
    }

    // If diskKey is in Prepared namespace then the in-memory StoredValue isn't
    // sufficient - as it doesn't contain the durability requirements (level).
    // Must get from disk.
    if (lookup.getKey().isPrepared()) {
        setStatus(cb::engine_errc::success);
        return;
    }

    // Check if the stream will allow the key, this is here to avoid reading
    // the value when dropping keys
    if (!stream_->collectionAllowed(lookup.getKey().getDocKey())) {
        setStatus(cb::engine_errc::key_already_exists);
        return;
    }

    auto gv = get(*vb, lookup, *stream_);
    if (gv.getStatus() == cb::engine_errc::success) {
        if (gv.item->getBySeqno() == lookup.getBySeqno()) {
            if (stream_->backfillReceived(std::move(gv.item),
                                          BACKFILL_FROM_MEMORY)) {
                setStatus(cb::engine_errc::key_already_exists);
                return;
            }
            setStatus(cb::engine_errc::no_memory); // Pause the backfill
            return;
        }
    }
    setStatus(cb::engine_errc::success);
}

DiskCallback::DiskCallback(std::shared_ptr<ActiveStream> s) : streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("DiskCallback(): stream is NULL");
    }
}

void DiskCallback::callback(GetValue& val) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(cb::engine_errc::success);
        return;
    }

    if (!val.item) {
        throw std::invalid_argument("DiskCallback::callback: val is NULL");
    }

    // MB-26705: Make the backfilled item cold so ideally the consumer would
    // evict this before any cached item if they get into memory pressure.
    val.item->setFreqCounterValue(0);

    if (!stream_->backfillReceived(std::move(val.item), BACKFILL_FROM_DISK)) {
        setStatus(cb::engine_errc::no_memory); // Pause the backfill
    } else {
        setStatus(cb::engine_errc::success);
    }
}

DCPBackfillDisk::DCPBackfillDisk(KVBucket& bucket) : bucket(bucket) {
}

DCPBackfillDisk::~DCPBackfillDisk() = default;

backfill_status_t DCPBackfillDisk::run() {
    std::lock_guard<std::mutex> lh(lock);
    auto runtimeGuard =
            folly::makeGuard([start = std::chrono::steady_clock::now(), this] {
                runtime += (std::chrono::steady_clock::now() - start);
            });
    switch (state) {
    case State::create:
        return create();
    case State::scan:
        return scan();
    case State::complete:
        complete();
        return backfill_finished;
    case State::done:
        return backfill_finished;
    }

    throw std::logic_error("DCPBackfillDisk::run: Invalid State " +
                           std::to_string(int(state)));
}

void DCPBackfillDisk::cancel() {
    std::lock_guard<std::mutex> lh(lock);
    if (state != State::done) {
        EP_LOG_WARN(
                "DCPBackfillDisk::cancel ({}) cancelled before reaching "
                "State::done",
                getVBucketId());
    }
}

std::string DCPBackfillDisk::to_string(State state) {
    switch (state) {
    case State::create:
        return "create";
    case State::scan:
        return "scan";
    case State::complete:
        return "complete";
    case State::done:
        return "done";
    }
    throw std::logic_error("DCPBackfillDisk::to_string: Invalid State " +
                           std::to_string(int(state)));
}

void DCPBackfillDisk::transitionState(State newState) {
    if (state == newState) {
        return;
    }

    bool validTransition = false;
    switch (newState) {
    case State::create:
        // No valid transition to 'create'
        break;
    case State::scan:
        if (state == State::create) {
            validTransition = true;
        }
        break;
    case State::complete:
        if (state == State::create || state == State::scan) {
            validTransition = true;
        }
        break;
    case State::done:
        if (state == State::create || state == State::scan ||
            state == State::complete) {
            validTransition = true;
        }
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "DCPBackfillDisk::transitionState:"
                " newState (which is " +
                to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state) + ")");
    }

    state = newState;
}
