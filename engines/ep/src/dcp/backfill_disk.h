/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once
#include "callbacks.h"
#include "dcp/backfill.h"

#include <mutex>

class ActiveStream;
class KVBucket;
class VBucket;

/* The possible states of the DCPBackfillDisk */
enum backfill_state_t {
    backfill_state_init,
    backfill_state_scanning,
    backfill_state_completing,
    backfill_state_done
};

/* Callback to get the items that are found to be in the cache */
class CacheCallback : public StatusCallback<CacheLookup> {
public:
    CacheCallback(KVBucket& bucket, std::shared_ptr<ActiveStream> s);

    void callback(CacheLookup& lookup);

private:
    /**
     * Attempt to perform the get of lookup
     *
     * @return return a GetValue by performing a vb::get with lookup::getKey.
     */
    GetValue get(VBucket& vb, CacheLookup& lookup, ActiveStream& stream);

    KVBucket& bucket;
    std::weak_ptr<ActiveStream> streamPtr;
};

/* Callback to get the items that are found to be in the disk */
class DiskCallback : public StatusCallback<GetValue> {
public:
    DiskCallback(std::shared_ptr<ActiveStream> s);

    void callback(GetValue& val);

private:
    std::weak_ptr<ActiveStream> streamPtr;
};

class DCPBackfillDisk : public virtual DCPBackfill {
public:
    DCPBackfillDisk(KVBucket& bucket, std::shared_ptr<ActiveStream> s);

    ~DCPBackfillDisk();

protected:
    backfill_status_t run() override;
    void cancel() override;
    void transitionState(backfill_state_t newState);

    /**
     * Creates a scan context with the KV Store to read items in the sequential
     * order from the disk. Backfill snapshot range is decided here.
     */
    virtual backfill_status_t create() = 0;

    /**
     * Scan the disk (by calling KVStore apis) for the items in the backfill
     * snapshot range created in the create scan context. This is an
     * asynchronous operation, KVStore calls the CacheCallback and DiskCallback
     * to populate the items read in the snapshot of scan.
     */
    virtual backfill_status_t scan() = 0;

    /**
     * Handles the completion of the backfill.
     * Destroys the scan context, indicates the completion to the stream.
     *
     * @param cancelled indicates the if backfill finished fully or was
     *                  cancelled in between; for debug
     */
    virtual backfill_status_t complete(bool cancelled) = 0;

    std::mutex lock;
    backfill_state_t state = backfill_state_init;

    KVBucket& bucket;

    // Order is important, the scanCtx (if created) will reference the callbacks
    // so the scanCtx should destruct before each callback
    std::unique_ptr<ScanContext> scanCtx;
    std::unique_ptr<DiskCallback> diskCallback;
    std::unique_ptr<CacheCallback> cacheCallback;
};
