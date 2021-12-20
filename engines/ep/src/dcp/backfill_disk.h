/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once
#include "callbacks.h"
#include "dcp/backfill.h"

#include <chrono>
#include <mutex>

class ActiveStream;
class KVBucket;
class VBucket;
enum class ValueFilter;

/* Callback to get the items that are found to be in the cache */
class CacheCallback : public StatusCallback<CacheLookup> {
public:
    CacheCallback(KVBucket& bucket, std::shared_ptr<ActiveStream> s);

    void callback(CacheLookup& lookup) override;

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
    explicit DiskCallback(std::shared_ptr<ActiveStream> s);

    void callback(GetValue& val) override;

private:
    std::weak_ptr<ActiveStream> streamPtr;
};

class DCPBackfillDisk : public virtual DCPBackfill {
public:
    explicit DCPBackfillDisk(KVBucket& bucket);

    ~DCPBackfillDisk() override;

protected:
    /**
     * States for the backfill, these reflect which create/scan/done
     * function is invoked when the backfill is told to run.
     *
     * All transitions are from "left to right", no state can go back.
     *
     * Valid transitions are:
     *
     * create->scan->done
     * create->done
     */
    enum class State { create, scan, done };

    backfill_status_t run() override;
    void cancel() override;
    void transitionState(State newState);

    static std::string to_string(State state);

    /**
     * Create the scan, e.g. open a disk snapshot and set the ScanContext
     * @return success if everything opened correctly or snooze if the task
     *         should delay before the next run.
     */
    virtual backfill_status_t create() = 0;

    /**
     * Run the scan, reading keys/values from disk and copying them to an end
     * point, e.g. an ActiveStream
     * @return success if the scan should be invoked again or finished when
     *         complete (scan doesn't have to be 100% done to finish).
     */
    virtual backfill_status_t scan() = 0;

    std::mutex lock;
    State state{State::create};

    KVBucket& bucket;

    std::unique_ptr<ScanContext> scanCtx;
};
