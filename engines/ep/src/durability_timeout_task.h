/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "ep_types.h"
#include "vb_visitors.h"
#include <executor/globaltask.h>
#include <platform/atomic_duration.h>

/*
 * Enforces the Durability Timeout for the SyncWrites tracked in this KVBucket.
 * Runs periodically (every durability_timeout_task_interval), and visits each
 * vBucket, calling processDurabilityTimeout() on each.
 *
 * This class is used when durability_timeout_mode == "polling". See also:
 * EventDrivenDurabilityTimeout.
 */
class DurabilityTimeoutTask : public GlobalTask {
public:
    class ConfigChangeListener;

    /**
     * @param engine The engine that will be visited
     */
    DurabilityTimeoutTask(EventuallyPersistentEngine& engine,
                          std::chrono::milliseconds interval);

    bool run() override;

    std::string getDescription() const override {
        return "DurabilityTimeoutTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // This tasks just spawns a new VBCBAdaptor, which is the actual tasks
        // that executes the DurabilityTimeoutVisitor. So, keeping the value
        // relatively high as there is no too much value in logging this timing.
        return std::chrono::seconds(1);
    }

    void setSleepTime(std::chrono::milliseconds value) {
        sleepTime = value;
    }

private:
    // Note: this is the actual minimum interval between subsequent runs.
    // The VBCBAdaptor (which is the actual task that executes this Visitor)
    // has its internal sleep-time which is used for a different purpose,
    // details in VBCBAdaptor.
    cb::AtomicDuration<std::chrono::milliseconds,
                       std::memory_order::memory_order_seq_cst>
            sleepTime;
};

/**
 * DurabilityTimeoutVisitor visits a VBucket for enforcing the Durability
 * Timeout for the SyncWrites tracked by VBucket.
 */
class DurabilityTimeoutVisitor : public CappedDurationVBucketVisitor {
public:
    DurabilityTimeoutVisitor() : startTime(std::chrono::steady_clock::now()) {
    }

    void visitBucket(VBucket& vb) override;

private:
    const std::chrono::steady_clock::time_point startTime;
};

class VBucketSyncWriteTimeoutTask : public GlobalTask {
public:
    VBucketSyncWriteTimeoutTask(Taskable& taskable, VBucket& vBucket);

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

protected:
    bool run() override;

private:
    VBucket& vBucket;
    // Need a separate vbid member variable as getDescription() can be
    // called during Bucket shutdown (after VBucket has been deleted)
    // as part of cleaning up tasks (see
    // EventuallyPersistentEngine::waitForTasks) - and hence calling
    // into vBucket->getId() would be accessing a deleted object.
    const Vbid vbid;
};
