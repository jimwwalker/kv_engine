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

#include "seqno_persistence_notify_task.h"

#include "ep_bucket.h"
#include "ep_engine.h"
#include "vbucket.h"

#include <executor/executorpool.h>

/**
 * Low cadence task which will only run whilst SeqnoPersistence requests exist.
 * Either this task or the flusher will notify/expire requests. When there are
 * no writes, the flusher snoozes permanently allowing this task to take over
 * expiry responsibility
 */
SeqnoPersistenceNotifyTask::SeqnoPersistenceNotifyTask(KVBucket& bucket)
    : GlobalTask(&bucket.getEPEngine(),
                 TaskId::SeqnoPersistenceNotifyTask,
                 INT_MAX,
                 false),
      bucket(bucket),
      vbuckets(bucket.getVBuckets().getSize()) {
}

bool SeqnoPersistenceNotifyTask::run() {
    processVbuckets();
    // Schedule again if not shutting down
    return !engine->getEpStats().isShutdown;
}

void SeqnoPersistenceNotifyTask::addVbucket(
        Vbid vbid, std::chrono::steady_clock::time_point deadline) {
    vbuckets.pushUnique(vbid);

    // Cannot have concurrent addVbucket figuring out the snooze time
    // T1 tries to set the snooze to 1 second
    // T2 tries to set the snooze to 2 second
    // We require the 1 second snooze, T2 could interleave and set the 2 second
    // value. So here we serialise the read/write of waketime
    auto now = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(adjustWakeUp);
    auto wakeTime = getWaketime();

    // Change waketime if this new request has a smaller deadline
    if (deadline < wakeTime) {
        updateWaketime(deadline);
        auto snoozeTime = std::chrono::duration<double>(deadline - now);
        // now() could be > than deadline
        ExecutorPool::get()->snoozeAndWait(getId(),
                                           std::max(snoozeTime.count(), 0.0));
    }
}

void SeqnoPersistenceNotifyTask::processVbuckets() {
    snooze(INT_MAX);

    if (vbuckets.empty()) {
        // A vb may have now entered the queue, but they will undo the INT_MAX
        // sleep
        return;
    }

    // Attempt to only process those requests which currently queued for this
    // run of the task
    const size_t iterations = vbuckets.size();
    for (size_t iteration = 0; iteration < iterations; iteration++) {
        Vbid vbid;
        vbuckets.popFront(vbid);

        auto vb = bucket.getVBucket(vbid);
        if (!vb) {
            continue;
        }

        auto deadline = vb->notifyHighPriorityRequests(
                *engine, vb->getPersistenceSeqno());
        if (deadline) {
            // If a deadline is returned, vbuckets still exist with outstanding
            // requests. The deadline returned is the next deadline from this
            // vbucket
            addVbucket(vbid, deadline.value());
        }
    }
}
