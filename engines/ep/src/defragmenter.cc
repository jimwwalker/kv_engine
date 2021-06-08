/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "defragmenter.h"

#include "bucket_logger.h"
#include "defragmenter_visitor.h"
#include "ep_engine.h"
#include "executorpool.h"
#include "kv_bucket.h"
#include "stored-value.h"
#include <phosphor/phosphor.h>
#include <platform/cb_arena_malloc.h>
#include <cinttypes>

DefragmenterTask::DefragmenterTask(EventuallyPersistentEngine* e,
                                   EPStats& stats_)
    : GlobalTask(e, TaskId::DefragmenterTask, 0, false),
      stats(stats_),
      epstore_position(engine->getKVBucket()->startPosition()),
      pid(engine->getConfiguration().getDefragmenterAutoTargetPerc(),
          0.1,
          0.0000001,
          1.0,
          std::chrono::milliseconds{10000}) {
}

bool DefragmenterTask::run() {
    TRACE_EVENT0("ep-engine/task", "DefragmenterTask");
    if (engine->getConfiguration().isDefragmenterEnabled()) {
        defrag();
    }
    snooze(getSleepTime().count());
    if (engine->getEpStats().isShutdown) {
        return false;
    }
    return true;
}

void DefragmenterTask::defrag() {
    // Get our pause/resume visitor. If we didn't finish the previous pass,
    // then resume from where we last were, otherwise create a new visitor
    // starting from the beginning.
    if (!prAdapter) {
        auto visitor = std::make_unique<DefragmentVisitor>(getMaxValueSize());

        prAdapter = std::make_unique<PauseResumeVBAdapter>(std::move(visitor));
        epstore_position = engine->getKVBucket()->startPosition();
    }

    // Print start status.
    if (getGlobalBucketLogger()->should_log(spdlog::level::info)) {
        std::stringstream ss;
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (epstore_position == engine->getKVBucket()->startPosition()) {
            ss << " starting. ";
        } else {
            ss << " resuming from " << epstore_position << ", ";
            ss << prAdapter->getHashtablePosition() << ".";
        }
        auto fragStats = cb::ArenaMalloc::getFragmentationStats(
                engine->getArenaMallocClient());
        ss << " Using chunk_duration=" << getChunkDuration().count() << " ms."
           << " mem_used=" << stats.getEstimatedTotalMemoryUsed() << ", "
           << fragStats;
        EP_LOG_INFO("{}", ss.str());
    }

    // Disable thread-caching (as we are about to defragment, and hence don't
    // want any of the new Blobs in tcache).
    cb::ArenaMalloc::switchToClient(engine->getArenaMallocClient(),
                                    false /* no tcache*/);

    // Prepare the underlying visitor.
    auto& visitor = getDefragVisitor();
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + getChunkDuration();
    visitor.setDeadline(deadline);
    visitor.setBlobAgeThreshold(getAgeThreshold());
    // Only defragment StoredValues of persistent buckets because the
    // HashTable defrag method doesn't yet know how to maintain the
    // ephemeral seqno linked-list
    if (engine->getConfiguration().getBucketType() == "persistent") {
        visitor.setStoredValueAgeThreshold(getStoredValueAgeThreshold());
    }
    visitor.clearStats();

    // Do it - set off the visitor.
    epstore_position = engine->getKVBucket()->pauseResumeVisit(
            *prAdapter, epstore_position);
    const auto end = std::chrono::steady_clock::now();

    // Defrag complete. Restore thread caching.
    cb::ArenaMalloc::switchToClient(engine->getArenaMallocClient(),
                                    true /* tcache*/);

    updateStats(visitor);

    // Release any free memory we now have in the allocator back to the OS.
    // TODO: Benchmark this - is it necessary? How much of a slowdown does it
    // add? How much memory does it return?
    cb::ArenaMalloc::releaseMemory(engine->getArenaMallocClient());

    // Check if the visitor completed a full pass.
    bool completed = (epstore_position == engine->getKVBucket()->endPosition());

    // Print status.
    if (getGlobalBucketLogger()->should_log(spdlog::level::info)) {
        std::stringstream ss;
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (completed) {
            ss << " finished.";
        } else {
            ss << " paused at position " << epstore_position << ".";
        }
        std::chrono::microseconds duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                      start);
        auto fragStats = cb::ArenaMalloc::getFragmentationStats(
                engine->getArenaMallocClient());
        ss << " Took " << duration.count() << " us."
           << " moved " << visitor.getDefragCount() << "/"
           << visitor.getVisitedCount() << " visited documents."
           << " mem_used=" << stats.getEstimatedTotalMemoryUsed() << ", "
           << fragStats << ". Sleeping for " << getSleepTime().count()
           << " seconds.";
        EP_LOG_INFO("{}", ss.str());
    }

    // Delete(reset) visitor if it finished.
    if (completed) {
        prAdapter.reset();
    }
}

void DefragmenterTask::stop() {
    if (uid) {
        ExecutorPool::get()->cancel(uid);
    }
}

std::string DefragmenterTask::getDescription() const {
    return "Memory defragmenter";
}

std::chrono::microseconds DefragmenterTask::maxExpectedDuration() const {
    // Defragmenter processes items in chunks, with each chunk constrained
    // by a ChunkDuration runtime, so we expect to only take that long.
    // However, the ProgressTracker used estimates the time remaining, so
    // apply some headroom to that figure so we don't get inundated with
    // spurious "slow tasks" which only just exceed the limit.
    return getChunkDuration() * 10;
}

std::chrono::duration<double> DefragmenterTask::getSleepTime() {
    if (engine->getConfiguration().isDefragmenterAuto()) {
        return calculateSleepDuration();
    }
    return std::chrono::duration<double>{
            engine->getConfiguration().getDefragmenterInterval()};
}

size_t DefragmenterTask::getAgeThreshold() const {
    return engine->getConfiguration().getDefragmenterAgeThreshold();
}

size_t DefragmenterTask::getStoredValueAgeThreshold() const {
    return engine->getConfiguration().getDefragmenterStoredValueAgeThreshold();
}

void DefragmenterTask::updateStats(DefragmentVisitor& visitor) {
    stats.defragNumMoved.fetch_add(visitor.getDefragCount());
    stats.defragStoredValueNumMoved.fetch_add(
            visitor.getStoredValueDefragCount());
    stats.defragNumVisited.fetch_add(visitor.getVisitedCount());
}

size_t DefragmenterTask::getMaxValueSize() {
    size_t nbins{0};
    cb::ArenaMalloc::getProperty("arenas.nbins", nbins);

    char buff[20];
    snprintf(buff,
             sizeof(buff),
             "arenas.bin.%" PRIu64 ".size",
             static_cast<uint64_t>(nbins) - 1);

    size_t largest_bin_size;
    cb::ArenaMalloc::getProperty(buff, largest_bin_size);

    return largest_bin_size;
}

std::chrono::milliseconds DefragmenterTask::getChunkDuration() const {
    return std::chrono::milliseconds(
            engine->getConfiguration().getDefragmenterChunkDuration());
}

DefragmentVisitor& DefragmenterTask::getDefragVisitor() {
    return dynamic_cast<DefragmentVisitor&>(prAdapter->getHTVisitor());
}

std::chrono::duration<double> DefragmenterTask::calculateSleepDuration() {
    auto fragStats = cb::ArenaMalloc::getFragmentationStats(
            engine->getArenaMallocClient());

    auto perc = fragStats.getFragmentationPercD();

    // If fragmentation goes below our set-point (SP), we can't continue to use
    // the PID. More general usage and it would be used to "speed up/slow down"
    // to reach the SP. We can't now force defragmentation up, we're just happy
    // it's below the SP. In this case reset and when we go over again begin
    // the ramping
    if (perc < engine->getConfiguration().getDefragmenterAutoTargetPerc()) {
        pid.reset();
        EP_LOG_INFO("Skipping returning max duration good frag:{}", perc);
        return std::chrono::duration<double>{
                engine->getConfiguration().getDefragmenterAutoMaxSleep()};
    }

    // Above setpoint, calculate a correction
    // The pid controller in this context will return negative values, so we
    // want a positive output for the next step.
    auto correction = 0.0 - pid.step<>(perc);

    // Now map correction to duration as the pid is tracking error based on
    // the difference from defragmenter_auto_target_perc. The
    // output of this function is also capped by:
    //    - defragmenter_auto_max_chunk_duration
    //    - defragmenter_auto_min_chunk_duration

    // Scale??
    // as it gets more magnitude we want the sleep time
    // to get smaller and cap at min/max
    // scale the correction and subtract from max
    auto scaled = correction; //* 0.1;
    auto final1 =
            engine->getConfiguration().getDefragmenterAutoMaxSleep() - scaled;
    auto final = final1;

    if (final1 < engine->getConfiguration().getDefragmenterAutoMinSleep()) {
        final = engine->getConfiguration().getDefragmenterAutoMinSleep();
    }
    EP_LOG_INFO(
            "calculateSleepDuration frag:{}, correction:{} scaled:{}, "
            "final1:{}, final:{}",
            perc,
            correction,
            scaled,
            final1,
            final);
    return std::chrono::duration<double>{final};
}
