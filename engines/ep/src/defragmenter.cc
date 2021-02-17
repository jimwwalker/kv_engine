/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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
      chunkDuration(getChunkDuration()),
      sleepTime(getSleepTime()) {
}

static auto getFragmentation(const EventuallyPersistentEngine* engine) {
    return cb::ArenaMalloc::getFragmentationStats(
            engine->getArenaMallocClient());
}

std::optional<cb::FragmentationStats> DefragmenterTask::needToDefrag() const {
    if (!engine->getConfiguration().isDefragmenterEnabled()) {
        return std::nullopt;
    }

    // Check with threshold and return the stats if defrag is needed (so we
    // avoid triggering a jemalloc sync unnecessarily)
    auto stats = getFragmentation(engine);
    if (stats.getFragmentationPerc() <
        engine->getConfiguration().getDefragmenterThreshold()) {
        return std::nullopt;
    }
    return stats;
}

bool DefragmenterTask::run() {
    TRACE_EVENT0("ep-engine/task", "DefragmenterTask");
    auto fragmentation = needToDefrag();
    if (fragmentation.has_value()) {
        // Get our pause/resume visitor. If we didn't finish the previous pass,
        // then resume from where we last were, otherwise create a new visitor
        // starting from the beginning.
        if (!prAdapter) {
            auto visitor =
                    std::make_unique<DefragmentVisitor>(getMaxValueSize());

            prAdapter =
                    std::make_unique<PauseResumeVBAdapter>(std::move(visitor));
            epstore_position = engine->getKVBucket()->startPosition();
        }

        // Print start status.
        if (globalBucketLogger->should_log(spdlog::level::debug)) {
            std::stringstream ss;
            ss << getDescription() << " for bucket '" << engine->getName()
               << "'";
            if (epstore_position == engine->getKVBucket()->startPosition()) {
                ss << " starting. ";
            } else {
                ss << " resuming from " << epstore_position << ", ";
                ss << prAdapter->getHashtablePosition() << ".";
            }
            ss << " Using chunk_duration=" << getChunkDuration().count()
               << " ms."
               << " mem_used=" << stats.getEstimatedTotalMemoryUsed() << ", "
               << fragmentation.value();
            EP_LOG_DEBUG("{}", ss.str());
        }

        // Disable thread-caching (as we are about to defragment, and hence don't
        // want any of the new Blobs in tcache).
        cb::ArenaMalloc::switchToClient(engine->getArenaMallocClient(),
                                        false /* no tcache*/);

        auto preFragmentation = cb::ArenaMalloc::getFragmentationStats(
                engine->getArenaMallocClient());

        // Prepare the underlying visitor.
        auto& visitor = getDefragVisitor();
        const auto start = std::chrono::steady_clock::now();

        const auto deadline = start + chunkDuration;
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

        auto postFragmentation = cb::ArenaMalloc::getFragmentationStats(
                engine->getArenaMallocClient());

        // Defrag complete. Restore thread caching.
        cb::ArenaMalloc::switchToClient(engine->getArenaMallocClient(),
                                        true /* tcache*/);

        updateStats(visitor);

        // Release any free memory we now have in the allocator back to the OS.
        // TODO: Benchmark this - is it necessary? How much of a slowdown does it
        // add? How much memory does it return?
        cb::ArenaMalloc::releaseMemory(engine->getArenaMallocClient());

        // Check if the visitor completed a full pass.
        bool completed = (epstore_position ==
                                    engine->getKVBucket()->endPosition());

        // Print status.
        if (globalBucketLogger->should_log(spdlog::level::debug)) {
            std::stringstream ss;
            ss << getDescription() << " for bucket '" << engine->getName()
               << "'";
            if (completed) {
                ss << " finished.";
            } else {
                ss << " paused at position " << epstore_position << ".";
            }
            std::chrono::microseconds duration =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            end - start);
            auto fragStats = cb::ArenaMalloc::getFragmentationStats(
                    engine->getArenaMallocClient());
            ss << " Took " << duration.count() << " us."
               << " planned duration " << chunkDuration.count() << " us."
               << " moved " << visitor.getDefragCount() << "/"
               << visitor.getVisitedCount() << " visited documents."
               << " mem_used=" << stats.getEstimatedTotalMemoryUsed() << ", "
               << fragStats << ". Sleeping for " << sleepTime
               << " seconds.";
            EP_LOG_DEBUG("{}", ss.str());
        }

        // Delete(reset) visitor if it finished.
        if (completed) {
            prAdapter.reset();
        }

        reconfigureSettings(preFragmentation, postFragmentation);
    }

    snooze(sleepTime);
    if (engine->getEpStats().isShutdown) {
        return false;
    }
    return true;
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

    // todo: this should be based on any tuned runtime
    return expectedDuration * 10;
}

double DefragmenterTask::getSleepTime() const {
    return engine->getConfiguration().getDefragmenterInterval();
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

void DefragmenterTask::reconfigureSettings(
        std::pair<size_t, size_t> preFragmentation,
        std::pair<size_t, size_t> postFragmentation) {
    auto state = examineRun(preFragmentation, postFragmentation);

    if (globalBucketLogger->should_log(spdlog::level::debug)) {
        EP_LOG_DEBUG(
                "{} state: pre-reconfig duration:{} sleep:{} mem:{} "
                "resident:{}",
                getDescription(),
                chunkDuration.count(),
                sleepTime,
                postFragmentation.first,
                postFragmentation.second);
    }

    switch (state) {
    case TuningState::ResetToConfig:
        resetToConfig();
        break;
    case TuningState::NoChange:
        break;
    case TuningState::SpeedUp:
        // change chunkDuration/sleepTime
        speedUp();
        break;
    case TuningState::SlowDown:
        // change chunkDuration/sleepTime
        slowDown();
        break;
    }

    if (globalBucketLogger->should_log(spdlog::level::debug)) {
        EP_LOG_DEBUG("{} post-reconfig duration:{} sleep:{} state:{}",
                     getDescription(),
                     chunkDuration.count(),
                     sleepTime,
                     int(state));
    }
}

DefragmenterTask::TuningState DefragmenterTask::examineRun(
        std::pair<size_t, size_t> preFragmentation,
        std::pair<size_t, size_t> postFragmentation) {
    auto getScore = [](std::pair<size_t, size_t> fragmentation) {
        const double memUsed = fragmentation.first;
        const double resident = fragmentation.second;
        double percent = ((resident - memUsed) / resident);
        std::cerr << (resident - memUsed) << std::endl;
        std::cerr << "P:" << percent << std::endl;
        if (percent < 0.01) {
            return size_t(0); // below threshold, all is good
        }

        // above threshold

        // The score though is scaled by size, this means that comparing
        // a pre/post score a decrease in resident is accounted. E.g.
        // We don't just care about 10% vs 20%, if the 10% is 10% of 1G and
        // the second is 20% of 1M

        // return number of bytes considered fragmented
        return size_t(resident * percent);
    };

    auto postScore = getScore(postFragmentation);

    if (postScore == 0) {
        return TuningState::ResetToConfig;
    }

    // fragmentation above threshold so state is SpeedUp
    auto state = TuningState::SpeedUp;

    // Next though compare with pre
    auto preScore = getScore(preFragmentation);

    // Is the situation improving?
    if (preScore > postScore) {
        // Yes, improving, less fragmented bytes, stay steady or slow-down?
        auto distance = preScore - postScore;

        // If there's a 'large' enough reduction, slow down
        if (distance > 1024) {
            state = TuningState::SlowDown;
        } else {
            state = TuningState::NoChange;
        }
    }

    if (globalBucketLogger->should_log(spdlog::level::debug)) {
        EP_LOG_DEBUG("{} examineRun: state: postScore:{} preScore:{}",
                     getDescription(),
                     postScore,
                     preScore);
    }
    return state;
}

void DefragmenterTask::resetToConfig() {
    chunkDuration = getChunkDuration();
    sleepTime = getSleepTime();
}

void DefragmenterTask::speedUp() {
    auto newDuration = chunkDuration + std::chrono::milliseconds(2);
    auto newSleep = sleepTime - 0.5;

    if (newDuration <= std::chrono::milliseconds(50)) {
        chunkDuration = newDuration;
    }

    if (newSleep >= 1) {
        sleepTime = newSleep;
    }
}

void DefragmenterTask::slowDown() {
    auto newDuration = chunkDuration - std::chrono::milliseconds(2);
    auto newSleep = sleepTime + 0.5;

    // todo copy chunk and sleep at construction
    // if a dynamic change occurs just reset everything

    auto l1 = getChunkDuration(); // upper limit1
    if (newDuration < l1) {
        chunkDuration = newDuration;
    } else {
        chunkDuration = l1;
    }

    auto l2 = getSleepTime(); // upper limit2
    if (newSleep < l2) {
        sleepTime = newSleep;
    } else {
        sleepTime = l2;
    }
}

// Look at fragmentation and tune
// return duration for this pass and the sleep time once done
std::pair<std::chrono::milliseconds, double> DefragmenterTask::configure()
        const {
    auto [memUsed, resident] = cb::ArenaMalloc::getFragmentationStats(
            engine->getArenaMallocClient());

    // The worse the fragmentation of this bucket
    // 1) The longer this run will be
    // 2) The shorter the delay until run again will be

    // First compare current fragmentation against acceptable threshold
    auto fragmentation = ((resident - memUsed) / resident) * 100.0;

    // What about the scale though? if resident is like 10bytes/100mb...
    // Small scale may find we cannot affect the fragmentation, so could just
    // get pointlessly aggressive?

    const double threshold = 5.0;

    auto duration = getChunkDuration();
    auto sleepTime = getSleepTime();
    if (fragmentation < threshold) {
        return {duration, sleepTime};
    }

    // Add 2ms for every 1% fragmented over the threshold?
    std::chrono::milliseconds oneMs{2};
    duration = duration + (oneMs * int(threshold - fragmentation));

    // Sleep time needs reducing, 0.5s for every 5%?
    double v = 0.5;
    double c = fragmentation / 5.0;
    sleepTime = sleepTime - (c * v);
    if (sleepTime < 0.0) {
        sleepTime = 0.0;
    }
    return {duration, sleepTime};
}
