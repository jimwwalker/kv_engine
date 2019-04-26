/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "stats.h"

#include "objectregistry.h"

#include <platform/cb_arena_malloc.h>

#ifndef DEFAULT_MAX_DATA_SIZE
/* Something something something ought to be enough for anybody */
#define DEFAULT_MAX_DATA_SIZE (std::numeric_limits<size_t>::max())
#endif

EPStats::EPStats()
    : warmedUpKeys(0),
      warmedUpValues(0),
      warmDups(0),
      warmOOM(0),
      warmupMemUsedCap(0),
      warmupNumReadCap(0),
      replicationThrottleWriteQueueCap(0),
      diskQueueSize(0),
      vbBackfillQueueSize(0),
      flusher_todo(0),
      flusherCommits(0),
      cumulativeFlushTime(0),
      cumulativeCommitTime(0),
      tooYoung(0),
      tooOld(0),
      totalPersisted(0),
      totalPersistVBState(0),
      totalEnqueued(0),
      flushFailed(0),
      flushExpired(0),
      expired_access(0),
      expired_compactor(0),
      expired_pager(0),
      beginFailed(0),
      commitFailed(0),
      dirtyAge(0),
      dirtyAgeHighWat(0),
      commit_time(0),
      vbucketDeletions(0),
      vbucketDeletionFail(0),
      mem_low_wat(0),
      mem_low_wat_percent(0),
      mem_high_wat(0),
      mem_high_wat_percent(0),
      cursorDroppingLThreshold(0),
      cursorDroppingUThreshold(0),
      cursorsDropped(0),
      cursorMemoryFreed(0),
      pagerRuns(0),
      expiryPagerRuns(0),
      freqDecayerRuns(0),
      itemsExpelledFromCheckpoints(0),
      itemsRemovedFromCheckpoints(0),
      numValueEjects(0),
      numFailedEjects(0),
      numNotMyVBuckets(0),
      estimatedTotalMemory(0),
      forceShutdown(false),
      oom_errors(0),
      tmp_oom_errors(0),
      pendingOps(0),
      pendingOpsTotal(0),
      pendingOpsMax(0),
      pendingOpsMaxDuration(0),
      pendingCompactions(0),
      bg_fetched(0),
      bg_meta_fetched(0),
      numRemainingBgItems(0),
      numRemainingBgJobs(0),
      bgNumOperations(0),
      bgWait(0),
      bgMinWait(0),
      bgMaxWait(0),
      bgLoad(0),
      bgMinLoad(0),
      bgMaxLoad(0),
      vbucketDelMaxWalltime(0),
      vbucketDelTotWalltime(0),
      replicationThrottleThreshold(0),
      numOpsStore(0),
      numOpsDelete(0),
      numOpsGet(0),
      numOpsGetMeta(0),
      numOpsSetMeta(0),
      numOpsDelMeta(0),
      numOpsSetMetaResolutionFailed(0),
      numOpsDelMetaResolutionFailed(0),
      numOpsSetRetMeta(0),
      numOpsDelRetMeta(0),
      numOpsGetMetaOnSetWithMeta(0),
      alogRuns(0),
      accessScannerSkips(0),
      alogNumItems(0),
      alogTime(0),
      alogRuntime(0),
      expPagerTime(0),
      isShutdown(false),
      rollbackCount(0),
      defragNumVisited(0),
      defragNumMoved(0),
      defragStoredValueNumMoved(0),
      compressorNumVisited(0),
      compressorNumCompressed(0),
      dirtyAgeHisto(),
      diskCommitHisto(),
      timingLog(NULL),
      maxDataSize(DEFAULT_MAX_DATA_SIZE),
      // A "sensible" default, will change when setMaxDataSize is called
      memUsedMergeThreshold(102400),
      memUsedMergeThresholdPercent(0.5) {
}

EPStats::~EPStats() {
    delete timingLog;
}

void EPStats::setMaxDataSize(size_t size) {
    if (size > 0) {
        maxDataSize.store(size);
        calculateMemUsedMergeThreshold();
    }
}

void EPStats::setMemUsedMergeThresholdPercent(float percent) {
    memUsedMergeThresholdPercent = percent;
    calculateMemUsedMergeThreshold();
}

void EPStats::calculateMemUsedMergeThreshold() {
    // threshold is n% of total (but divided by the number of CoreStore
    // elements, i.e. nCpu)
    memUsedMergeThreshold =
            maxDataSize * (memUsedMergeThresholdPercent / 100.0);
    memUsedMergeThreshold = memUsedMergeThreshold / coreLocal.size();
}

void EPStats::memAllocated(size_t sz) {
    if (isShutdown) {
        return;
    }

    if (0 == sz) {
        return;
    }

    auto& coreMemory = coreLocal.get()->totalMemory;

    // Update the coreMemory and also create a local copy of the old value + sz
    // This value will be used to check the threshold
    auto value = coreMemory.fetch_add(sz) + sz;

    maybeUpdateEstimatedTotalMemUsed(coreMemory, value);
}

void EPStats::memDeallocated(size_t sz) {
    if (isShutdown) {
        return;
    }

    if (0 == sz) {
        return;
    }

    auto& coreMemory = coreLocal.get()->totalMemory;

    // Update the coreMemory and also create a local copy of the old value - sz
    // This value will be used to check the threshold
    auto value = coreMemory.fetch_sub(sz) - sz;

    maybeUpdateEstimatedTotalMemUsed(coreMemory, value);
}

void EPStats::maybeUpdateEstimatedTotalMemUsed(
        cb::RelaxedAtomic<int64_t>& coreMemory, int64_t value) {
    // If this thread succeeds in swapping, this thread updates total
    if (std::abs(value) > memUsedMergeThreshold) {
        // Swap the core's value to 0 and update total with whatever we got
        estimatedTotalMemory->fetch_add(coreMemory.exchange(0));
    }
}

size_t EPStats::getPreciseTotalMemoryUsed() const {
    if (isMemoryTrackingEnabled()) {
        estimatedTotalMemory.get()->store(cb::ArenaMalloc::getAllocated(arena));
        return estimatedTotalMemory.get()->load();
    }
    return size_t(std::max(size_t(0), getCurrentSize() + getMemOverhead()));
}

size_t EPStats::getCurrentSize() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->currentSize;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getNumBlob() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->numBlob;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getBlobOverhead() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->blobOverhead;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getTotalValueSize() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->totalValueSize;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getNumStoredVal() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->numStoredVal;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getStoredValSize() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->totalStoredValSize;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getMemOverhead() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->memOverhead;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getNumItem() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->numItem;
    }
    return std::max(int64_t(0), result);
}
