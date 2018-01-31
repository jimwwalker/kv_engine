/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc.
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

#include "warmup.h"

#include "checkpoint.h"
#include "collections/collections_callbacks.h"
#include "common.h"
#include "connmap.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "mutation_log.h"
#include "statwriter.h"
#include "vbucket_bgfetch_item.h"

#include <platform/make_unique.h>
#include <platform/timeutils.h>

#include <limits>
#include <string>
#include <utility>
#include <array>
#include <random>

struct WarmupCookie {
    WarmupCookie(KVBucket* s, StatusCallback<GetValue>& c)
        : cb(c), epstore(s), loaded(0), skipped(0), error(0) { /* EMPTY */
    }
    StatusCallback<GetValue>& cb;
    KVBucket* epstore;
    size_t loaded;
    size_t skipped;
    size_t error;
};

// Warmup Tasks ///////////////////////////////////////////////////////////////

class WarmupInitialize : public GlobalTask {
public:
    WarmupInitialize(KVBucket& st, Warmup* w) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupInitialize, 0, false),
        _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return "Warmup - initialize";
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Typically takes single-digits ms.
        return std::chrono::milliseconds(50);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "WarmupInitialize");
        _warmup->initialize();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};

class WarmupCreateVBuckets : public GlobalTask {
public:
    WarmupCreateVBuckets(KVBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupCreateVBuckets, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - creating vbuckets: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // VB creation typically takes some 10s of milliseconds.
        return std::chrono::milliseconds(100);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "WarmupCreateVBuckets");
        _warmup->createVBuckets(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupEstimateDatabaseItemCount : public GlobalTask {
public:
    WarmupEstimateDatabaseItemCount(KVBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(),
                     TaskId::WarmupEstimateDatabaseItemCount,
                     0,
                     false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - estimate item count: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Typically takes a few 10s of milliseconds (need to open kstore files
        // and read statistics.
        return std::chrono::milliseconds(100);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "WarpupEstimateDatabaseItemCount");
        _warmup->estimateDatabaseItemCount(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupKeyDump : public GlobalTask {
public:
    WarmupKeyDump(KVBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupKeyDump, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - key dump: shard " + std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Runtime is a function of the number of keys in the database; can be
        // many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() {
        TRACE_EVENT1("ep-engine/task", "WarmupKeyDump", "shard", _shardId);
        _warmup->keyDumpforShard(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupCheckforAccessLog : public GlobalTask {
public:
    WarmupCheckforAccessLog(KVBucket& st, Warmup* w) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupCheckforAccessLog, 0,
                   false),
        _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return "Warmup - check for access log";
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Checking for the access log is a disk task (so can take a variable
        // amount of time), however it should be relatively quick as we are
        // just checking files exist.
        return std::chrono::milliseconds(100);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "WarmupCheckForAccessLog");
        _warmup->checkForAccessLog();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};

class WarmupLoadAccessLog : public GlobalTask {
public:
    WarmupLoadAccessLog(KVBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadAccessLog, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - loading access log: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Runtime is a function of the number of keys in the access log files;
        // can be many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadAccessLog");
        _warmup->loadingAccessLog(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupLoadingKVPairs : public GlobalTask {
public:
    WarmupLoadingKVPairs(KVBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadingKVPairs, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - loading KV Pairs: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Runtime is a function of the number of documents which can
        // be held in RAM (and need to be laoded from disk),
        // can be many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadingKVPairs");
        _warmup->loadKVPairsforShard(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupLoadingData : public GlobalTask {
public:
    WarmupLoadingData(KVBucket& st, uint16_t sh, Warmup* w) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadingData, 0, false),
        _shardId(sh),
        _warmup(w),
        _description("Warmup - loading data: shard " +
                     std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Runtime is a function of the number of documents which can
        // be held in RAM (and need to be laoded from disk),
        // can be many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadingData");
        _warmup->loadDataforShard(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupCompletion : public GlobalTask {
public:
    WarmupCompletion(KVBucket& st, Warmup* w) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupCompletion, 0, false),
        _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    cb::const_char_buffer getDescription() {
        return "Warmup - completion";
    }

    std::chrono::microseconds maxExpectedDuration() {
        // This task should be very quick - just the final warmup steps.
        return std::chrono::milliseconds(1);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "WarmupCompletion");
        _warmup->done();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};


static bool batchWarmupCallback(uint16_t vbId,
                                const std::set<StoredDocKey>& fetches,
                                void *arg)
{
    WarmupCookie *c = static_cast<WarmupCookie *>(arg);

    if (!c->epstore->maybeEnableTraffic()) {
        vb_bgfetch_queue_t items2fetch;
        for (auto& key : fetches) {
            // Deleted below via a unique_ptr in the next loop
            vb_bgfetch_item_ctx_t& bg_itm_ctx = items2fetch[key];
            bg_itm_ctx.isMetaOnly = GetMetaOnly::No;
            bg_itm_ctx.bgfetched_list.emplace_back(
                    std::make_unique<VBucketBGFetchItem>(nullptr, false));
            bg_itm_ctx.bgfetched_list.back()->value = &bg_itm_ctx.value;
        }

        c->epstore->getROUnderlying(vbId)->getMulti(vbId, items2fetch);

        // applyItem controls the  mode this loop operates in.
        // true we will attempt the callback (attempt a HashTable insert)
        // false we don't attempt the callback
        // in both cases the loop must delete the VBucketBGFetchItem we
        // allocated above.
        bool applyItem = true;
        for (auto& items : items2fetch) {
            vb_bgfetch_item_ctx_t& bg_itm_ctx = items.second;
            std::unique_ptr<VBucketBGFetchItem> fetchedItem(
                    std::move(bg_itm_ctx.bgfetched_list.back()));
            if (applyItem) {
                GetValue& val = *fetchedItem->value;
                if (val.getStatus() == ENGINE_SUCCESS) {
                    // NB: callback will delete the GetValue's Item
                    c->cb.callback(val);
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                    "Warmup failed to load data for vBucket = %d"
                    " key{%s} error = %X\n",
                    vbId, items.first.c_str(), val.getStatus());
                    c->error++;
                }

                if (c->cb.getStatus() == ENGINE_SUCCESS) {
                    c->loaded++;
                } else {
                    // Failed to apply an Item, so fail the rest
                    applyItem = false;
                }
            } else {
                c->skipped++;
            }
        }

        return true;
    } else {
        c->skipped++;
        return false;
    }
}

static bool warmupCallback(void *arg, uint16_t vb, const DocKey& key)
{
    WarmupCookie *cookie = static_cast<WarmupCookie*>(arg);

    if (!cookie->epstore->maybeEnableTraffic()) {
        GetValue cb = cookie->epstore->getROUnderlying(vb)->get(key, vb);

        if (cb.getStatus() == ENGINE_SUCCESS) {
            cookie->cb.callback(cb);
            cookie->loaded++;
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Warmup failed to load data "
                "for vb:%" PRIu16 ", key{%.*s}, error:%X\n",
                vb,
                int(key.size()),
                key.data(),
                cb.getStatus());
            cookie->error++;
        }

        return true;
    } else {
        cookie->skipped++;
        return false;
    }
}

const int WarmupState::Initialize = 0;
const int WarmupState::CreateVBuckets = 1;
const int WarmupState::EstimateDatabaseItemCount = 2;
const int WarmupState::KeyDump = 3;
const int WarmupState::CheckForAccessLog = 4;
const int WarmupState::LoadingAccessLog = 5;
const int WarmupState::LoadingKVPairs = 6;
const int WarmupState::LoadingData = 7;
const int WarmupState::Done = 8;

const char *WarmupState::toString(void) const {
    return getStateDescription(state.load());
}

const char *WarmupState::getStateDescription(int st) const {
    switch (st) {
    case Initialize:
        return "initialize";
    case CreateVBuckets:
        return "creating vbuckets";
    case EstimateDatabaseItemCount:
        return "estimating database item count";
    case KeyDump:
        return "loading keys";
    case CheckForAccessLog:
        return "determine access log availability";
    case LoadingAccessLog:
        return "loading access log";
    case LoadingKVPairs:
        return "loading k/v pairs";
    case LoadingData:
        return "loading data";
    case Done:
        return "done";
    default:
        return "Illegal state";
    }
}

void WarmupState::transition(int to, bool allowAnystate) {
    if (allowAnystate || legalTransition(to)) {
        std::stringstream ss;
        ss << "Warmup transition from state \""
           << getStateDescription(state.load()) << "\" to \""
           << getStateDescription(to) << "\"";
        LOG(EXTENSION_LOG_DEBUG, "%s", ss.str().c_str());
        state.store(to);
    } else {
        // Throw an exception to make it possible to test the logic ;)
        std::stringstream ss;
        ss << "Illegal state transition from \"" << *this << "\" to " << to;
        throw std::runtime_error(ss.str());
    }
}

bool WarmupState::legalTransition(int to) const {
    switch (state.load()) {
    case Initialize:
        return (to == CreateVBuckets);
    case CreateVBuckets:
        return (to == EstimateDatabaseItemCount);
    case EstimateDatabaseItemCount:
        return (to == KeyDump || to == CheckForAccessLog);
    case KeyDump:
        return (to == LoadingKVPairs || to == CheckForAccessLog);
    case CheckForAccessLog:
        return (to == LoadingAccessLog || to == LoadingData ||
                to == LoadingKVPairs || to == Done);
    case LoadingAccessLog:
        return (to == Done || to == LoadingData);
    case LoadingKVPairs:
        return (to == Done);
    case LoadingData:
        return (to == Done);

    default:
        return false;
    }
}

std::ostream& operator <<(std::ostream &out, const WarmupState &state)
{
    out << state.toString();
    return out;
}

LoadStorageKVPairCallback::LoadStorageKVPairCallback(KVBucket& ep,
                                                     bool _maybeEnableTraffic,
                                                     int _warmupState)
    : vbuckets(ep.vbMap),
      stats(ep.getEPEngine().getEpStats()),
      epstore(ep),
      startTime(ep_real_time()),
      hasPurged(false),
      maybeEnableTraffic(_maybeEnableTraffic),
      warmupState(_warmupState) {
}

void LoadStorageKVPairCallback::callback(GetValue &val) {
    // This callback method is responsible for deleting the Item
    std::unique_ptr<Item> i(std::move(val.item));

    // Don't attempt to load the system event documents.
    if (i->getKey().getDocNamespace() == DocNamespace::System) {
        return;
    }

    bool stopLoading = false;
    if (i != NULL && !epstore.getWarmup()->isComplete()) {
        VBucketPtr vb = vbuckets.getBucket(i->getVBucketId());
        if (!vb) {
            setStatus(ENGINE_NOT_MY_VBUCKET);
            return;
        }
        bool succeeded(false);
        int retry = 2;
        do {
            if (i->getCas() == static_cast<uint64_t>(-1)) {
                if (val.isPartial()) {
                    i->setCas(0);
                } else {
                    i->setCas(vb->nextHLCCas());
                }
            }

            EPVBucket* epVb = dynamic_cast<EPVBucket*>(vb.get());
            if (!epVb) {
                setStatus(ENGINE_NOT_MY_VBUCKET);
                return;
            }

            const auto res =
                    epVb->insertFromWarmup(*i, shouldEject(), val.isPartial());
            switch (res) {
            case MutationStatus::NoMem:
                if (retry == 2) {
                    if (hasPurged) {
                        if (++stats.warmOOM == 1) {
                            LOG(EXTENSION_LOG_WARNING,
                                "Warmup dataload failure: max_size too low.");
                        }
                    } else {
                        LOG(EXTENSION_LOG_WARNING,
                            "Emergency startup purge to free space for load.");
                        purge();
                    }
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "Cannot store an item after emergency purge.");
                    ++stats.warmOOM;
                }
                break;
            case MutationStatus::InvalidCas:
                LOG(EXTENSION_LOG_DEBUG,
                    "Value changed in memory before restore from disk. "
                    "Ignored disk value for: key{%s}.", i->getKey().c_str());
                ++stats.warmDups;
                succeeded = true;
                break;
            case MutationStatus::NotFound:
                succeeded = true;
                break;
            default:
                throw std::logic_error(
                        "LoadStorageKVPairCallback::callback: "
                        "Unexpected result from HashTable::insert: " +
                        std::to_string(static_cast<uint16_t>(res)));
            }
        } while (!succeeded && retry-- > 0);

        if (maybeEnableTraffic) {
            stopLoading = epstore.maybeEnableTraffic();
        }

        switch (warmupState) {
            case WarmupState::KeyDump:
                if (stats.warmOOM) {
                    epstore.getWarmup()->setOOMFailure();
                    stopLoading = true;
                } else {
                    ++stats.warmedUpKeys;
                }
                break;
            case WarmupState::LoadingData:
            case WarmupState::LoadingAccessLog:
                if (epstore.getItemEvictionPolicy() == FULL_EVICTION) {
                    ++stats.warmedUpKeys;
                }
                ++stats.warmedUpValues;
                break;
            default:
                ++stats.warmedUpKeys;
                ++stats.warmedUpValues;
        }
    } else {
        stopLoading = true;
    }

    if (stopLoading) {
        // warmup has completed, return ENGINE_ENOMEM to
        // cancel remaining data dumps from couchstore
        if (epstore.getWarmup()->setComplete()) {
            epstore.getWarmup()->setWarmupTime();
            epstore.warmupCompleted();
            LOG(EXTENSION_LOG_NOTICE, "Warmup completed in %s",
                cb::time2text(std::chrono::nanoseconds(
                        epstore.getWarmup()->getTime())).c_str());

        }
        LOG(EXTENSION_LOG_NOTICE,
            "Engine warmup is complete, request to stop "
            "loading remaining database");
        setStatus(ENGINE_ENOMEM);
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

bool LoadStorageKVPairCallback::shouldEject() const {
    return stats.getEstimatedTotalMemoryUsed() >= stats.mem_low_wat;
}

void LoadStorageKVPairCallback::purge() {
    class EmergencyPurgeVisitor : public VBucketVisitor,
                                  public HashTableVisitor {
    public:
        EmergencyPurgeVisitor(KVBucket& store) :
            epstore(store) {}

        void visitBucket(VBucketPtr &vb) override {
            if (vBucketFilter(vb->getId())) {
                currentBucket = vb;
                vb->ht.visit(*this);
            }
        }

        bool visit(const HashTable::HashBucketLock& lh,
                   StoredValue& v) override {
            StoredValue* vPtr = &v;
            currentBucket->ht.unlocked_ejectItem(
                    vPtr, epstore.getItemEvictionPolicy());
            return true;
        }

    private:
        KVBucket& epstore;
        VBucketPtr currentBucket;
    };

    auto vbucketIds(vbuckets.getBuckets());
    EmergencyPurgeVisitor epv(epstore);
    for (auto vbid : vbucketIds) {
        VBucketPtr vb = vbuckets.getBucket(vbid);
        if (vb) {
            epv.visitBucket(vb);
        }
    }
    hasPurged = true;
}

void LoadValueCallback::callback(CacheLookup &lookup)
{
    if (warmupState == WarmupState::LoadingData) {
        VBucketPtr vb = vbuckets.getBucket(lookup.getVBucketId());
        if (!vb) {
            return;
        }

        auto hbl = vb->ht.getLockedBucket(lookup.getKey());

        StoredValue* v = vb->ht.unlocked_find(lookup.getKey(),
                                              hbl.getBucketNum(),
                                              WantsDeleted::No,
                                              TrackReference::Yes);
        if (v && v->isResident()) {
            setStatus(ENGINE_KEY_EEXISTS);
            return;
        }
    }
    setStatus(ENGINE_SUCCESS);
}

//////////////////////////////////////////////////////////////////////////////
//                                                                          //
//    Implementation of the warmup class                                    //
//                                                                          //
//////////////////////////////////////////////////////////////////////////////

Warmup::Warmup(KVBucket& st, Configuration& config_)
    : state(),
      store(st),
      config(config_),
      shardVbStates(store.vbMap.getNumShards()),
      threadtask_count(0),
      shardKeyDumpStatus(store.vbMap.getNumShards()),
      shardVbIds(store.vbMap.getNumShards()),
      estimatedItemCount(std::numeric_limits<size_t>::max()),
      cleanShutdown(true),
      corruptAccessLog(false),
      warmupComplete(false),
      warmupOOMFailure(false),
      estimatedWarmupCount(std::numeric_limits<size_t>::max()),
      createVBucketsComplete(false) {
}

void Warmup::addToTaskSet(size_t taskId) {
    LockHolder lh(taskSetMutex);
    taskSet.insert(taskId);
}

void Warmup::removeFromTaskSet(size_t taskId) {
    LockHolder lh(taskSetMutex);
    taskSet.erase(taskId);
}

void Warmup::setEstimatedWarmupCount(size_t to)
{
    estimatedWarmupCount.store(to);
}

size_t Warmup::getEstimatedItemCount()
{
    return estimatedItemCount.load();
}

void Warmup::start(void)
{
    step();
}

void Warmup::stop(void)
{
    {
        LockHolder lh(taskSetMutex);
        if(taskSet.empty()) {
            return;
        }
        for (auto id : taskSet) {
            ExecutorPool::get()->cancel(id);
        }
        taskSet.clear();
    }
    transition(WarmupState::Done, true);
    done();
}

void Warmup::scheduleInitialize()
{
    ExTask task = std::make_shared<WarmupInitialize>(store, this);
    ExecutorPool::get()->schedule(task);
}

void Warmup::initialize()
{
    {
        std::lock_guard<std::mutex> lock(warmupStart.mutex);
        warmupStart.time = ProcessClock::now();
    }

    std::map<std::string, std::string> session_stats;
    store.getOneROUnderlying()->getPersistedStats(session_stats);


    std::map<std::string, std::string>::const_iterator it =
        session_stats.find("ep_force_shutdown");

    if (it == session_stats.end() || it->second.compare("false") != 0) {
        cleanShutdown = false;
    }

    populateShardVbStates();
    transition(WarmupState::CreateVBuckets);
}

void Warmup::scheduleCreateVBuckets()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupCreateVBuckets>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::createVBuckets(uint16_t shardId) {
    size_t maxEntries = store.getEPEngine().getMaxFailoverEntries();

    // Iterate over all VBucket states defined for this shard, creating VBucket
    // objects if they do not already exist.
    for (const auto itr : shardVbStates[shardId]) {
        uint16_t vbid = itr.first;
        vbucket_state vbs = itr.second;

        VBucketPtr vb = store.getVBucket(vbid);
        if (!vb) {
            std::unique_ptr<FailoverTable> table;
            if (vbs.failovers.empty()) {
                table = std::make_unique<FailoverTable>(maxEntries);
            } else {
                table = std::make_unique<FailoverTable>(vbs.failovers,
                                                        maxEntries);
            }
            KVShard* shard = store.getVBuckets().getShardByVbId(vbid);

            vb = store.makeVBucket(
                    vbid,
                    vbs.state,
                    shard,
                    std::move(table),
                    std::make_unique<NotifyNewSeqnoCB>(store),
                    vbs.state,
                    vbs.highSeqno,
                    vbs.lastSnapStart,
                    vbs.lastSnapEnd,
                    vbs.purgeSeqno,
                    vbs.maxCas,
                    vbs.hlcCasEpochSeqno,
                    vbs.mightContainXattrs,
                    config.isCollectionsPrototypeEnabled()
                            ? store.getROUnderlyingByShard(shardId)
                                      ->getCollectionsManifest(vbid)
                            : "" /*no collections manifest*/);

            if(vbs.state == vbucket_state_active && !cleanShutdown) {
                if (static_cast<uint64_t>(vbs.highSeqno) == vbs.lastSnapEnd) {
                    vb->failovers->createEntry(vbs.lastSnapEnd);
                } else {
                    vb->failovers->createEntry(vbs.lastSnapStart);
                }
            }

            store.vbMap.addBucket(vb);
        }

        // Pass the open checkpoint Id for each vbucket.
        vb->checkpointManager->setOpenCheckpointId(vbs.checkpointId + 1);
        // Pass the max deleted seqno for each vbucket.
        vb->ht.setMaxDeletedRevSeqno(vbs.maxDeletedSeqno);
        // For each vbucket, set its latest checkpoint Id that was
        // successfully persisted.
        vb->setPersistenceCheckpointId(vbs.checkpointId);
        // For each vbucket, set the last persisted seqno checkpoint
        vb->setPersistenceSeqno(vbs.highSeqno);
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        processCreateVBucketsComplete();
        transition(WarmupState::EstimateDatabaseItemCount);
    }
}

void Warmup::processCreateVBucketsComplete() {
    std::unique_lock<std::mutex> lock(pendingSetVBStateCookiesMutex);
    createVBucketsComplete = true;
    if (!pendingSetVBStateCookies.empty()) {
        LOG(EXTENSION_LOG_NOTICE,
            "Warmup::processCreateVBucketsComplete unblocking %zu cookie(s)",
            pendingSetVBStateCookies.size());
        while (!pendingSetVBStateCookies.empty()) {
            const void* c = pendingSetVBStateCookies.front();
            pendingSetVBStateCookies.pop_front();
            // drop lock to avoid lock inversion
            lock.unlock();
            store.getEPEngine().notifyIOComplete(c, ENGINE_SUCCESS);
            lock.lock();
        }
    }
}

bool Warmup::shouldSetVBStateBlock(const void* cookie) {
    std::lock_guard<std::mutex> lg(pendingSetVBStateCookiesMutex);
    if (!createVBucketsComplete) {
        pendingSetVBStateCookies.push_back(cookie);
        return true;
    }
    return false;
}

void Warmup::scheduleEstimateDatabaseItemCount()
{
    threadtask_count = 0;
    estimateTime.store(ProcessClock::duration::zero());
    estimatedItemCount = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupEstimateDatabaseItemCount>(
                store, i, this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::estimateDatabaseItemCount(uint16_t shardId)
{
    auto st = ProcessClock::now();
    size_t item_count = 0;

    for (const auto vbid : shardVbIds[shardId]) {
        size_t vbItemCount = store.getROUnderlyingByShard(shardId)->
                                                        getItemCount(vbid);
        VBucketPtr vb = store.getVBucket(vbid);
        if (vb) {
            vb->setNumTotalItems(vbItemCount);
        }
        item_count += vbItemCount;
    }

    estimatedItemCount.fetch_add(item_count);
    estimateTime.fetch_add(ProcessClock::now() - st);

    if (++threadtask_count == store.vbMap.getNumShards()) {
        if (store.getItemEvictionPolicy() == VALUE_ONLY) {
            transition(WarmupState::KeyDump);
        } else {
            transition(WarmupState::CheckForAccessLog);
        }
    }
}

void Warmup::scheduleKeyDump()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupKeyDump>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }

}

void Warmup::keyDumpforShard(uint16_t shardId)
{
    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    auto cb = std::make_shared<LoadStorageKVPairCallback>(
            store, false, state.getState());
    auto cl =
            std::make_shared<Collections::VB::LogicallyDeletedCallback>(store);

    for (const auto vbid : shardVbIds[shardId]) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, vbid, 0,
                                                    DocumentFilter::NO_DELETES,
                                                    ValueFilter::KEYS_ONLY);
        if (ctx) {
            auto errorCode = kvstore->scan(ctx);
            kvstore->destroyScanContext(ctx);
            if (errorCode == scan_again) { // ENGINE_ENOMEM
                // skip loading remaining VBuckets as memory limit was reached
                break;
            }
        }
    }

    shardKeyDumpStatus[shardId] = true;

    if (++threadtask_count == store.vbMap.getNumShards()) {
        bool success = false;
        for (size_t i = 0; i < store.vbMap.getNumShards(); i++) {
            if (shardKeyDumpStatus[i]) {
                success = true;
            } else {
                success = false;
                break;
            }
        }

        if (success) {
            transition(WarmupState::CheckForAccessLog);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to dump keys, falling back to full dump");
            transition(WarmupState::LoadingKVPairs);
        }
    }
}

void Warmup::scheduleCheckForAccessLog()
{
    ExTask task = std::make_shared<WarmupCheckforAccessLog>(store, this);
    ExecutorPool::get()->schedule(task);
}

void Warmup::checkForAccessLog()
{
    {
        std::lock_guard<std::mutex> lock(warmupStart.mutex);
        metadata.store(ProcessClock::now() - warmupStart.time);
    }
    LOG(EXTENSION_LOG_NOTICE, "metadata loaded in %s",
        cb::time2text(std::chrono::nanoseconds(metadata.load())).c_str());

    if (store.maybeEnableTraffic()) {
        transition(WarmupState::Done);
    }

    size_t accesslogs = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        std::string curr = store.accessLog[i].getLogFile();
        std::string old = store.accessLog[i].getLogFile();
        old.append(".old");
        if (access(curr.c_str(), F_OK) == 0 ||
            access(old.c_str(), F_OK) == 0) {
            accesslogs++;
        }
    }
    if (accesslogs == store.vbMap.shards.size()) {
        transition(WarmupState::LoadingAccessLog);
    } else {
        if (store.getItemEvictionPolicy() == VALUE_ONLY) {
            transition(WarmupState::LoadingData);
        } else {
            transition(WarmupState::LoadingKVPairs);
        }
    }

}

void Warmup::scheduleLoadingAccessLog()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupLoadAccessLog>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::loadingAccessLog(uint16_t shardId)
{
    LoadStorageKVPairCallback load_cb(store, true, state.getState());
    bool success = false;
    auto stTime = ProcessClock::now();
    if (store.accessLog[shardId].exists()) {
        try {
            store.accessLog[shardId].open();
            if (doWarmup(store.accessLog[shardId],
                         shardVbStates[shardId],
                         load_cb) != (size_t)-1) {
                success = true;
            }
        } catch (MutationLog::ReadException &e) {
            corruptAccessLog = true;
            LOG(EXTENSION_LOG_WARNING, "Error reading warmup access log:  %s",
                    e.what());
        }
    }

    if (!success) {
        // Do we have the previous file?
        std::string nm = store.accessLog[shardId].getLogFile();
        nm.append(".old");
        MutationLog old(nm);
        if (old.exists()) {
            try {
                old.open();
                if (doWarmup(old, shardVbStates[shardId], load_cb) !=
                    (size_t)-1) {
                    success = true;
                }
            } catch (MutationLog::ReadException &e) {
                corruptAccessLog = true;
                LOG(EXTENSION_LOG_WARNING, "Error reading old access log:  %s",
                        e.what());
            }
        }
    }

    size_t numItems = store.getEPEngine().getEpStats().warmedUpValues;
    if (success && numItems) {
        LOG(EXTENSION_LOG_NOTICE,
            "%" PRIu64 " items loaded from access log, completed in %s",
            uint64_t(numItems),
            cb::time2text(ProcessClock::now() - stTime).c_str());
    } else {
        size_t estimatedCount= store.getEPEngine().getEpStats().warmedUpKeys;
        setEstimatedWarmupCount(estimatedCount);
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        if (!store.maybeEnableTraffic()) {
            transition(WarmupState::LoadingData);
        } else {
            transition(WarmupState::Done);
        }

    }
}

size_t Warmup::doWarmup(MutationLog& lf,
                        const std::map<uint16_t, vbucket_state>& vbmap,
                        StatusCallback<GetValue>& cb) {
    MutationLogHarvester harvester(lf, &store.getEPEngine());
    std::map<uint16_t, vbucket_state>::const_iterator it;
    for (it = vbmap.begin(); it != vbmap.end(); ++it) {
        harvester.setVBucket(it->first);
    }

    // To constrain the number of elements from the access log we have to keep
    // alive (there may be millions of items per-vBucket), process it
    // a batch at a time.
    std::chrono::nanoseconds log_load_duration{};
    std::chrono::nanoseconds log_apply_duration{};
    WarmupCookie cookie(&store, cb);

    auto alog_iter = lf.begin();
    do {
        // Load a chunk of the access log file
        auto start = ProcessClock::now();
        alog_iter = harvester.loadBatch(alog_iter, config.getWarmupBatchSize());
        log_load_duration += (ProcessClock::now() - start);

        // .. then apply it to the store.
        auto apply_start = ProcessClock::now();
        if (store.multiBGFetchEnabled()) {
            harvester.apply(&cookie, &batchWarmupCallback);
        } else {
            harvester.apply(&cookie, &warmupCallback);
        }
        log_apply_duration += (ProcessClock::now() - apply_start);
    } while (alog_iter != lf.end());

    size_t total = harvester.total();
    setEstimatedWarmupCount(total);
    LOG(EXTENSION_LOG_DEBUG, "Completed log read in %s with %ld entries",
        cb::time2text(log_load_duration).c_str(), total);

    LOG(EXTENSION_LOG_DEBUG,
        "Populated log in %s with(l: %ld, s: %ld, e: %ld)",
        cb::time2text(log_apply_duration).c_str(), cookie.loaded, cookie.skipped,
        cookie.error);

    return cookie.loaded;
}

void Warmup::scheduleLoadingKVPairs()
{
    // We reach here only if keyDump didn't return SUCCESS or if
    // in case of Full Eviction. Either way, set estimated value
    // count equal to the estimated item count, as very likely no
    // keys have been warmed up at this point.
    setEstimatedWarmupCount(estimatedItemCount);

    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupLoadingKVPairs>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }

}

ValueFilter getValueFilterForCompressionMode(
                    const BucketCompressionMode& compressionMode) {

    if (compressionMode != BucketCompressionMode::Off) {
        return ValueFilter::VALUES_COMPRESSED;
    }

    return ValueFilter::VALUES_DECOMPRESSED;
}

void Warmup::loadKVPairsforShard(uint16_t shardId)
{
    bool maybe_enable_traffic = false;
    scan_error_t errorCode = scan_success;

    if (store.getItemEvictionPolicy() == FULL_EVICTION) {
        maybe_enable_traffic = true;
    }

    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    auto cb = std::make_shared<LoadStorageKVPairCallback>(
            store, maybe_enable_traffic, state.getState());
    auto cl =
            std::make_shared<LoadValueCallback>(store.vbMap, state.getState());

    ValueFilter valFilter = getValueFilterForCompressionMode(
                                    store.getEPEngine().getCompressionMode());

    for (const auto vbid : shardVbIds[shardId]) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, vbid, 0,
                                                    DocumentFilter::NO_DELETES,
                                                    valFilter);
        if (ctx) {
            errorCode = kvstore->scan(ctx);
            kvstore->destroyScanContext(ctx);
            if (errorCode == scan_again) { // ENGINE_ENOMEM
                // skip loading remaining VBuckets as memory limit was reached
                break;
            }
        }
    }
    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::Done);
    }
}

void Warmup::scheduleLoadingData()
{
    size_t estimatedCount = store.getEPEngine().getEpStats().warmedUpKeys;
    setEstimatedWarmupCount(estimatedCount);

    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupLoadingData>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::loadDataforShard(uint16_t shardId)
{
    scan_error_t errorCode = scan_success;

    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    auto cb = std::make_shared<LoadStorageKVPairCallback>(
            store, true, state.getState());
    auto cl =
            std::make_shared<LoadValueCallback>(store.vbMap, state.getState());

    ValueFilter valFilter = getValueFilterForCompressionMode(
                                          store.getEPEngine().getCompressionMode());

    for (const auto vbid : shardVbIds[shardId]) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, vbid, 0,
                                                    DocumentFilter::NO_DELETES,
                                                    valFilter);
        if (ctx) {
            errorCode = kvstore->scan(ctx);
            kvstore->destroyScanContext(ctx);
            if (errorCode == scan_again) { // ENGINE_ENOMEM
                // skip loading remaining VBuckets as memory limit was reached
                break;
            }
        }
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::Done);
    }
}

void Warmup::scheduleCompletion() {
    ExTask task = std::make_shared<WarmupCompletion>(store, this);
    ExecutorPool::get()->schedule(task);
}

void Warmup::done()
{
    if (setComplete()) {
        setWarmupTime();
        store.warmupCompleted();
        LOG(EXTENSION_LOG_NOTICE, "warmup completed in %s",
            cb::time2text(std::chrono::nanoseconds(warmup.load())).c_str());
    }
}

void Warmup::step() {
    switch (state.getState()) {
        case WarmupState::Initialize:
            scheduleInitialize();
            break;
        case WarmupState::CreateVBuckets:
            scheduleCreateVBuckets();
            break;
        case WarmupState::EstimateDatabaseItemCount:
            scheduleEstimateDatabaseItemCount();
            break;
        case WarmupState::KeyDump:
            scheduleKeyDump();
            break;
        case WarmupState::CheckForAccessLog:
            scheduleCheckForAccessLog();
            break;
        case WarmupState::LoadingAccessLog:
            scheduleLoadingAccessLog();
            break;
        case WarmupState::LoadingKVPairs:
            scheduleLoadingKVPairs();
            break;
        case WarmupState::LoadingData:
            scheduleLoadingData();
            break;
        case WarmupState::Done:
            scheduleCompletion();
            break;
        default:
            throw std::logic_error("Warmup::step: illegal warmup state:" +
                                   std::to_string(state.getState()));
    }
}

void Warmup::transition(int to, bool force) {
    int old = state.getState();
    if (old != WarmupState::Done) {
        state.transition(to, force);
        step();
    }
}

template <typename T>
void Warmup::addStat(const char *nm, const T &val, ADD_STAT add_stat,
                     const void *c) const {
    std::string name = "ep_warmup";
    if (nm != NULL) {
        name.append("_");
        name.append(nm);
    }

    std::stringstream value;
    value << val;
    add_casted_stat(name.data(), value.str().data(), add_stat, c);
}

void Warmup::addStats(ADD_STAT add_stat, const void *c) const
{
    using namespace std::chrono;

    EPStats& stats = store.getEPEngine().getEpStats();
    addStat(NULL, "enabled", add_stat, c);
    const char* stateName = state.toString();
    addStat("state", stateName, add_stat, c);
    if (warmupComplete.load()) {
        addStat("thread", "complete", add_stat, c);
    } else {
        addStat("thread", "running", add_stat, c);
    }
    addStat("key_count", stats.warmedUpKeys, add_stat, c);
    addStat("value_count", stats.warmedUpValues, add_stat, c);
    addStat("dups", stats.warmDups, add_stat, c);
    addStat("oom", stats.warmOOM, add_stat, c);
    addStat("min_memory_threshold",
            stats.warmupMemUsedCap * 100.0,
            add_stat,
            c);
    addStat("min_item_threshold", stats.warmupNumReadCap * 100.0, add_stat, c);

    auto md_time = metadata.load();
    if (md_time > md_time.zero()) {
        addStat("keys_time",
                duration_cast<microseconds>(md_time).count(),
                add_stat,
                c);
    }

    auto w_time = warmup.load();
    if (w_time > w_time.zero()) {
        addStat("time",
                duration_cast<microseconds>(w_time).count(),
                add_stat,
                c);
    }

    size_t itemCount = estimatedItemCount.load();
    if (itemCount == std::numeric_limits<size_t>::max()) {
        addStat("estimated_key_count", "unknown", add_stat, c);
    } else {
        auto e_time = estimateTime.load();
        if (e_time != e_time.zero()) {
            addStat("estimate_time",
                    duration_cast<microseconds>(e_time).count(),
                    add_stat,
                    c);
        }
        addStat("estimated_key_count", itemCount, add_stat, c);
    }

    if (corruptAccessLog) {
        addStat("access_log", "corrupt", add_stat, c);
    }

    size_t warmupCount = estimatedWarmupCount.load();
    if (warmupCount == std::numeric_limits<size_t>::max()) {
        addStat("estimated_value_count", "unknown", add_stat, c);
    } else {
        addStat("estimated_value_count", warmupCount, add_stat, c);
    }
}

/* In the case of CouchKVStore, all vbucket states of all the shards are stored
 * in a single instance. ForestKVStore stores only the vbucket states specific
 * to that shard. Hence the vbucket states of all the shards need to be
 * retrieved */
uint16_t Warmup::getNumKVStores()
{
    Configuration& config = store.getEPEngine().getConfiguration();
    if (config.getBackend().compare("couchdb") == 0) {
        return 1;
    } else if (config.getBackend().compare("forestdb") == 0) {
        return config.getMaxNumShards();
    } else if (config.getBackend().compare("rocksdb") == 0) {
        return config.getMaxNumShards();
    }
    return 0;
}

void Warmup::populateShardVbStates()
{
    uint16_t numKvs = getNumKVStores();

    for (size_t i = 0; i < numKvs; i++) {
        std::vector<vbucket_state *> allVbStates =
                     store.getROUnderlyingByShard(i)->listPersistedVbuckets();
        for (uint16_t vb = 0; vb < allVbStates.size(); vb++) {
            if (!allVbStates[vb] || allVbStates[vb]->state == vbucket_state_dead) {
                continue;
            }
            std::map<uint16_t, vbucket_state> &shardVB =
                shardVbStates[vb % store.vbMap.getNumShards()];
            shardVB.insert(std::pair<uint16_t, vbucket_state>(vb,
                                                          *(allVbStates[vb])));
        }
    }

    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        std::vector<uint16_t> activeVBs, replicaVBs;
        std::map<uint16_t, vbucket_state>::const_iterator it;
        for (it = shardVbStates[i].begin(); it != shardVbStates[i].end(); ++it) {
            uint16_t vbid = it->first;
            vbucket_state vbs = it->second;
            if (vbs.state == vbucket_state_active) {
                activeVBs.push_back(vbid);
            } else if (vbs.state == vbucket_state_replica) {
                replicaVBs.push_back(vbid);
            }
        }

        // Push one active VB to the front.
        // When the ratio of RAM to VBucket is poor (big vbuckets) this will
        // ensure we at least bring active data in before replicas eat RAM.
        if (!activeVBs.empty()) {
            shardVbIds[i].push_back(activeVBs.back());
            activeVBs.pop_back();
        }

        // Now the VB lottery can begin.
        // Generate a psudeo random, weighted list of active/replica vbuckets.
        // The random seed is the shard ID so that re-running warmup
        // for the same shard and vbucket set always gives the same output and keeps
        // nodes of the cluster more equal after a warmup.

        std::mt19937 twister(i);
        // Give 'true' (aka active) 60% of the time
        // Give 'false' (aka replica) 40% of the time.
        std::bernoulli_distribution distribute(0.6);
        std::array<std::vector<uint16_t>*, 2> activeReplicaSource = {{&activeVBs,
                                                                      &replicaVBs}};

        while (!activeVBs.empty() || !replicaVBs.empty()) {
            const bool active = distribute(twister);
            int num = active ? 0 : 1;
            if (!activeReplicaSource[num]->empty()) {
                shardVbIds[i].push_back(activeReplicaSource[num]->back());
                activeReplicaSource[num]->pop_back();
            } else {
                // Once active or replica set is empty, just drain the other one.
                num = num ^ 1;
                while (!activeReplicaSource[num]->empty()) {
                    shardVbIds[i].push_back(activeReplicaSource[num]->back());
                    activeReplicaSource[num]->pop_back();
                }
            }
        }
    }
}
