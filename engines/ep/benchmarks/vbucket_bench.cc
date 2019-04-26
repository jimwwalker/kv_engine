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

/*
 * Benchmarks relating to the VBucket class.
 */

#include "checkpoint_manager.h"
#include "engine_fixture.h"
#include "fakes/fake_executorpool.h"
#include "stored_value_factories.h"

#include "../tests/module_tests/thread_gate.h"

#include <mock/mock_synchronous_ep_engine.h>
#include <programs/engine_testapp/mock_server.h>

#include <folly/portability/GTest.h>

#include <algorithm>
#include <thread>

enum class Store { Couchstore = 0, RocksDB = 1 };

static std::string to_string(Store store) {
    switch (store) {
    case Store::Couchstore:
        return "couchdb";
    case Store::RocksDB:
        return "rocksdb";
    }
    throw std::invalid_argument("to_string(Store): invalid enumeration " +
                                std::to_string(int(store)));
}

class VBucketBench : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        store = Store(state.range(0));
        varConfig = "backend=" + to_string(store) +
                    // A number of benchmarks require more than the default
                    // 100MB bucket quota - bump to ~1GB.
                    ";max_size=1000000000";
        EngineFixture::SetUp(state);
        if (state.thread_index == 0) {
            engine->getKVBucket()->setVBucketState(Vbid(0),
                                                   vbucket_state_active);
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            ASSERT_EQ(ENGINE_SUCCESS,
                      engine->getKVBucket()->deleteVBucket(vbid, nullptr));
            executorPool->runNextTask(
                    AUXIO_TASK_IDX,
                    "Removing (dead) vb:0 from memory and disk");
        }
        EngineFixture::TearDown(state);
    }

    /// Flush all items in the vBucket to disk.
    size_t flushAllItems(Vbid vbid) {
        size_t itemsFlushed = 0;
        auto& ep = dynamic_cast<EPBucket&>(*engine->getKVBucket());
        bool moreAvailable;
        do {
            size_t count;
            std::tie(moreAvailable, count) = ep.flushVBucket(vbid);
            itemsFlushed += count;
        } while (moreAvailable);
        return itemsFlushed;
    }

    Store store;
};

/*
 * Fixture for CheckpointManager benchmarks
 */
class CheckpointBench : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        // Allow many checkpoints
        varConfig =
                "max_size=1000000000;max_checkpoints=100000000;chk_max_items=1";

        EngineFixture::SetUp(state);
        if (state.thread_index == 0) {
            engine->getKVBucket()->setVBucketState(Vbid(0),
                                                   vbucket_state_active);
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            engine->getKVBucket()->deleteVBucket(vbid, this);
        }
        EngineFixture::TearDown(state);
    }
};

/**
 * Benchmark queueing items into a vBucket.
 * Items have a 10% chance of being a duplicate key of a previous item (to
 * model de-dupe).
 */
BENCHMARK_DEFINE_F(VBucketBench, QueueDirty)
(benchmark::State& state) {
    const auto itemCount = state.range(1);

    std::default_random_engine gen;
    auto makeKeyWithDuplicates = [&gen](int i) {
        // 10% of the time; return a key which is the same as a previous one.
        std::uniform_real_distribution<> dis(0, 1.0);
        if (dis(gen) < 0.1) {
            return std::string("key") + std::to_string((i + 1) / 2);
        } else {
            return std::string("key") + std::to_string(i);
        }
    };

    int itemsQueuedTotal = 0;

    // Pre-size the VBucket's hashtable to a sensible size.
    auto* vb = engine->getKVBucket()->getVBucket(vbid).get();
    vb->ht.resize(itemCount);

    const std::string value(1, 'x');
    while (state.KeepRunning()) {
        // Benchmark: Add the given number of items to checkpoint manager.
        // Note we don't include the time taken to make the item.
        for (int i = 0; i < itemCount; ++i) {
            state.PauseTiming();
            const auto key = makeKeyWithDuplicates(i);
            auto item = make_item(vbid, key, value);
            state.ResumeTiming();
            ASSERT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(item, cookie));
            ++itemsQueuedTotal;
        }

        state.PauseTiming();
        /// Cleanup VBucket
        vb->ht.clear();
        vb->checkpointManager->clear(*vb, 0);
        state.ResumeTiming();
    }

    state.SetItemsProcessed(itemsQueuedTotal);
}

BENCHMARK_DEFINE_F(VBucketBench, FlushVBucket)
(benchmark::State& state) {
    const auto itemCount = state.range(1);
    int itemsFlushedTotal = 0;

    // Pre-size the VBucket's hashtable so a sensible size.
    engine->getKVBucket()->getVBucket(vbid)->ht.resize(itemCount);

    while (state.KeepRunning()) {
        // Add the given number of items to checkpoint manager.
        state.PauseTiming();
        std::string value(1, 'x');
        for (int i = 0; i < itemCount; ++i) {
            auto item = make_item(
                    vbid, std::string("key") + std::to_string(i), value);
            ASSERT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(item, cookie));
        }

        state.ResumeTiming();

        // Benchmark.
        size_t itemsFlushed = flushAllItems(vbid);

        ASSERT_EQ(itemCount, itemsFlushed);

        itemsFlushedTotal += itemsFlushed;
    }
    state.SetItemsProcessed(itemsFlushedTotal);
    state.SetLabel(std::string("store:" + to_string(store)).c_str());
}

BENCHMARK_DEFINE_F(VBucketBench, CreateDeleteStoredValue)
(benchmark::State& state) {
    auto factory = std::make_unique<StoredValueFactory>(engine->getEpStats());

    const std::string value(1, 'x');
    while (state.KeepRunning()) {
        auto item = make_item(vbid, "key", value);
        benchmark::DoNotOptimize(item);
        auto sv = (*factory)(item, nullptr);
        benchmark::DoNotOptimize(sv);
    }
}

/*
 * MB-31834: Load throughput degradation when the number of checkpoints
 * eligible for removing is high.
 * At both checkpoint-removing and CM:queueDirty we acquire the CM::queueLock.
 * If the number of checkpoints eligible for removing is high, then any slow
 * operation under lock in CheckpointRemover delays frontend operations.
 * Note that the CheckpointRemover is O(N) in the size of the
 * CM::checkpointList. The regression is caused by a change in MB-30916 where we
 * started with deallocating checkpoint memory under lock.
 *
 * This benchmark measures resource contention between a mc:worker (frontend
 * thread) executing CM::queueDirty and the ClosedUnrefCheckpointRemoverTask
 * when the number of checkpoint eligible for removing is high.
 */
BENCHMARK_DEFINE_F(CheckpointBench, QueueDirtyWithManyClosedUnrefCheckpoints)
(benchmark::State& state) {
    // Test approach:
    // - Fix the number of checkpoints to be removed and run the
    //     CheckpointRemover in a background thread.
    // - Fix the number of checkpoint to be removed at each CheckpointRemover
    //     run (must be in the order of 10^3 to catch the regression scenario).
    // - Enqueue items into the CheckpointMaanager in the frontend thread.
    //     Break when the CheckpointRemover has done. Measure (A) the number
    //     of items enqueued and (B) the runtime of the frontend thread.
    // - Output the average runtime of frontend operations (B/A), which is the
    //     measured metric for this benchmark

    ASSERT_EQ(1, state.max_iterations);

    const size_t numCheckpoints = state.range(0);
    const size_t numCkptToRemovePerIteration = state.range(1);

    auto* vb = engine->getKVBucket()->getVBucket(vbid).get();
    auto* ckptMgr = vb->checkpointManager.get();

    // Same queued_item used for both checkpointList pre-filling and
    // front-end queueDirty().
    // Note that we will generate many 1-item checkpoints even if we enqueue
    // always the same identical item. That is because we have 'chk_max_items=1'
    // in configuration, which leads to the following order of steps at every
    // call to CM::queueDirty:
    // 1) close the open checkpoint
    // 2) create a new open checkpoint
    // 3) enqueue the new mutation (note that de-duplication happens here).
    //     The new mutation will be inserted into the /new/ (empty) open
    //     checkpoint. So, there will be no de-duplication.
    queued_item qi{
            new Item(StoredDocKey(std::string("key"), CollectionID::Default),
                     vbid,
                     queue_op::mutation,
                     /*revSeq*/ 0,
                     /*bySeq*/ 0)};

    // Pre-fill CM with the defined number of checkpoints
    for (size_t i = 0; i < numCheckpoints; ++i) {
        ckptMgr->queueDirty(*vb,
                            qi,
                            GenerateBySeqno::Yes,
                            GenerateCas::Yes,
                            /*preLinkDocCtx*/ nullptr);
    }

    // Simulate the Flusher, this makes all checkpoints eligible for removing
    std::vector<queued_item> items;
    ckptMgr->getAllItemsForPersistence(items);
    ckptMgr->itemsPersisted();

    ThreadGate tg(2);

    // Note: numUnrefItems is also the number of removed checkpoints as
    //     we have 1 item per checkpoint.
    size_t numUnrefItems = 0;
    size_t numCkptRemoverRuns = 0;
    std::atomic<bool> bgDone{false};
    auto removeCkpt = [&tg,
                       ckptMgr,
                       vb,
                       numCkptToRemovePerIteration,
                       &numUnrefItems,
                       &numCheckpoints,
                       &numCkptRemoverRuns,
                       &bgDone]() {
        tg.threadUp();
        bool newOpenCheckpointCreated;
        while (true) {
            auto removed = ckptMgr->removeClosedUnrefCheckpoints(
                    *vb, newOpenCheckpointCreated, numCkptToRemovePerIteration);
            numUnrefItems += removed;
            numCkptRemoverRuns++;

            if (numUnrefItems >= numCheckpoints) {
                break;
            }
        }
        // Done, exit frontend thread
        bgDone = true;
    };

    // Note: thread started but still blocked on ThreadGate
    std::thread bgThread(removeCkpt);

    size_t itemsQueuedTotal = 0;
    size_t runtime = 0;
    while (state.KeepRunning()) {
        tg.threadUp();
        auto begin = std::chrono::steady_clock::now();
        while (!bgDone) {
            ckptMgr->queueDirty(*vb,
                                qi,
                                GenerateBySeqno::Yes,
                                GenerateCas::Yes,
                                /*preLinkDocCtx*/ nullptr);
            itemsQueuedTotal++;
        }

        runtime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now() - begin)
                          .count();
    }
    ASSERT_TRUE(itemsQueuedTotal);

    state.counters["NumCheckpointsRemoverRuns"] = numCkptRemoverRuns;
    state.counters["NumCheckpointsRemovedPerIteration"] =
            numUnrefItems / numCkptRemoverRuns;
    state.counters["ItemsEnqueued"] = itemsQueuedTotal;
    // Clang-scan-build complains about a possible division on 0.. guess
    // it doesn't know that the ASSERT_TRUE above would terminate the method
    if (itemsQueuedTotal > 0) {
        state.counters["AvgQueueDirtyRuntime"] = runtime / itemsQueuedTotal;
    }

    bgThread.join();
}

// Run with item counts from 1..10,000,000.
BENCHMARK_REGISTER_F(VBucketBench, QueueDirty)
        ->Args({1})
        ->Args({100})
        ->Args({10000})
        ->Args({1000000});

static void FlushArguments(benchmark::internal::Benchmark* b) {
    // Add both couchstore (0) and rocksdb (1) variants for a range of sizes.
    for (size_t items = 1; items <= 1000000; items *= 100) {
        b->ArgPair(0, items);
        b->ArgPair(1, items);
    }
}

BENCHMARK_REGISTER_F(VBucketBench, FlushVBucket)->Apply(FlushArguments);

BENCHMARK_REGISTER_F(CheckpointBench, QueueDirtyWithManyClosedUnrefCheckpoints)
        ->Args({1000000, 1000})
        ->Iterations(1);
