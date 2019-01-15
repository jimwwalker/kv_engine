/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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
 * Unit tests for the EPBucket class.
 */

#pragma once

#include "evp_store_test.h"
#include "fakes/fake_executorpool.h"

struct dcp_message_producers;
class MockActiveStreamWithOverloadedRegisterCursor;
class MockDcpMessageProducers;
class MockDcpProducer;

/*
 * A subclass of KVBucketTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class SingleThreadedKVBucketTest : public KVBucketTest {
public:
    /*
     * Run the next task from the taskQ
     * The task must match the expectedTaskName parameter
     */
    std::chrono::steady_clock::time_point runNextTask(
            TaskQueue& taskQ, const std::string& expectedTaskName);

    /*
     * Run the next task from the taskQ
     */
    std::chrono::steady_clock::time_point runNextTask(TaskQueue& taskQ);

    /*
     * DCP helper. Create a MockDcpProducer configured with (or without)
     * collections and/or delete_times enabled
     * @param cookie cookie to associate with the new producer
     * @param deleteTime yes/no - enable/disable delete times
     */
    std::shared_ptr<MockDcpProducer> createDcpProducer(
            const void* cookie,
            IncludeDeleteTime deleteTime);

    /*
     * DCP helper.
     * Notify and step the given producer
     * @param expectedOp once stepped we expect to see this DCP opcode produced
     * @param fromMemory if false then step a backfill
     */
    void notifyAndStepToCheckpoint(
            MockDcpProducer& producer,
            MockDcpMessageProducers& producers,
            cb::mcbp::ClientOpcode expectedOp =
                    cb::mcbp::ClientOpcode::DcpSnapshotMarker,
            bool fromMemory = true);

    /*
     * DCP helper.
     * Run the active-checkpoint processor task for the given producer
     * @param producer The producer whose task will be ran
     * @param producers The dcp callbacks
     */
    void runCheckpointProcessor(MockDcpProducer& producer,
                                dcp_message_producers& producers);

    /**
     * Create a DCP stream on the producer for this->vbid
     */
    void createDcpStream(MockDcpProducer& producer);

    /**
     * Create a DCP stream on the producer for vbid
     */
    void createDcpStream(MockDcpProducer& producer, Vbid vbid);

    /**
     * Run the compaction task
     * @param purgeBeforeTime purge tombstones with timestamps less than this
     * @param purgeBeforeSeq purge tombstones with seqnos less than this
     */
    void runCompaction(uint64_t purgeBeforeTime = 0,
                       uint64_t purgeBeforeSeq = 0);

    /**
     * Run the task responsible for iterating the documents and erasing them
     * For persistent buckets integrated into compaction.
     * For ephemeral buckets integrated into stale item removal task
     */
    void runCollectionsEraser();

protected:
    void SetUp() override;

    void TearDown() override;

    /*
     * Change the vbucket state, and run the VBStatePeristTask (if necessary
     * for this bucket type).
     * On return the state will be changed and the task completed.
     */
    void setVBucketStateAndRunPersistTask(Vbid vbid, vbucket_state_t newState);

    /*
     * Set the stats isShutdown and attempt to drive all tasks to cancel for
     * the specified engine.
     */
    void shutdownAndPurgeTasks(EventuallyPersistentEngine* ep);

    void cancelAndPurgeTasks();

    /**
     * This method will keep running reader tasks until the engine shows warmup
     * is complete.
     */
    void runReadersUntilWarmedUp();

    /**
     * Destroy engine and replace it with a new engine that can be warmed up.
     * Finally, run warmup.
     */
    void resetEngineAndWarmup(std::string new_config = "");

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    }

    SingleThreadedExecutorPool* task_executor;
};

/**
 * Test fixture for single-threaded tests on EPBucket.
 */
class SingleThreadedEPBucketTest : public SingleThreadedKVBucketTest {
public:
    enum class BackfillBufferLimit { StreamByte, StreamItem, ConnectionByte };

    void backfillExpiryOutput(bool xattr);
    void producerReadyQLimitOnBackfill(BackfillBufferLimit limitType);

protected:
    EPBucket& getEPBucket() {
        return dynamic_cast<EPBucket&>(*store);
    }
};

/**
 * Test fixture for KVBucket tests running in single-threaded mode.
 *
 * Parameterised on a pair of:
 * - bucket_type (ephemeral of persistent)
 * - eviction type (for specifying ephemeral auto-delete & fail_new_data
 *   eviction modes). If empty then unused (persistent buckets).
 */
class STParameterizedBucketTest
    : virtual public SingleThreadedKVBucketTest,
      public ::testing::WithParamInterface<
              std::tuple<std::string, std::string>> {
public:
    bool persistent() const {
        return std::get<0>(GetParam()) == "persistent";
    }

    bool isFullEviction() const {
        return persistent() && std::get<1>(GetParam()) == "full_eviction";
    }

protected:
    void SetUp() {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string += "bucket_type=" + std::get<0>(GetParam());
        auto evictionPolicy = std::get<1>(GetParam());

        if (!evictionPolicy.empty()) {
            if (persistent()) {
                config_string += ";item_eviction_policy=" + evictionPolicy;
            } else {
                config_string += ";ephemeral_full_policy=" + evictionPolicy;

            }
        }

        SingleThreadedKVBucketTest::SetUp();
    }
};

struct STParameterizedBucketTestPrintName {
    std::string operator() (const ::testing::TestParamInfo<
                  ::testing::tuple<std::string, std::string>>&) const;
};
