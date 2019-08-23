/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_ep_bucket.h"
#include "../mock/mock_item_freq_decayer.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_manager.h"
#include "dcp/response.h"
#include "durability/durability_monitor.h"
#include "ep_time.h"
#include "evp_store_durability_test.h"
#include "evp_store_single_threaded_test.h"
#include "kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
#include "vbucket_state.h"
#include "warmup.h"

class WarmupTest : public SingleThreadedKVBucketTest {};

// Test that the FreqSaturatedCallback of a vbucket is initialized and after
// warmup is set to the "wakeup" function of ItemFreqDecayerTask.
TEST_F(WarmupTest, setFreqSaturatedCallback) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // The FreqSaturatedCallback should be initialised
    {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        EXPECT_TRUE(vb->ht.getFreqSaturatedCallback());
    }
    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);
    rewriteCouchstoreVBState(vbid, test_dbname, 1);

    // Resetting the engine and running warmup will result in the
    // Warmup::createVBuckets being invoked for vbid.
    resetEngineAndWarmup();

    dynamic_cast<MockEPBucket*>(store)->createItemFreqDecayerTask();
    auto itemFreqTask =
            dynamic_cast<MockEPBucket*>(store)->getMockItemFreqDecayerTask();
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    // The FreqSaturatedCallback should be initialised
    EXPECT_TRUE(vb->ht.getFreqSaturatedCallback());
    ASSERT_FALSE(itemFreqTask->wakeupCalled);
    // We now invoke the FreqSaturatedCallback function.
    vb->ht.getFreqSaturatedCallback()();
    // This should have resulted in calling the wakeup function of the
    // MockItemFreqDecayerTask.
    EXPECT_TRUE(itemFreqTask->wakeupCalled);
}

TEST_F(WarmupTest, hlcEpoch) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);
    rewriteCouchstoreVBState(vbid, test_dbname, 1);

    resetEngineAndWarmup();

    {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        // We've warmed up from a down-level vbstate, so expect epoch to be
        // HlcCasSeqnoUninitialised
        ASSERT_EQ(HlcCasSeqnoUninitialised, vb->getHLCEpochSeqno());

        // Store a new key, the flush will change hlc_epoch to be the next seqno
        // (2)
        store_item(vbid, makeStoredDocKey("key2"), "value");
        flush_vbucket_to_disk(vbid);

        EXPECT_EQ(2, vb->getHLCEpochSeqno());

        // Store a 3rd item
        store_item(vbid, makeStoredDocKey("key3"), "value");
        flush_vbucket_to_disk(vbid);
    }

    // Warmup again, hlcEpoch will still be 2
    resetEngineAndWarmup();
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(2, vb->getHLCEpochSeqno());

    // key1 stored before we established the epoch should have cas_is_hlc==false
    auto item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item1.getStatus());
    auto info1 = engine->getItemInfo(*item1.item);
    EXPECT_FALSE(info1.cas_is_hlc);

    // key2 should have a CAS generated from the HLC
    auto item2 = store->get(makeStoredDocKey("key2"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item2.getStatus());
    auto info2 = engine->getItemInfo(*item2.item);
    EXPECT_TRUE(info2.cas_is_hlc);
}

TEST_F(WarmupTest, fetchDocInDifferentCompressionModes) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    std::string valueData(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");

    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), valueData);
    flush_vbucket_to_disk(vbid);

    resetEngineAndWarmup("compression_mode=off");
    auto item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item1.getStatus());
    auto info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, info1.datatype);

    resetEngineAndWarmup("compression_mode=passive");
    item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item1.getStatus());
    info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ((PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              info1.datatype);

    resetEngineAndWarmup("compression_mode=active");
    item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item1.getStatus());
    info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ((PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              info1.datatype);
}

TEST_F(WarmupTest, mightContainXattrs) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);
    rewriteCouchstoreVBState(vbid, test_dbname, 1);

    resetEngineAndWarmup();
    {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        EXPECT_FALSE(vb->mightContainXattrs());

        auto xattr_data = createXattrValue("value");

        auto itm = store_item(vbid,
                              makeStoredDocKey("key"),
                              xattr_data,
                              1,
                              {cb::engine_errc::success},
                              PROTOCOL_BINARY_DATATYPE_XATTR);

        EXPECT_TRUE(vb->mightContainXattrs());

        flush_vbucket_to_disk(vbid);
    }
    // Warmup - we should have xattr dirty
    resetEngineAndWarmup();

    EXPECT_TRUE(engine->getKVBucket()->getVBucket(vbid)->mightContainXattrs());
}

/**
 * Performs the following operations
 * 1. Store an item
 * 2. Delete the item
 * 3. Recreate the item
 * 4. Perform a warmup
 * 5. Get meta data of the key to verify the revision seq no is
 *    equal to number of updates on it
 */
TEST_F(WarmupTest, MB_27162) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto key = makeStoredDocKey("key");

    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);

    delete_item(vbid, key);
    flush_vbucket_to_disk(vbid);

    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);

    resetEngineAndWarmup();

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto doGetMetaData = [&]() {
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
    };
    auto engineResult = doGetMetaData();

    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    EXPECT_EQ(3, itemMeta.revSeqno);
}

// MB-25197 and MB-34422
// Some operations must block until warmup has loaded the vbuckets
TEST_F(WarmupTest, OperationsInterlockedWithWarmup) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);

    resetEngineAndEnableWarmup();

    // Manually run the reader queue so that the warmup tasks execute whilst we
    // perform the interlocked operations
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    EXPECT_EQ(nullptr, store->getVBuckets().getBucket(vbid));
    const void* setVBStateCookie = create_mock_cookie();
    const void* getFailoverCookie = create_mock_cookie();
    const void* statsCookie1 = create_mock_cookie();
    const void* statsCookie2 = create_mock_cookie();
    const void* statsCookie3 = create_mock_cookie();
    const void* delVbCookie = create_mock_cookie();

    std::unordered_map<const void*, int> notifications;
    notifications[setVBStateCookie] =
            get_number_of_mock_cookie_io_notifications(setVBStateCookie);
    notifications[setVBStateCookie] =
            get_number_of_mock_cookie_io_notifications(setVBStateCookie);
    notifications[getFailoverCookie] =
            get_number_of_mock_cookie_io_notifications(getFailoverCookie);
    notifications[getFailoverCookie] =
            get_number_of_mock_cookie_io_notifications(getFailoverCookie);
    notifications[statsCookie1] =
            get_number_of_mock_cookie_io_notifications(statsCookie1);
    notifications[statsCookie2] =
            get_number_of_mock_cookie_io_notifications(statsCookie2);
    notifications[statsCookie3] =
            get_number_of_mock_cookie_io_notifications(statsCookie3);
    notifications[delVbCookie] = get_number_of_mock_cookie_io_notifications(delVbCookie);

    auto dummyAddStats = [](const char*,
                            const uint16_t,
                            const char*,
                            const uint32_t,
                            gsl::not_null<const void*>) {

    };

    while (engine->getKVBucket()->maybeWaitForVBucketWarmup(cookie)) {
        CheckedExecutor executor(task_executor, readerQueue);
        // Do a setVBState but don't flush it through. This call should be
        // failed ewouldblock whilst warmup has yet to attempt to create VBs.
        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  store->setVBucketState(vbid,
                                         vbucket_state_active,
                                         {},
                                         TransferVB::No,
                                         setVBStateCookie));

        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  engine->get_failover_log(getFailoverCookie,
                                           1 /*opaque*/,
                                           vbid,
                                           fakeDcpAddFailoverLog));

        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  engine->get_stats(statsCookie1, "vbucket", dummyAddStats));

        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  engine->get_stats(
                          statsCookie2, "vbucket-details", dummyAddStats));

        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  engine->get_stats(
                          statsCookie3, "vbucket-seqno", dummyAddStats));

        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  engine->deleteVBucket(vbid, true, delVbCookie));

        executor.runCurrentTask();
    }

    for (const auto& n : notifications) {
        EXPECT_GT(get_number_of_mock_cookie_io_notifications(n.first),
                  n.second);
    }

    EXPECT_NE(nullptr, store->getVBuckets().getBucket(vbid));

    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid,
                                     vbucket_state_active,
                                     {},
                                     TransferVB::No,
                                     setVBStateCookie));

    EXPECT_EQ(ENGINE_SUCCESS,
              engine->get_failover_log(getFailoverCookie,
                                       1 /*opaque*/,
                                       vbid,
                                       fakeDcpAddFailoverLog));

    EXPECT_EQ(ENGINE_SUCCESS,
              engine->get_stats(statsCookie1, "vbucket", dummyAddStats));

    EXPECT_EQ(
            ENGINE_SUCCESS,
            engine->get_stats(statsCookie2, "vbucket-details", dummyAddStats));

    EXPECT_EQ(ENGINE_SUCCESS,
              engine->get_stats(statsCookie3, "vbucket-seqno", dummyAddStats));

    EXPECT_EQ(ENGINE_SUCCESS, engine->deleteVBucket(vbid, false, delVbCookie));

    // finish warmup so the test can exit
    while (engine->getKVBucket()->isWarmingUp()) {
        CheckedExecutor executor(task_executor, readerQueue);
        executor.runCurrentTask();
    }

    for (const auto& n : notifications) {
        destroy_mock_cookie(n.first);
    }
}

/**
 * WarmupTest.MB_32577
 *
 * This test checks that we do not open DCP consumer connections until
 * warmup has finished. To prevent a race condition where a DCP deletion request
 * can be received for a replica and flushed to disk before the vbucket has
 * been fully initialisation.
 *
 * The test if performed by the following steps:
 * 1. Create a replica vbucket with a document in
 * 2. Warm up to till the point WarmupState::State::LoadingCollectionCounts
 *    at this point stop warmup and continue (we should be part warmed up)
 * 3. Open a DCP connection to the replica vbucket (this should fail with
 *    ENGINE_TMPFAIL as the vbucket is not warmed up)
 * 4. Send a DCP deletion for the document to the vbucket (this should fail with
 *    ENGINE_DISCONNECT as there shouldn't be a DCP connection open)
 * 5. Try and flush the vbucket, nothing should be flushed as the deletion
 *    should have failed
 * 6. Finish warming up the vbucket
 * 7. repeat steps 3, 4 and 5 which should now return ENGINE_SUCCESS and
 *    the item should be deleted from disk as we are know fully warmed up.
 */
TEST_F(WarmupTest, MB_32577) {
    std::string keyName("key");
    std::string value("value");
    nlohmann::json metaStateChange{};
    uint32_t zeroFlags{0};

    // create an active vbucket
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item to the vbucket
    store_item(vbid, makeStoredDocKey(keyName), value);
    // Change the type of vbucket to a replica
    store->setVBucketState(
            vbid, vbucket_state_replica, metaStateChange, TransferVB::Yes);
    // flush all document to disk
    flush_vbucket_to_disk(vbid);

    // check that the item has been recoded by collections
    ASSERT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionID::Default));

    // shutdown memcached
    shutdownAndPurgeTasks(engine.get());

    // reinitialise memcached and set everything up for warm up
    reinitialise("");
    if (engine->getConfiguration().getBucketType() == "persistent") {
        static_cast<EPBucket*>(engine->getKVBucket())->initializeWarmupTask();
        static_cast<EPBucket*>(engine->getKVBucket())->startWarmupTask();
    } else {
        FAIL() << "Should not reach here - persistent buckets only. type:"
               << engine->getConfiguration().getBucketType();
    }

    // get hold of a pointer to the WarmUp object
    auto* warmupPtr = store->getWarmup();

    // Run though all the warmup tasks till LoadingCollectionCounts task
    // at this point we want to stop as this is when we want to send a delete
    // request using DCP
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    while (store->isWarmingUp()) {
        if (warmupPtr->getWarmupState() ==
            WarmupState::State::LoadingCollectionCounts) {
            break;
        }
        runNextTask(readerQueue);
    }

    // Try and set a DCP connection, this should return ENGINE_TMPFAIL
    // as were in warm up but without the fix for MB-32577 this will return
    // ENGINE_SUCCESS
    EXPECT_EQ(ENGINE_TMPFAIL,
              engine->open(cookie, 0, 0 /*seqno*/, zeroFlags, "test_consumer"));

    // create a stream to send the delete request so we can send vbucket on the
    // consumer
    EXPECT_EQ(ENGINE_DISCONNECT, engine->add_stream(cookie, 0, vbid, 0));

    // create snapshot so we can delete the document
    EXPECT_EQ(ENGINE_DISCONNECT,
              engine->snapshot_marker(cookie,
                                      /*opaque*/ 1,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ 100,
                                      zeroFlags,
                                      /*HCS*/ {}));

    // create a DocKey object for the delete request
    const DocKey docKey{reinterpret_cast<const uint8_t*>(keyName.data()),
                        keyName.size(),
                        DocKeyEncodesCollectionId::No};
    // Try and delete the doc
    EXPECT_EQ(ENGINE_DISCONNECT,
              engine->deletion(cookie,
                               /*opaque*/ 1,
                               /*key*/ docKey,
                               /*value*/ {},
                               /*priv_bytes*/ 0,
                               /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
                               /*cas*/ 0,
                               /*vbucket*/ vbid,
                               /*bySeqno*/ 2,
                               /*revSeqno*/ 0,
                               /*meta*/ {}));
    // Get the engine to flush the delete to disk, this will cause an
    // underflow exception if the deletion request was successful
    EXPECT_NO_THROW(dynamic_cast<EPBucket&>(*store).flushVBucket(vbid));

    // finish warmup so we can check the number of items in the engine
    while (store->isWarmingUp()) {
        runNextTask(readerQueue);
    }

    // We shouldn't have deleted the doc and thus it should still be there
    EXPECT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionID::Default));

    // Try and set a DCP connection, this should return ENGINE_SUCCESS
    // as we have now warmed up.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->open(cookie, 0, 0 /*seqno*/, zeroFlags, "test_consumer"));

    // create a stream to send the delete request so we can send vbucket on the
    // consumer
    EXPECT_EQ(ENGINE_SUCCESS, engine->add_stream(cookie, 0, vbid, 0));

    // create snapshot so we can delete the document
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->snapshot_marker(cookie,
                                      /*opaque*/ 1,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ 100,
                                      zeroFlags,
                                      /*HCS*/ {}));

    // Try and delete the doc
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->deletion(cookie,
                               /*opaque*/ 1,
                               /*key*/ docKey,
                               /*value*/ {},
                               /*priv_bytes*/ 0,
                               /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
                               /*cas*/ 0,
                               /*vbucket*/ vbid,
                               /*bySeqno*/ 2,
                               /*revSeqno*/ 0,
                               /*meta*/ {}));

    // flush delete to disk
    EXPECT_NO_THROW(dynamic_cast<EPBucket&>(*store).flushVBucket(vbid));

    // We shouldn't have deleted the doc and thus it should still be there
    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionID::Default));

    // Close stream before deleting the connection
    engine->handleDisconnect(cookie);

    shutdownAndPurgeTasks(engine.get());
}

class WarmupVbState : public WarmupTest {};

// Demonstrate vbstate {"checkpoint_id" : n}
TEST_F(WarmupVbState, CheckpointId) {
    auto active = vbid;
    Vbid replica(vbid.get() + 1);

    // create an active and replica vbucket, don't flush
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(active, vbucket_state_active));
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(replica, vbucket_state_replica));

    auto check = [this, active, replica](uint64_t activeId,
                                         uint64_t replicaId) {
        auto lastPersisted = store->getLastPersistedCheckpointId(active);
        EXPECT_EQ(activeId, lastPersisted.first);
        EXPECT_TRUE(lastPersisted.second);
        lastPersisted = store->getLastPersistedCheckpointId(replica);
        EXPECT_EQ(replicaId, lastPersisted.first);
        EXPECT_TRUE(lastPersisted.second);
    };

    SCOPED_TRACE("new vbuckets, nothing flushed");
    check(0, 0);

    // flush vb states
    flush_vbucket_to_disk(active);
    flush_vbucket_to_disk(replica);

    SCOPED_TRACE("new vbuckets, vbstate flushed");
    check(0, 0);

    // Store an item to the vbucket
    store_item(vbid, makeStoredDocKey("key"), "value");
    // flush all document to disk
    flush_vbucket_to_disk(active);
    SCOPED_TRACE("active has 1 item");
    check(0, 0);

    resetEngineAndWarmup();

    SCOPED_TRACE("post warmup 1");
    check(0, 0);
}

// Test fixture for Durability-related Warmup tests.
class DurabilityWarmupTest : public DurabilityKVBucketTest {
protected:
    // Test that a pending SyncWrite/Delete not yet committed is correctly
    // warmed up when the bucket restarts.
    void testPendingSyncWrite(vbucket_state_t vbState,
                              const std::vector<std::string>& keys,
                              DocumentState docState);

    // Test that a pending SyncWrite/Delete which was committed is correctly
    // warmed up when the bucket restarts (as a Committed item).
    void testCommittedSyncWrite(vbucket_state_t vbState,
                                const std::vector<std::string>& keys,
                                DocumentState docState);

    // Test that a committed mutation followed by a pending SyncWrite to the
    // same key is correctly warmed up when the bucket restarts.
    void testCommittedAndPendingSyncWrite(vbucket_state_t vbState,
                                          DocumentState docState);

    // Helper method - fetches the given Item via engine->get(); issuing a
    // BG fetch and second get() if necessary.
    GetValue getItemFetchFromDiskIfNeeded(const DocKey& key,
                                          DocumentState docState);

    enum class Resolution : uint8_t { Commit, Abort };

    /**
     * Test that when we complete a Prepare the correct HCS is persisted into
     * the local document.
     */
    void testHCSPersistedAndLoadedIntoVBState();

    class PrePostStateChecker {
    public:
        PrePostStateChecker(VBucketPtr vb);
        ~PrePostStateChecker();

        void setVBucket(VBucketPtr vb) {
            this->vb = vb;
        }

        // Checker can be disabled if the test is doing something special, e.g.
        // driving the ADM directly
        void disable() {
            disabled = true;
        }

    private:
        VBucketPtr vb;
        bool disabled = false;
        int64_t preHPS = 0;
        int64_t preHCS = 0;
    };

    PrePostStateChecker resetEngineAndWarmup();
};

DurabilityWarmupTest::PrePostStateChecker::PrePostStateChecker(VBucketPtr vb) {
    EXPECT_TRUE(vb);
    preHPS = vb->getHighPreparedSeqno();
    preHCS = vb->getHighCompletedSeqno();
}

DurabilityWarmupTest::PrePostStateChecker::~PrePostStateChecker() {
    if (disabled) {
        return;
    }

    EXPECT_TRUE(vb);
    EXPECT_EQ(preHPS, vb->getHighPreparedSeqno())
            << "PrePostStateChecker: Found that post warmup the HPS does not "
               "match the pre-warmup value";
    EXPECT_EQ(preHCS, vb->getHighCompletedSeqno())
            << "PrePostStateChecker: Found that post warmup the HCS does not "
               "match the pre-warmup value";
}

DurabilityWarmupTest::PrePostStateChecker
DurabilityWarmupTest::resetEngineAndWarmup() {
    PrePostStateChecker checker(engine->getVBucket(vbid));
    DurabilityKVBucketTest::resetEngineAndWarmup();
    checker.setVBucket(engine->getVBucket(vbid));
    return checker;
}

GetValue DurabilityWarmupTest::getItemFetchFromDiskIfNeeded(
        const DocKey& key, DocumentState docState) {
    const auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto gv = engine->getKVBucket()->get(key, vbid, cookie, options);
    if (docState == DocumentState::Deleted) {
        // Need an extra bgFetch to get a deleted item.
        EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());
        runBGFetcherTask();
        gv = engine->getKVBucket()->get(key, vbid, cookie, options);
    }
    return gv;
}

void DurabilityWarmupTest::testPendingSyncWrite(
        vbucket_state_t vbState,
        const std::vector<std::string>& keys,
        DocumentState docState) {
    // Store the given pending SyncWrites/Deletes (without committing) and then
    // restart

    auto vb = engine->getVBucket(vbid);
    auto numTracked = vb->getDurabilityMonitor().getNumTracked();

    for (const auto& k : keys) {
        // Previous runs could have left the VB into a non-active state - must
        // be active to perform set().
        if (vb->getState() != vbucket_state_active) {
            setVBucketToActiveWithValidTopology();
        }

        const auto key = makeStoredDocKey(k);
        auto item = makePendingItem(key, "pending_value");
        if (docState == DocumentState::Deleted) {
            item->setDeleted(DeleteSource::Explicit);
        }
        ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
        flush_vbucket_to_disk(vbid);

        // Set the state that we want to test
        if (vbState != vbucket_state_active) {
            setVBucketStateAndRunPersistTask(vbid, vbState);
        }

        // About to destroy engine; reset vb shared_ptr.
        vb.reset();
        resetEngineAndWarmup();
        vb = engine->getVBucket(vbid);

        // Check that attempts to read this key via frontend are blocked.
        auto gv = store->get(key, vbid, cookie, {});
        EXPECT_EQ(ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS, gv.getStatus());

        // Check that the item is still pending with the correct CAS.
        auto handle = vb->lockCollections(item->getKey());
        auto prepared = vb->fetchPreparedValue(handle);
        EXPECT_TRUE(prepared.storedValue);
        EXPECT_TRUE(prepared.storedValue->isPending());
        EXPECT_EQ(item->isDeleted(), prepared.storedValue->isDeleted());
        EXPECT_EQ(item->getCas(), prepared.storedValue->getCas());

        // DurabilityMonitor be tracking the prepare.
        EXPECT_EQ(++numTracked, vb->getDurabilityMonitor().getNumTracked());

        EXPECT_EQ(numTracked,
                  store->getEPEngine().getEpStats().warmedUpPrepares);
        EXPECT_EQ(numTracked,
                  store->getEPEngine()
                          .getEpStats()
                          .warmupItemsVisitedWhilstLoadingPrepares);
    }
}

TEST_P(DurabilityWarmupTest, ActivePendingSyncWrite) {
    testPendingSyncWrite(vbucket_state_active,
                         {"key1", "key2", "key3"},
                         DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ActivePendingSyncDelete) {
    testPendingSyncWrite(vbucket_state_active,
                         {"key1", "key2", "key3"},
                         DocumentState::Deleted);
}

TEST_P(DurabilityWarmupTest, ReplicaPendingSyncWrite) {
    testPendingSyncWrite(vbucket_state_replica,
                         {"key1", "key2", "key3"},
                         DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ReplicaPendingSyncDelete) {
    testPendingSyncWrite(vbucket_state_replica,
                         {"key1", "key2", "key3"},
                         DocumentState::Deleted);
}
void DurabilityWarmupTest::testCommittedSyncWrite(
        vbucket_state_t vbState,
        const std::vector<std::string>& keys,
        DocumentState docState) {
    // Prepare
    testPendingSyncWrite(vbState, keys, docState);

    auto vb = engine->getVBucket(vbid);
    auto numTracked = vb->getDurabilityMonitor().getNumTracked();
    ASSERT_EQ(keys.size(), numTracked);

    auto prepareSeqno = 1;
    for (const auto& k : keys) {
        // Commit
        const auto key = makeStoredDocKey(k);
        if (vbState == vbucket_state_active) {
            // Commit on active is driven by the ADM so we need to drive our
            // commit via seqno ack
            EXPECT_EQ(ENGINE_SUCCESS,
                      vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(
                                                    vb->getStateLock()),
                                            "replica",
                                            prepareSeqno++));
            vb->processResolvedSyncWrites();
        } else {
            // Commit on non-active is driven by VBucket::commit
            vb->commit(key, prepareSeqno++, {}, vb->lockCollections(key));
        }

        flush_vbucket_to_disk(vbid, 1);

        if (vbState != vbucket_state_active) {
            setVBucketStateAndRunPersistTask(vbid, vbState);
        }

        const auto expectedItem = getItemFetchFromDiskIfNeeded(key, docState);
        ASSERT_EQ(ENGINE_SUCCESS, expectedItem.getStatus());

        // About to destroy engine; reset vb shared_ptr.
        vb.reset();
        resetEngineAndWarmup();
        vb = engine->getVBucket(vbid);

        // Check that the item is CommittedviaPrepare.
        GetValue gv = getItemFetchFromDiskIfNeeded(key, docState);
        EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
        EXPECT_EQ(CommittedState::CommittedViaPrepare, gv.item->getCommitted());
        EXPECT_EQ(*expectedItem.item, *gv.item);

        // DurabilityMonitor should be empty as no outstanding prepares.
        EXPECT_EQ(--numTracked, vb->getDurabilityMonitor().getNumTracked());

        EXPECT_EQ(numTracked,
                  store->getEPEngine().getEpStats().warmedUpPrepares);
        EXPECT_EQ(numTracked,
                  store->getEPEngine()
                          .getEpStats()
                          .warmupItemsVisitedWhilstLoadingPrepares);
    }
}

TEST_P(DurabilityWarmupTest, ActiveCommittedSyncWrite) {
    testCommittedSyncWrite(vbucket_state_active,
                           {"key1", "key2", "key3"},
                           DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ActiveCommittedSyncDelete) {
    testCommittedSyncWrite(vbucket_state_active,
                           {"key1", "key2", "key3"},
                           DocumentState::Deleted);
}

TEST_P(DurabilityWarmupTest, ReplicaCommittedSyncWrite) {
    testCommittedSyncWrite(vbucket_state_replica,
                           {"key1", "key2", "key3"},
                           DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ReplicaCommittedSyncDelete) {
    testCommittedSyncWrite(vbucket_state_replica,
                           {"key1", "key2", "key3"},
                           DocumentState::Deleted);
}

void DurabilityWarmupTest::testCommittedAndPendingSyncWrite(
        vbucket_state_t vbState, DocumentState docState) {
    // Store committed mutation followed by a pending SyncWrite (without
    // committing) and then restart.
    auto key = makeStoredDocKey("key");
    auto committedItem = store_item(vbid, key, "A");
    auto item = makePendingItem(key, "B");
    if (docState == DocumentState::Deleted) {
        item->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));

    flush_vbucket_to_disk(vbid, 2);

    if (vbState != vbucket_state_active) {
        setVBucketStateAndRunPersistTask(vbid, vbState);
    }
    resetEngineAndWarmup();
    EXPECT_EQ(1, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(2,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);

    // Should load two items into memory - both committed and the pending value.
    // Check the original committed value is inaccessible due to the pending
    // needing to be re-committed.
    auto vb = engine->getVBucket(vbid);
    // @TODO: RocksDB currently only has an estimated item count in
    // full-eviction, so it fails this check. Skip if RocksDB && full_eviction.
    if ((std::get<0>(GetParam()).find("Rocksdb") == std::string::npos) ||
        std::get<0>(GetParam()) == "value_only") {
        EXPECT_EQ(1, vb->getNumTotalItems());
    }
    EXPECT_EQ(1, vb->ht.getNumPreparedSyncWrites());

    auto gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS, gv.getStatus());

    // Check that the item is still pending.
    {
        auto handle = vb->lockCollections(item->getKey());
        auto prepared = vb->fetchPreparedValue(handle);
        EXPECT_TRUE(prepared.storedValue);
        EXPECT_TRUE(prepared.storedValue->isPending());
        EXPECT_EQ(item->getCas(), prepared.storedValue->getCas());
        EXPECT_EQ("B", prepared.storedValue->getValue()->to_s());
    }

    // DurabilityMonitor be tracking the prepare.
    EXPECT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // Abort the prepare so we can validate the previous Committed value
    // is present, readable and the same it was before warmup.
    {
        auto handle = vb->lockCollections(item->getKey());
        ASSERT_EQ(ENGINE_SUCCESS,
                  vb->abort(key, item->getBySeqno(), {}, handle));
    }
    gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(committedItem, *gv.item);
}

TEST_P(DurabilityWarmupTest, ActiveCommittedAndPendingSyncWrite) {
    testCommittedAndPendingSyncWrite(vbucket_state_active,
                                     DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ActiveCommittedAndPendingSyncDelete) {
    testCommittedAndPendingSyncWrite(vbucket_state_active,
                                     DocumentState::Deleted);
}

TEST_P(DurabilityWarmupTest, ReplicaCommittedAndPendingSyncWrite) {
    testCommittedAndPendingSyncWrite(vbucket_state_replica,
                                     DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ReplicaCommittedAndPendingSyncDelete) {
    testCommittedAndPendingSyncWrite(vbucket_state_replica,
                                     DocumentState::Deleted);
}

// Negative test - check that a prepared SyncWrite which has been Aborted
// does _not_ restore the old, prepared SyncWrite after warmup.
TEST_P(DurabilityWarmupTest, AbortedSyncWritePrepareIsNotLoaded) {
    // Commit an initial value 'A', then prepare and then abort a SyncWrite of
    // "B".
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "A");
    auto item = makePendingItem(key, "B");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid, 2);

    { // scoping vb - is invalid once resetEngineAndWarmup() is called.
        auto vb = engine->getVBucket(vbid);
        EXPECT_EQ(1, vb->getNumItems());
        // Force an abort
        vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                     std::chrono::seconds(1000));
        vb->processResolvedSyncWrites();

        flush_vbucket_to_disk(vbid, 1);
        EXPECT_EQ(1, vb->getNumItems());
    }
    resetEngineAndWarmup();
    EXPECT_EQ(0, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(0,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);

    // Should load one item into memory - committed value.
    auto vb = engine->getVBucket(vbid);
    // @TODO: RocksDB currently only has an estimated item count in
    // full-eviction, so it fails this check. Skip if RocksDB && full_eviction.
    if ((std::get<0>(GetParam()).find("Rocksdb") == std::string::npos) ||
        std::get<0>(GetParam()) == "value_only") {
        EXPECT_EQ(1, vb->getNumItems());
    }
    EXPECT_EQ(0, vb->ht.getNumPreparedSyncWrites());
    auto gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ("A", gv.item->getValue()->to_s());

    // Check there's no pending item
    auto handle = vb->lockCollections(item->getKey());
    auto prepared = vb->fetchPreparedValue(handle);
    EXPECT_FALSE(prepared.storedValue);

    // DurabilityMonitor should be empty.
    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
}

// Test that not having a replication topology stored on disk (i.e. pre v6.5
// file) is correctly handled during warmup.
TEST_P(DurabilityWarmupTest, ReplicationTopologyMissing) {
    // Store an item, then make the VB appear old ready for warmup
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);

    // Remove the replicationTopology and re-persist.
    auto* kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
    auto vbstate = *kvstore->getVBucketState(vbid);
    vbstate.svb.replicationTopology.clear();
    kvstore->snapshotVBucket(
            vbid, vbstate, VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT);

    resetEngineAndWarmup();
    EXPECT_EQ(0, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(0,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);

    // Check topology is empty.
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(nlohmann::json().dump(), vb->getReplicationTopology().dump());
}

// Test that replication topology is correctly loaded from disk during warmup.
TEST_P(DurabilityWarmupTest, ReplicationTopologyLoaded) {
    // Change the replication topology to a specific value (different from
    // normal test SetUp method).
    const auto topology = nlohmann::json::array(
            {{"other_active", "other_replica", "other_replica2"}});
    setVBucketToActiveWithValidTopology(topology);

    resetEngineAndWarmup();

    // Check topology has been correctly loaded from disk.
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(topology.dump(), vb->getReplicationTopology().dump());
}

// Test that if we 'crashed' whilst committing, that the warmup will re-commit
TEST_P(DurabilityWarmupTest, WarmupCommit) {
    // Change the replication topology to a specific value (different from
    // normal test SetUp method).
    const auto topology = nlohmann::json::array({{"active"}});
    setVBucketToActiveWithValidTopology(topology);
    auto key = makeStoredDocKey("key");
    auto item = makePendingItem(key, "do");
    auto vb = store->getVBucket(vbid);
    { // collections read-lock scope
        auto cHandle = vb->lockCollections(item->getKey());
        EXPECT_TRUE(cHandle.valid());
        // Use vb level set so that the commit doesn't yet happen, we want to
        // simulate the prepare, but not commit landing on disk
        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  vb->set(*item, cookie, *engine, {}, cHandle));
    }
    flush_vbucket_to_disk(vbid, 1);
    vb.reset();

    // Now warmup, we've stored the prepare but never made it to commit
    // Because we bypassed KVBucket::set the HPS/HCS will be incorrect and fail
    // the pre/post warmup checker, so disable the checker for this test.
    resetEngineAndWarmup().disable();
    EXPECT_EQ(1, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(1,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);

    vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    vb->processResolvedSyncWrites();

    auto sv = vb->ht.findForRead(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isCommitted());
}

void DurabilityWarmupTest::testHCSPersistedAndLoadedIntoVBState() {
    // Queue a Prepare
    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*prepare, cookie));

    // Check the Prepared
    const int64_t preparedSeqno = 1;
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    const auto* sv = vb->ht.findForWrite(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isPending());
    ASSERT_EQ(preparedSeqno, sv->getBySeqno());

    // Persist the Prepare and vbstate.
    flush_vbucket_to_disk(vbid);
    auto hps1 = engine->getKVBucket()->getVBucket(vbid)->getHighPreparedSeqno();
    vb.reset();
    resetEngineAndWarmup();

    // Check hps matches the pre-warmup value
    EXPECT_EQ(hps1,
              engine->getKVBucket()->getVBucket(vbid)->getHighPreparedSeqno());

    auto checkHCS = [this](int64_t hcs) -> void {
        auto* kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
        auto vbstate = *kvstore->getVBucketState(vbid);
        ASSERT_EQ(hcs, vbstate.highCompletedSeqno);
    };

    // HCS still 0 in vbstate
    checkHCS(0);

    // Complete the Prepare
    vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      preparedSeqno));
    vb->processResolvedSyncWrites();

    sv = vb->ht.findForRead(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isCommitted());
    ASSERT_GT(sv->getBySeqno(), preparedSeqno);

    // Persist the Commit/Abort and vbstate.
    flush_vbucket_to_disk(vbid);
    checkHCS(preparedSeqno);

    vb.reset();
    resetEngineAndWarmup();

    // HCS must have been loaded from vbstate from disk
    checkHCS(preparedSeqno);
    EXPECT_EQ(preparedSeqno,
              engine->getKVBucket()->getVBucket(vbid)->getHighCompletedSeqno());
    EXPECT_EQ(preparedSeqno,
              engine->getKVBucket()->getVBucket(vbid)->getHighPreparedSeqno());
}

TEST_P(DurabilityWarmupTest, HCSPersistedAndLoadedIntoVBState_Commit) {
    testHCSPersistedAndLoadedIntoVBState();
}

TEST_P(DurabilityWarmupTest, testHPSPersistedAndLoadedIntoVBState) {
    // Queue a Prepare
    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*prepare, cookie));

    // Not flushed yet
    auto* kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
    auto vbstate = *kvstore->getVBucketState(vbid);
    ASSERT_EQ(0, vbstate.highPreparedSeqno);
    ASSERT_EQ(0, vbstate.onDiskPrepares);

    // Check the Prepared
    const int64_t preparedSeqno = 1;
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    const auto* sv = vb->ht.findForWrite(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isPending());
    ASSERT_EQ(preparedSeqno, sv->getBySeqno());

    // Persist the Prepare and vbstate.
    flush_vbucket_to_disk(vbid);

    // HPS and prepare counter incremented
    vbstate = *kvstore->getVBucketState(vbid);
    EXPECT_EQ(preparedSeqno, vbstate.highPreparedSeqno);
    // @TODO: RocksDB currently does not track the prepare count
    if ((std::get<0>(GetParam()).find("Rocksdb") == std::string::npos)) {
        EXPECT_EQ(1, vbstate.onDiskPrepares);
    }

    // Warmup
    vb.reset();
    resetEngineAndWarmup();

    kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
    vbstate = *kvstore->getVBucketState(vbid);
    EXPECT_EQ(preparedSeqno, vbstate.highPreparedSeqno);
    // @TODO: RocksDB currently only has an estimated prepare count
    if ((std::get<0>(GetParam()).find("Rocksdb") == std::string::npos)) {
        EXPECT_EQ(1, vbstate.onDiskPrepares);
    }
}

// Test that when setting a vbucket to dead after warmup, when at least one
// Prepared SyncWrite is still pending, that notification ignores the null
// cookie from a warmed up SyncWrite.
TEST_P(DurabilityWarmupTest, SetStateDeadWithWarmedUpPrepare) {
    // Setup: Store a pending SyncWrite/Delete (without committing) and then
    // restart.
    auto key = makeStoredDocKey("key");
    auto item = makePendingItem(key, "pending_value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid);
    resetEngineAndWarmup();

    // Sanity check - should have the SyncWrite after warmup and not committed.
    auto vb = engine->getVBucket(vbid);
    auto gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS, gv.getStatus());

    // Test: Set state to dead. Should skip notification for the warmed-up
    // Prepare (as it has no cookie) when task is run.
    EXPECT_EQ(ENGINE_SUCCESS, store->setVBucketState(vbid, vbucket_state_dead));
    runNextTask(*task_executor->getLpTaskQ()[NONIO_TASK_IDX],
                "Notify clients of Sync Write Ambiguous vb:0");
}

// Test actually covers an issue seen in MB-34956, the issue was just the lack
// of more complete warmup support which is added by MB-34910, in this test
// we check that even after some sync-writes have completed/committed we can
// still warmup and handle latent seqnoAcks, i.e. 0 prepares on disk but we have
// non zero HCS/HPS.
TEST_P(DurabilityWarmupTest, CommittedWithAckAfterWarmup) {
    auto key = makeStoredDocKey("okey");
    auto item = makePendingItem(key, "dokey");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid);
    {
        auto vb = engine->getVBucket(vbid);
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                1);
        vb->processResolvedSyncWrites();

        flush_vbucket_to_disk(vbid, 1);
    }
    resetEngineAndWarmup();
    {
        auto vb = engine->getVBucket(vbid);
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                1);
    }
}

// MB-35192: EPBucket::flushVBucket calls rwUnderlying->optimizeWrites(items);
// Which may reorder the items before they are written to disk.
// Test to ensure the persisted HPS and HCS are set to the highest
// value found in the items that are about to be flushed.
TEST_P(DurabilityWarmupTest, WarmUpHPSAndHCSWithNonSeqnoSortedItems) {
    auto key = makeStoredDocKey("okey");

    // These items will be sorted by key by optimizeWrites
    // ordering them a -> b, the opposite order to their seqnos.
    auto itemB = makePendingItem(makeStoredDocKey("b"), "value");
    auto itemA = makePendingItem(makeStoredDocKey("a"), "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*itemB, cookie));
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*itemA, cookie));
    SCOPED_TRACE("A");
    flush_vbucket_to_disk(vbid, 2);
    {
        auto vb = engine->getVBucket(vbid);
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                2);
        vb->processResolvedSyncWrites();

        flush_vbucket_to_disk(vbid, 2);
    }
    SCOPED_TRACE("B");
    resetEngineAndWarmup();
    {
        auto vb = engine->getVBucket(vbid);
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                2);
    }
}

// Manipulate a replicaVB as if it is receiving from an active (calling correct
// replica methods) and test the VB warms up.
TEST_P(DurabilityWarmupTest, ReplicaVBucket) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto key = makeStoredDocKey("okey");
    auto item = makePendingItem(key, "dokey");
    item->setCas(1);
    item->setBySeqno(1);
    item->setPendingSyncWrite({cb::durability::Level::Majority, 5000});

    auto vb = engine->getVBucket(vbid);

    // Drive a replica just like DCP does
    // Send two snapshots, 1 prepare and 1 commit

    // snap 1
    vb->checkpointManager->createSnapshot(
            1, 1, {} /*HCS*/, CheckpointType::Memory);
    ASSERT_EQ(ENGINE_SUCCESS, store->prepare(*item, cookie));
    flush_vbucket_to_disk(vbid);
    vb->notifyPassiveDMOfSnapEndReceived(1);

    // snap 2
    vb->checkpointManager->createSnapshot(
            2, 2, {} /*HCS*/, CheckpointType::Memory);
    EXPECT_EQ(ENGINE_SUCCESS, vb->commit(key, 1, 2, vb->lockCollections(key)));
    flush_vbucket_to_disk(vbid, 1);
    vb->notifyPassiveDMOfSnapEndReceived(2);

    vb.reset();

    // Warmup and allow the pre/post checker to test the state
    resetEngineAndWarmup();
}

INSTANTIATE_TEST_CASE_P(
        FullOrValue,
        DurabilityWarmupTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

class MB_34718_WarmupTest : public STParameterizedBucketTest {};

// In MB-34718 a GET arrived during warmup on a full-eviction bucket. The GET
// was processed and found an item which had expired. The expiry path queued
// a delete which was flushed. In the document count callbacks, we processed
// the delete and subtracted 1 from the collection count. All of this happened
// before warmup had read the collection counts from disk, so the counter goes
// negative and throws an exception. The test performs those steps seen in the
// MB and demonstrates how changes in warmup prevent this situation, the VB
// is not visible until it is fully initialised by warmup.
TEST_P(MB_34718_WarmupTest, getTest) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active, {});

    // Store a key and trigger a warmup
    auto key = makeStoredDocKey("key");
    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    store_item(vbid, key, "meh", ep_real_time() + 3600);
    flush_vbucket_to_disk(vbid);
    resetEngineAndEnableWarmup();

    // Now run the reader tasks to the stage of interest, we will run the test
    // once we have ran the new warmup stage which puts the fully initialised VB
    // into the vbMap, before warmup has reached that stage we expect the GET to
    // faile with NMVB.
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    bool keepRunningReaderTasks = true;
    while (keepRunningReaderTasks) {
        runNextTask(readerQueue);
        CheckedExecutor executor(task_executor, readerQueue);
        keepRunningReaderTasks = executor.getCurrentTask()->getTaskId() !=
                                 TaskId::WarmupPopulateVBucketMap;

        auto gv = store->get(key, vbid, cookie, options);
        ASSERT_EQ(ENGINE_NOT_MY_VBUCKET, gv.getStatus());

        executor.runCurrentTask();
        executor.completeCurrentTask();
    }

    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb);
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionID::Default));

    // - Full eviction a get is allowed and it can expire documents during the
    //   final stages of warmup.
    // - Value eviction will fail until all K/V are loaded and warmup completes
    if (fullEviction()) {
        // FE can read the item count before loading items
        EXPECT_EQ(1, vb->getNumItems());
        TimeTraveller rick(4800);
        auto gv = store->get(key, vbid, cookie, options);
        EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());
        runBGFetcherTask();

        // Expect expired (key_noent)
        gv = store->get(key, vbid, cookie, options);
        EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

        // Prior to the MB being resolved, this would trigger a negative counter
        // exception as we tried to decrement the collection counter which is
        // 0 because warmup hadn't loaded the counts
        flush_vbucket_to_disk(vbid);

        // Finish warmup so we don't hang TearDown
        runReadersUntilWarmedUp();
    } else {
        // Value eviction, expect no key whilst warming up
        auto gv = store->get(key, vbid, cookie, options);
        EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

        runReadersUntilWarmedUp();

        // VE: Can only read the item count once items are loaded
        EXPECT_EQ(1, vb->getNumItems());

        gv = store->get(key, vbid, cookie, options);

        EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
        TimeTraveller morty(4800);

        // And expired
        gv = store->get(key, vbid, cookie, options);
        EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

        flush_vbucket_to_disk(vbid);
    }
    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionID::Default));
    EXPECT_EQ(0, vb->getNumItems());
}

// Perform the sequence of operations which lead to MB-35326, a snapshot range
// exception. When the issue is fixed this test will pass
TEST_F(WarmupTest, MB_35326) {
    // 1) Write an item to an active vbucket and flush it.
    //    vb state on disk will have a range of {1,1}
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);

    // 2) Mark the vbucket as dead and persist the state
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_dead);

    // 3) Warmup - the dead vbucket will be skipped by warmup but KVStore has
    //    loaded the state into cachedVBStates
    resetEngineAndWarmup();

    EXPECT_FALSE(engine->getVBucket(vbid)) << "Dead vbuckets shouldn't warmup";

    // 4) Now active creation, this results in a new VBucket object with default
    //    state, for this issue the snapshot range of {0,0}
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // 5) Store an item and flush, this would crash because we combine the in
    //    memory range {0,0} with the on disk range {1,1}, the crash occurs
    //    as the new range is {1, 0} and start:1 cannot be greater than end:0.
    store_item(vbid, key, "value");

    flush_vbucket_to_disk(vbid);
}

INSTANTIATE_TEST_CASE_P(FullOrValue,
                        MB_34718_WarmupTest,
                        STParameterizedBucketTest::persistentConfigValues(),
                        STParameterizedBucketTest::PrintToStringParamName);
