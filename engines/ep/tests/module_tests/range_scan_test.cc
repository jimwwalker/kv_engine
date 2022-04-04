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

#include "collections/collection_persisted_stats.h"
#include "ep_bucket.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan.h"
#include "range_scans/range_scan_callbacks.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

#include <unordered_set>
#include <vector>

// A handler implementation that just stores the scan key/items in vectors
class TestRangeScanHandler : public RangeScanDataHandlerIFace {
public:
    void handleKey(DocKey key) override {
        scannedKeys.emplace_back(key);
        testHook(scannedKeys.size());
    }

    void handleItem(std::unique_ptr<Item> item) override {
        scannedItems.emplace_back(std::move(item));
        testHook(scannedItems.size());
    }

    void validateKeyScan(const std::unordered_set<StoredDocKey>& expectedKeys);
    void validateItemScan(const std::unordered_set<StoredDocKey>& expectedKeys);

    std::function<void(size_t)> testHook = [](size_t) {};
    std::vector<std::unique_ptr<Item>> scannedItems;
    std::vector<StoredDocKey> scannedKeys;
};

class RangeScanTest
    : public SingleThreadedEPBucketTest,
      public ::testing::WithParamInterface<
              std::tuple<std::string, std::string, std::string>> {
public:
    void SetUp() override {
        config_string += generateBackendConfig(std::get<0>(GetParam()));
        config_string += ";item_eviction_policy=" + getEvictionMode();
#ifdef EP_USE_MAGMA
        config_string += ";" + magmaRollbackConfig;
#endif
        SingleThreadedEPBucketTest::SetUp();

        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        // Setup collections and store keys
        cm.add(CollectionEntry::vegetable);
        cm.add(CollectionEntry::fruit);
        cm.add(CollectionEntry::dairy);
        setCollections(cookie, cm);
        flush_vbucket_to_disk(vbid, 3);
        storeTestKeys();
    }

    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info) {
        return std::get<0>(info.param) + "_" + std::get<1>(info.param) + "_" +
               std::get<2>(info.param);
    }

    std::string getEvictionMode() const {
        return std::get<1>(GetParam());
    }

    bool isKeyOnly() const {
        return std::get<2>(GetParam()) == "key_scan";
    }

    RangeScanKeyOnly getScanType() const {
        return std::get<2>(GetParam()) == "key_scan" ? RangeScanKeyOnly::Yes
                                                     : RangeScanKeyOnly::No;
    }

    RangeScanId createScan(
            const DocKey& start,
            const DocKey& end,
            std::optional<RangeScanSnapshotRequirements> seqno = std::nullopt,
            std::optional<RangeScanSamplingConfiguration> samplingConfig =
                    std::nullopt,
            cb::engine_errc expectedStatus = cb::engine_errc::success);

    const std::unordered_set<StoredDocKey> getUserKeys() {
        // Create a number of user prefixed collections and place them in the
        // collection that we will scan.
        std::unordered_set<StoredDocKey> keys;
        for (const auto& k :
             {"user-alan", "useralan", "user.claire", "user::zoe", "users"}) {
            keys.emplace(makeStoredDocKey(k, scanCollection));
        }
        return keys;
    }

    /**
     * generate a vector containing all of the keys which will be stored before
     * the test runs. Tests can then scan for these using various start/end
     * patterns
     */
    const std::vector<StoredDocKey> generateTestKeys() {
        std::vector<StoredDocKey> keys;

        for (const auto& k : getUserKeys()) {
            keys.push_back(k);
            keys.push_back(makeStoredDocKey(k.c_str(), collection2));
            keys.push_back(makeStoredDocKey(k.c_str(), collection3));
        }

        // Add some other keys, one above and below user and then some at
        // further ends of the alphabet
        for (const auto& k : {"useq", "uses", "abcd", "uuu", "uuuu", "xyz"}) {
            keys.push_back(makeStoredDocKey(k, scanCollection));
            keys.push_back(makeStoredDocKey(k, collection2));
            keys.push_back(makeStoredDocKey(k, collection3));
        }
        // Some stuff in other collections, no real meaning to this, just other
        // data we should never hit in the scan
        for (const auto& k : {"1000", "718", "ZOOM", "U", "@@@@"}) {
            keys.push_back(makeStoredDocKey(k, collection2));
            keys.push_back(makeStoredDocKey(k, collection3));
        }
        return keys;
    }

    void storeTestKeys() {
        for (const auto& key : generateTestKeys()) {
            // Store key with StoredDocKey::to_string as the value
            store_item(vbid, key, key.to_string());
        }
        flushVBucket(vbid);
    }

    // Run a scan using the relatively low level pieces
    void testRangeScan(
            const std::unordered_set<StoredDocKey>& expectedKeys,
            const DocKey& start,
            const DocKey& end,
            size_t itemLimit = 0,
            std::chrono::milliseconds timeLimit = std::chrono::milliseconds(0),
            size_t extraContinues = 0);

    void testLessThan(std::string key);

    // Tests all scan against the following collection
    const CollectionID scanCollection = CollectionEntry::vegetable.getId();
    // Tests also have data in these collections, and these deliberately enclose
    // the vegetable collection
    const CollectionID collection2 = CollectionEntry::fruit.getId();
    const CollectionID collection3 = CollectionEntry::dairy.getId();

    std::unique_ptr<TestRangeScanHandler> handler{
            std::make_unique<TestRangeScanHandler>()};
    CollectionsManifest cm;
};

void TestRangeScanHandler::validateKeyScan(
        const std::unordered_set<StoredDocKey>& expectedKeys) {
    EXPECT_TRUE(scannedItems.empty());
    EXPECT_EQ(expectedKeys.size(), scannedKeys.size());
    for (const auto& key : scannedKeys) {
        // Expect to find the key
        EXPECT_EQ(1, expectedKeys.count(key));
    }
}

void TestRangeScanHandler::validateItemScan(
        const std::unordered_set<StoredDocKey>& expectedKeys) {
    EXPECT_TRUE(scannedKeys.empty());
    EXPECT_EQ(expectedKeys.size(), scannedItems.size());
    for (const auto& scanItem : scannedItems) {
        auto itr = expectedKeys.find(scanItem->getKey());
        // Expect to find the key
        EXPECT_NE(itr, expectedKeys.end());
        // And the value of StoredDocKey::to_string should equal the value
        EXPECT_EQ(itr->to_string(), scanItem->getValueView());
    }
}

RangeScanId RangeScanTest::createScan(
        const DocKey& start,
        const DocKey& end,
        std::optional<RangeScanSnapshotRequirements> snapshotReqs,
        std::optional<RangeScanSamplingConfiguration> samplingConfig,
        cb::engine_errc expectedStatus) {
    // Create a new RangeScan object and give it a handler we can inspect.
    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     start,
                                     end,
                                     *handler,
                                     cookie,
                                     getScanType(),
                                     snapshotReqs,
                                     samplingConfig)
                      .first);
    // Now run via auxio task
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanCreateTask");

    EXPECT_EQ(expectedStatus, mock_waitfor_cookie(cookie));

    if (expectedStatus != cb::engine_errc::success) {
        return {};
    }

    // Next frontend will add the uuid/scan, client can be informed of the uuid
    auto status = store->createRangeScan(vbid,
                                         start,
                                         end,
                                         *handler,
                                         cookie,
                                         getScanType(),
                                         snapshotReqs,
                                         samplingConfig);
    EXPECT_EQ(cb::engine_errc::success, status.first);

    auto vb = store->getVBucket(vbid);
    EXPECT_TRUE(vb);
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    auto scan = epVb.getRangeScan(status.second);
    EXPECT_TRUE(scan);

    return status.second;
}

// This method drives a range scan through create/continue/cancel for the given
// range. The test drives a range scan serially and the comments indicate where
// a frontend thread would be executing and where a background I/O task would.
void RangeScanTest::testRangeScan(
        const std::unordered_set<StoredDocKey>& expectedKeys,
        const DocKey& start,
        const DocKey& end,
        size_t itemLimit,
        std::chrono::milliseconds timeLimit,
        size_t extraContinues) {
    // Not smart enough to test both limits yet
    EXPECT_TRUE(!(itemLimit && timeLimit.count()));

    // 1) create a RangeScan to scan the user prefixed keys.
    auto uuid = createScan(start, end);

    // 2) Continue a RangeScan
    // 2.1) Frontend thread would call this method using clients uuid
    EXPECT_EQ(cb::engine_errc::would_block,
              store->continueRangeScan(vbid, uuid, itemLimit, timeLimit));

    // 2.2) An I/O task now reads data from disk
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "RangeScanContinueTask");

    // Tests will need more continues if a limit is in-play
    for (size_t count = 0; count < extraContinues; count++) {
        EXPECT_EQ(cb::engine_errc::would_block,
                  store->continueRangeScan(vbid, uuid, itemLimit, timeLimit));
        runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                    "RangeScanContinueTask");
    }

    // 2.3) All expected keys must have been read from disk
    if (isKeyOnly()) {
        handler->validateKeyScan(expectedKeys);
    } else {
        handler->validateItemScan(expectedKeys);
    }

    // In this case the scan finished and cleaned up

    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key, store->cancelRangeScan(vbid, uuid));

    // Or continued, uuid is unknown
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->continueRangeScan(
                      vbid, uuid, 0, std::chrono::milliseconds(0)));
}

// Scan for the user prefixed keys
TEST_P(RangeScanTest, user_prefix) {
    testRangeScan(getUserKeys(),
                  makeStoredDocKey("user", scanCollection),
                  makeStoredDocKey("user\xFF", scanCollection));
}

TEST_P(RangeScanTest, user_prefix_with_item_limit) {
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  makeStoredDocKey("user", scanCollection),
                  makeStoredDocKey("user\xFF", scanCollection),
                  1,
                  std::chrono::milliseconds(0),
                  expectedKeys.size());

    handler->scannedKeys.clear();
    handler->scannedItems.clear();

    testRangeScan(expectedKeys,
                  makeStoredDocKey("user", scanCollection),
                  makeStoredDocKey("user\xFF", scanCollection),
                  2,
                  std::chrono::milliseconds(0),
                  expectedKeys.size() / 2);
}

TEST_P(RangeScanTest, user_prefix_with_time_limit) {
    // Replace time with a function that ticks per call, forcing the scan to
    // yield for every item
    RangeScan::setClockFunction([]() {
        static auto now = std::chrono::steady_clock::now();
        now += std::chrono::milliseconds(100);
        return now;
    });
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  makeStoredDocKey("user", scanCollection),
                  makeStoredDocKey("user\xFF", scanCollection),
                  0,
                  std::chrono::milliseconds(10),
                  expectedKeys.size());
}

// Test ensures callbacks cover disk read case
TEST_P(RangeScanTest, user_prefix_evicted) {
    for (const auto& key : generateTestKeys()) {
        evict_key(vbid, key);
    }
    testRangeScan(getUserKeys(),
                  makeStoredDocKey("user", scanCollection),
                  makeStoredDocKey("user\xFF", scanCollection));
}

// Run a >= user scan by setting the keys to user and the end (255)
TEST_P(RangeScanTest, greater_than_or_equal) {
    auto expectedKeys = getUserKeys();
    auto rangeStart = makeStoredDocKey("user", scanCollection);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but >= will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() >= rangeStart.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(
            expectedKeys, rangeStart, makeStoredDocKey("\xFF", scanCollection));
}

// Run a <= user scan y setting the keys to 0 and user\xFF
TEST_P(RangeScanTest, less_than_or_equal) {
    auto expectedKeys = getUserKeys();
    auto rangeEnd = makeStoredDocKey("user\xFF", scanCollection);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but <= will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() <= rangeEnd.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(
            expectedKeys, makeStoredDocKey("\0", scanCollection), rangeEnd);
}

// Perform > uuu, this simulates a request for an exclusive start range-scan
TEST_P(RangeScanTest, greater_than) {
    // Here the client could of specified "aaa" and flag to set exclusive-start
    // so we set the start to "skip" aaa and start from the next key

    // This test kind of walks through how a client may be resuming after the
    // scan being destroyed for some reason (restart/rebalance).
    // key "uuu" is the last received key, so they'd like to receive in the next
    // scan all keys greater then uuu, but not uuu itself (exclusive start or >)
    std::string key = "uuu";

    // In this case the client requests exclusive start and we manipulate the
    // key to achieve exactly that by appending the value of 0
    key += char(0);
    auto rangeStart = makeStoredDocKey(key, scanCollection);

    // Let's also store rangeStart as if a client had written such a key (it's)
    // possible.
    store_item(vbid, rangeStart, rangeStart.to_string());
    flushVBucket(vbid);

    // So now generate the expected keys. rangeStart is logically greater than
    // uuu so >= here will select all keys we expect to see in the result
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : generateTestKeys()) {
        if (k.getCollectionID() == scanCollection &&
            k.to_string() >= rangeStart.to_string()) {
            expectedKeys.emplace(k);
        }
    }
    expectedKeys.emplace(rangeStart);

    testRangeScan(
            expectedKeys, rangeStart, makeStoredDocKey("\xFF", scanCollection));
}

// scan for less than key
void RangeScanTest::testLessThan(std::string key) {
    // In this case the client requests an exclusive end and we manipulate the
    // key by changing the suffix character, pop or subtract based on value
    if (key.back() == char(0)) {
        key.pop_back();
    } else {
        key.back()--;
    }

    auto rangeEnd = makeStoredDocKey(key, scanCollection);

    // Let's also store rangeEnd as if a client had written such a key (it's)
    // possible.
    store_item(vbid, rangeEnd, rangeEnd.to_string());
    flushVBucket(vbid);

    // So now generate the expected keys. rangeEnd is logically less than
    // the input key  so <=here will select all keys we expect to see in the
    // result
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : generateTestKeys()) {
        if (k.getCollectionID() == scanCollection &&
            k.to_string() <= rangeEnd.to_string()) {
            expectedKeys.emplace(k);
        }
    }
    expectedKeys.emplace(rangeEnd);

    testRangeScan(
            expectedKeys, makeStoredDocKey("\0", scanCollection), rangeEnd);
}

TEST_P(RangeScanTest, less_than) {
    testLessThan("uuu");
}

TEST_P(RangeScanTest, less_than_with_zero_suffix) {
    std::string key = "uuu";
    key += char(0);
    testLessThan(key);
}

// Test that we reject continue whilst a scan is already being continued
TEST_P(RangeScanTest, continue_must_be_serialised) {
    auto uuid = createScan(makeStoredDocKey("a"), makeStoredDocKey("b"));
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(uuid, 0, std::chrono::milliseconds(0)));
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    EXPECT_TRUE(epVb.getRangeScan(uuid)->isContinuing());

    // Cannot continue again
    EXPECT_EQ(cb::engine_errc::too_busy,
              vb->continueRangeScan(uuid, 0, std::chrono::milliseconds(0)));

    // But can cancel
    EXPECT_EQ(cb::engine_errc::would_block, vb->cancelRangeScan(uuid, true));
}

// Create and then straight to cancel
TEST_P(RangeScanTest, create_cancel) {
    auto uuid = createScan(makeStoredDocKey("user", scanCollection),
                           makeStoredDocKey("user\xFF", scanCollection));
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::would_block, vb->cancelRangeScan(uuid, true));
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "RangeScanContinueTask");

    // Nothing read
    EXPECT_TRUE(handler->scannedKeys.empty());
    EXPECT_TRUE(handler->scannedItems.empty());
}

// Test that whilst the scan has been continued, but before the task runs, it
// can be cancelled, and the task brings the scan ends on the task
TEST_P(RangeScanTest, create_continue_is_cancelled) {
    auto uuid = createScan(makeStoredDocKey("user", scanCollection),
                           makeStoredDocKey("user\xFF", scanCollection));
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(uuid, 0, std::chrono::milliseconds(0)));

    // Cancel
    EXPECT_EQ(cb::engine_errc::would_block, vb->cancelRangeScan(uuid, true));

    // At the moment continue and cancel are creating new tasks, run them both

    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "RangeScanContinueTask");
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "RangeScanContinueTask");

    // Nothing read
    EXPECT_TRUE(handler->scannedKeys.empty());
    EXPECT_TRUE(handler->scannedItems.empty());
}

// Test that a scan doesn't blindly keep on reading if a cancel occurs
TEST_P(RangeScanTest, create_continue_is_cancelled_2) {
    auto uuid = createScan(makeStoredDocKey("user", scanCollection),
                           makeStoredDocKey("user\xFF", scanCollection));
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(uuid, 0, std::chrono::milliseconds(0)));

    // Set a hook which will cancel when the 2nd key is read
    handler->testHook = [&vb, uuid](size_t count) {
        EXPECT_LT(count, 3); // never reach third key
        if (count == 2) {
            EXPECT_EQ(cb::engine_errc::would_block,
                      vb->cancelRangeScan(uuid, true));
        }
    };

    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "RangeScanContinueTask");

    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key, vb->cancelRangeScan(uuid, true));

    // Or continued, uuid is unknown
    EXPECT_EQ(cb::engine_errc::no_such_key,
              vb->continueRangeScan(uuid, 0, std::chrono::milliseconds(0)));

    // Scan only read 2 of the possible keys
    if (isKeyOnly()) {
        EXPECT_EQ(2, handler->scannedKeys.size());
    } else {
        EXPECT_EQ(2, handler->scannedItems.size());
    }
}

TEST_P(RangeScanTest, snapshot_does_not_contain_seqno) {
    auto vb = store->getVBucket(vbid);
    // Nothing @ seqno 0 (use of optional makes this a valid input)
    RangeScanSnapshotRequirements reqs{vb->failovers->getLatestUUID(), 0, true};
    createScan(makeStoredDocKey("user", scanCollection),
               makeStoredDocKey("user\xFF", scanCollection),
               reqs,
               {/* no sampling config*/},
               cb::engine_errc::failed);

    // Nothing @ high seqno + 1
    reqs.seqno = uint64_t(vb->getHighSeqno() + 1);
    createScan(makeStoredDocKey("user", scanCollection),
               makeStoredDocKey("user\xFF", scanCollection),
               reqs,
               {/* no sampling config*/},
               cb::engine_errc::failed);
}

TEST_P(RangeScanTest, snapshot_contains_seqno) {
    // Something @ high seqno
    auto vb = store->getVBucket(vbid);
    RangeScanSnapshotRequirements reqs{
            vb->failovers->getLatestUUID(), uint64_t(vb->getHighSeqno()), true};
    auto uuid = createScan(makeStoredDocKey("user", scanCollection),
                           makeStoredDocKey("user\xFF", scanCollection),
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, false));
}

TEST_P(RangeScanTest, random_sample) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // We'll sample 1/2 of the keys from the collection
    auto sampleSize = stats[scanCollection].itemCount / 2;

    // key ranges covers all keys in scanCollection
    auto uuid = createScan(makeStoredDocKey("\0", scanCollection),
                           makeStoredDocKey("\xff", scanCollection),
                           {/* no snapshot requirements */},
                           RangeScanSamplingConfiguration{0, sampleSize});

    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(uuid, 0, std::chrono::milliseconds(0)));

    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "RangeScanContinueTask");

    if (isKeyOnly()) {
        EXPECT_EQ(sampleSize, handler->scannedKeys.size());
    } else {
        EXPECT_EQ(sampleSize, handler->scannedItems.size());
    }
}

TEST_P(RangeScanTest, not_my_vbucket) {
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              store->createRangeScan(Vbid(4),
                                     makeStoredDocKey("\0", scanCollection),
                                     makeStoredDocKey("\xff", scanCollection),
                                     *handler,
                                     cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);
}

TEST_P(RangeScanTest, unknown_collection) {
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->createRangeScan(
                           vbid,
                           makeStoredDocKey("\0", CollectionEntry::meat),
                           makeStoredDocKey("\xff", CollectionEntry::meat),
                           *handler,
                           cookie,
                           getScanType(),
                           {},
                           {})
                      .first);
}

// Test that the collection going away after part 1 of create, cleans up
TEST_P(RangeScanTest, scan_cancels_after_create) {
    EXPECT_EQ(
            cb::engine_errc::would_block,
            store->createRangeScan(vbid,
                                   makeStoredDocKey("user", scanCollection),
                                   makeStoredDocKey("user\xff", scanCollection),
                                   *handler,
                                   cookie,
                                   getScanType(),
                                   {},
                                   {})
                    .first);
    // Now run via auxio task
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanCreateTask");

    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));

    // Drop scanCollection
    EXPECT_EQ(scanCollection, CollectionEntry::vegetable.getId());
    setCollections(cookie, cm.remove(CollectionEntry::vegetable));

    // Second part of create runs and fails
    EXPECT_EQ(
            cb::engine_errc::unknown_collection,
            store->createRangeScan(vbid,
                                   makeStoredDocKey("user", scanCollection),
                                   makeStoredDocKey("user\xff", scanCollection),
                                   *handler,
                                   cookie,
                                   getScanType(),
                                   {},
                                   {})
                    .first);

    // Task was scheduled to cancel (close the snapshot)
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanCreateTask");

    // Can't get hold of the scan object as we never got the uuid
}

auto scanConfigValues = ::testing::Combine(
        // Run for couchstore only until MB-49816 is resolved
        ::testing::Values("persistent_couchdb"),
        ::testing::Values("value_only", "full_eviction"),
        ::testing::Values("key_scan", "value_scan"));

INSTANTIATE_TEST_SUITE_P(RangeScanFullAndValueEviction,
                         RangeScanTest,
                         scanConfigValues,
                         RangeScanTest::PrintToStringParamName);
