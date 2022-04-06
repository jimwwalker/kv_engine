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

#include <memcached/range_scan_optional_configuration.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

#include <chrono>
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

    cb::rangescan::KeyOnly getScanType() const {
        return std::get<2>(GetParam()) == "key_scan"
                       ? cb::rangescan::KeyOnly::Yes
                       : cb::rangescan::KeyOnly::No;
    }

    cb::rangescan::Id createScan(
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            std::optional<cb::rangescan::SnapshotRequirements> seqno =
                    std::nullopt,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig =
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
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
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

cb::rangescan::Id RangeScanTest::createScan(
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig,
        cb::engine_errc expectedStatus) {
    // Create a new RangeScan object and give it a handler we can inspect.
    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     cid,
                                     start,
                                     end,
                                     *handler,
                                     *cookie,
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
                                         cid,
                                         start,
                                         end,
                                         *handler,
                                         *cookie,
                                         getScanType(),
                                         snapshotReqs,
                                         samplingConfig);
    EXPECT_EQ(cb::engine_errc::success, status.first);

    auto vb = store->getVBucket(vbid);
    EXPECT_TRUE(vb);
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    auto scan = epVb.getRangeScan(status.second);
    EXPECT_TRUE(scan);
    return scan->getUuid();
}

// This method drives a range scan through create/continue/cancel for the given
// range. The test drives a range scan serially and the comments indicate where
// a frontend thread would be executing and where a background I/O task would.
void RangeScanTest::testRangeScan(
        const std::unordered_set<StoredDocKey>& expectedKeys,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        size_t itemLimit,
        std::chrono::milliseconds timeLimit,
        size_t extraContinues) {
    // Not smart enough to test both limits yet
    EXPECT_TRUE(!(itemLimit && timeLimit.count()));

    // 1) create a RangeScan to scan the user prefixed keys.
    auto uuid = createScan(cid, start, end);

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
    testRangeScan(getUserKeys(), scanCollection, {"user"}, {"user\xFF"});
}

TEST_P(RangeScanTest, user_prefix_with_item_limit) {
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  1,
                  std::chrono::milliseconds(0),
                  expectedKeys.size());

    handler->scannedKeys.clear();
    handler->scannedItems.clear();

    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
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
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  0,
                  std::chrono::milliseconds(10),
                  expectedKeys.size());
}

// Test ensures callbacks cover disk read case
TEST_P(RangeScanTest, user_prefix_evicted) {
    for (const auto& key : generateTestKeys()) {
        evict_key(vbid, key);
    }
    testRangeScan(getUserKeys(), scanCollection, {"user"}, {"user\xFF"});
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

    testRangeScan(expectedKeys, scanCollection, {"user"}, {"\xFF"});
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

    // note: start is ensuring the key is byte 0 with a length of 1
    testRangeScan(expectedKeys, scanCollection, {"\0", 1}, {"user\xFF"});
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

    testRangeScan(expectedKeys, scanCollection, {key}, {"\xFF"});
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

    // note: start is ensuring the key is byte 0 with a length of 1
    testRangeScan(expectedKeys, scanCollection, {"\0", 1}, {key});
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
    auto uuid = createScan(scanCollection, {"a"}, {"b"});
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
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
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
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
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
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
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

TEST_P(RangeScanTest, snapshot_does_not_contain_seqno_0) {
    auto vb = store->getVBucket(vbid);
    // Nothing @ seqno 0 (use of optional makes this a valid input)
    cb::rangescan::SnapshotRequirements reqs{
            vb->failovers->getLatestUUID(), 0, std::nullopt, true};
    createScan(scanCollection,
               {"user"},
               {"user\xFF"},
               reqs,
               {/* no sampling config*/},
               cb::engine_errc::no_such_key);
}

TEST_P(RangeScanTest, snapshot_does_not_contain_seqno) {
    auto vb = store->getVBucket(vbid);
    // Store, capture high-seqno and update so it's gone from the snapshot
    store_item(vbid, StoredDocKey("update_me", scanCollection), "1");
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             true};
    store_item(vbid, StoredDocKey("update_me", scanCollection), "2");
    flushVBucket(vbid);
    createScan(scanCollection,
               {"user"},
               {"user\xFF"},
               reqs,
               {/* no sampling config*/},
               cb::engine_errc::no_such_key);
}

TEST_P(RangeScanTest, snapshot_contains_seqno) {
    // Something @ high seqno
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             true};
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, false));
}

// There is no wait option, so a future seqno is a failure
TEST_P(RangeScanTest, future_seqno_fails) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::nullopt,
                                             true};
    // This error is detected on first invocation, no need for ewouldblock
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              vb->createRangeScan(scanCollection,
                                  {"user"},
                                  {"user\xFF"},
                                  *handler,
                                  *cookie,
                                  getScanType(),
                                  reqs,
                                  {/* no sampling config*/})
                      .first);
}

TEST_P(RangeScanTest, vb_uuid_check) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{
            1, uint64_t(vb->getHighSeqno()), std::nullopt, true};
    // This error is detected on first invocation, no need for ewouldblock
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              vb->createRangeScan(scanCollection,
                                  {"user"},
                                  {"user\xFF"},
                                  *handler,
                                  *cookie,
                                  getScanType(),
                                  reqs,
                                  {/* no sampling config*/})
                      .first);
}

TEST_P(RangeScanTest, random_sample_not_enough_items) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // Request more samples than keys, which is not allowed
    auto sampleSize = stats[scanCollection].itemCount + 1;
    createScan(scanCollection,
               {"\0", 1},
               {"\xFF"},
               {/* no snapshot requirements */},
               cb::rangescan::SamplingConfiguration{sampleSize, 0},
               cb::engine_errc::out_of_range);
}

TEST_P(RangeScanTest, random_sample) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // We'll sample 1/2 of the keys from the collection
    auto sampleSize = stats[scanCollection].itemCount / 2;

    // key ranges covers all keys in scanCollection, kv_engine will do this
    // not the client
    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 0});

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
                                     scanCollection,
                                     {"\0", 1},
                                     {"\xFF"},
                                     *handler,
                                     *cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);
}

TEST_P(RangeScanTest, unknown_collection) {
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->createRangeScan(vbid,
                                     CollectionEntry::meat.getId(),
                                     {"\0", 1},
                                     {"\xFF"},
                                     *handler,
                                     *cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);
}

// Test that the collection going away after part 1 of create, cleans up
TEST_P(RangeScanTest, scan_cancels_after_create) {
    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     *handler,
                                     *cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);
    // Now run via auxio task
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanCreateTask");

    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));

    // Drop scanCollection on a different cookie
    auto cookie2 = create_mock_cookie();
    EXPECT_EQ(scanCollection, CollectionEntry::vegetable.getId());
    setCollections(cookie2, cm.remove(CollectionEntry::vegetable));
    destroy_mock_cookie(cookie2);

    // Second part of create runs and fails
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     *handler,
                                     *cookie,
                                     getScanType(),
                                     {},
                                     {})
                      .first);

    // Task was scheduled to cancel (close the snapshot). The continue task
    // does cancels and runs on the reader queue
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "RangeScanContinueTask");

    // Can't get hold of the scan object as we never got the uuid
}

TEST_P(RangeScanTest, wait_for_persistence_success) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. We are willing to wait
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::chrono::milliseconds(100),
                                             false};

    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     *handler,
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {})
                      .first);

    // store our item and flush (so the waitForPersistence is notified)
    store_item(vbid, StoredDocKey("waiting", scanCollection), "");
    EXPECT_EQ(1, vb->getHighPriorityChkSize());
    flushVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));
    EXPECT_EQ(0, vb->getHighPriorityChkSize());

    // Now the task will move to create, we can drive the scan using our wrapper
    // it will do the next ewouldblock phase finally creating the scan
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);

    // Close the scan
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, false));
}

TEST_P(RangeScanTest, wait_for_persistence_fails) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. No timeout so fails on
    // the first crack of the whip
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::nullopt,
                                             false};

    EXPECT_EQ(cb::engine_errc::temporary_failure,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     *handler,
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {})
                      .first);
}

TEST_P(RangeScanTest, wait_for_persistence_timeout) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. We are willing to wait
    // set the timeout to 0, so first flush will expire
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 2),
                                             std::chrono::milliseconds(0),
                                             false};

    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(vbid,
                                     scanCollection,
                                     {"user"},
                                     {"user\xFF"},
                                     *handler,
                                     *cookie,
                                     getScanType(),
                                     reqs,
                                     {})
                      .first);

    // store an item and flush (so the waitForPersistence is notified and
    // expired)
    store_item(vbid, StoredDocKey("waiting", scanCollection), "");
    flushVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::temporary_failure, mock_waitfor_cookie(cookie));
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
