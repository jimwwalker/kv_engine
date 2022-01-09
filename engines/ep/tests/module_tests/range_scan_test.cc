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

#include "dcp/backfill.h"
#include "ep_bucket.h"
#include "ep_vb.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"

#include <utilities/test_manifest.h>

#include <unordered_set>

class RangeScanTest : public STParamPersistentBucketTest {
public:
    void SetUp() override {
        ASSERT_TRUE(collection > collection2);
        ASSERT_TRUE(collection < collection3);

        STParamPersistentBucketTest::SetUp();

        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        storeAndFlush = [this](const DocKey& key) {
            // set value to be the key's debug value
            store_item(vbid, key, key.to_string());
            flush_vbucket_to_disk(vbid, 1);
        };

        CollectionsManifest cm;
        cm.add(CollectionEntry::vegetable);
        cm.add(CollectionEntry::fruit);
        cm.add(CollectionEntry::dairy);
        setCollections(cookie, cm);
        flush_vbucket_to_disk(vbid, 3);
    }

    void testRangeScan(std::function<void(const DocKey&)> storeFunc,
                       const std::vector<StoredDocKey>& keys,
                       const std::unordered_set<StoredDocKey>& expectedKeys,
                       const DocKey& start,
                       const DocKey& end);

    void testLessThan(std::string key);

    std::function<void(const DocKey&)> storeAndFlush;

    // Tests all operate against the following collection
    const CollectionID collection = CollectionEntry::vegetable.getId();
    // Tests also have data in these collections, and these deliberately "wrap"
    // the vegetable collection
    const CollectionID collection2 = CollectionEntry::fruit.getId();
    const CollectionID collection3 = CollectionEntry::dairy.getId();

    /// return a set of keys that are all "user" prefixed
    const std::unordered_set<StoredDocKey> getUserKeys() {
        std::unordered_set<StoredDocKey> keys;
        for (const auto& k :
             {"user-alan", "useralan", "user.claire", "user::zoe", "users"}) {
            keys.emplace(makeStoredDocKey(k, collection));
        }
        return keys;
    }

    /// generate test keys
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
            keys.push_back(makeStoredDocKey(k, collection));
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
};

void RangeScanTest::testRangeScan(
        std::function<void(const DocKey&)> store,
        const std::vector<StoredDocKey>& keys,
        const std::unordered_set<StoredDocKey>& expectedKeys,
        const DocKey& start,
        const DocKey& end) {
    for (const auto& key : keys) {
        store(key);
    }

    // scan for "user" prefixed documents
    auto scan = getEPBucket().createAndScheduleRangeScan(vbid, start, end);
    ASSERT_EQ(cb::engine_errc::success, scan.first);
    EXPECT_NE(0, scan.second);
    runBackfill();

    // Find the scan and verify the items
    auto vb = this->store->getVBuckets().getBucket(vbid);
    auto& epVB = dynamic_cast<EPVBucket&>(*vb);
    auto ctx = epVB.getRangeScanContext(scan.second);
    ASSERT_TRUE(ctx);

    // Subtract one to remove the null sentinel
    EXPECT_EQ(expectedKeys.size(), ctx->getSize() - 1);

    while (ctx->getSize()) {
        auto item = ctx->popFront();
        if (item) {
            auto itr = expectedKeys.find(item->getKey());
            EXPECT_NE(itr, expectedKeys.end());
            EXPECT_EQ((*itr).to_string(), item->getValueView());
        } else {
            // sentinel popped, expect empty
            EXPECT_EQ(0, ctx->getSize());
        }
    }
}

// Test that if we set the range to be "user" - "user\xFF" our scan returns
// the user prefixed keys
TEST_P(RangeScanTest, basicScan) {
    testRangeScan(storeAndFlush,
                  generateTestKeys(),
                  getUserKeys(),
                  makeStoredDocKey("user", collection),
                  makeStoredDocKey("user\xFF", collection));
}

// Same test, but with eviction means the scan must fetch from disk
TEST_P(RangeScanTest, basicScanEvictAllItems) {
    auto store = [this](const DocKey& key) {
        storeAndFlush(key);
        evict_key(vbid, key);
    };
    testRangeScan(store,
                  generateTestKeys(),
                  getUserKeys(),
                  makeStoredDocKey("user", collection),
                  makeStoredDocKey("user\xFF", collection));
}

// Run a >= user scan by setting the keys to user and the end (255)
TEST_P(RangeScanTest, greaterThanOrEqual) {
    auto expectedKeys = getUserKeys();
    auto testData = generateTestKeys();
    auto rangeStart = makeStoredDocKey("user", collection);
    for (const auto& key : testData) {
        // to_string returns a debug "cid:key", but >= will select the
        // correct keys for this text
        if (key.getCollectionID() == collection &&
            key.to_string() >= rangeStart.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(storeAndFlush,
                  testData,
                  expectedKeys,
                  rangeStart,
                  makeStoredDocKey("\xFF", collection));
}

// Run a <= user scan y setting the keys to 0 and user\xFF
TEST_P(RangeScanTest, lessThanOrEqual) {
    auto expectedKeys = getUserKeys();
    auto testData = generateTestKeys();
    auto rangeEnd = makeStoredDocKey("user\xFF", collection);
    for (const auto& key : testData) {
        // to_string returns a debug "cid:key", but <= will select the
        // correct keys for this text
        if (key.getCollectionID() == collection &&
            key.to_string() <= rangeEnd.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(storeAndFlush,
                  testData,
                  expectedKeys,
                  makeStoredDocKey("\0", collection),
                  rangeEnd);
}

// Perform > uuu, this simulates a request for an exclusive start range-scan
TEST_P(RangeScanTest, greaterThan) {
    // Here the client could of specified "aaa" and flag to set exclusive-start
    // so we set the start to "skip" aaa and start from the next key

    // This test kind of walks through how a client may be resuming after the
    // scan being destroyed for some reason (restart/rebalance).
    // key "uuu" is the last received key, so they'd like to receive in the next
    // scan all keys greater then uuu, but not uuu itself (exclusive start or >)
    std::string key = "uuu";
    auto testData = generateTestKeys();

    // In this case the client requests exclusive start and we manipulate the
    // key to achieve exactly that by appending the value of 0
    key += char(0);
    auto rangeStart = makeStoredDocKey(key, collection);

    // Let's also store rangeStart as if a client had written such a key (it's)
    // possible.
    testData.push_back(rangeStart);

    // So now generate the expected keys. rangeStart is logically greater than
    // uuu so >= here will select all keys we expect to see in the result
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : testData) {
        if (k.getCollectionID() == collection &&
            k.to_string() >= rangeStart.to_string()) {
            expectedKeys.emplace(k);
        }
    }

    testRangeScan(storeAndFlush,
                  testData,
                  expectedKeys,
                  rangeStart,
                  makeStoredDocKey("\xFF", collection));
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

    auto rangeEnd = makeStoredDocKey(key, collection);

    // Let's also store rangeEnd as if a client had written such a key (it's)
    // possible.
    auto testData = generateTestKeys();

    testData.push_back(rangeEnd);

    // So now generate the expected keys. rangeEnd is logically less than
    // the input key  so <=here will select all keys we expect to see in the
    // result
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : testData) {
        if (k.getCollectionID() == collection &&
            k.to_string() <= rangeEnd.to_string()) {
            expectedKeys.emplace(k);
        }
    }

    testRangeScan(storeAndFlush,
                  testData,
                  expectedKeys,
                  makeStoredDocKey("\0", collection),
                  rangeEnd);
}

TEST_P(RangeScanTest, lessThan) {
    testLessThan("uuu");
}

TEST_P(RangeScanTest, lessThanWithZeroSuffix) {
    // a key that is "uuu\0" in a less than scan has to become uuu (pop of \0)
    std::string key = "uuu";
    key += char(0);
    testLessThan(key);
}

// Can only scan active vbuckets
TEST_P(RangeScanTest, detectVbucketStateChangeAtScan) {
    // Keys are needed so we get a callback which and we detect the state change
    // leading to the scan stop
    auto testData = generateTestKeys();
    for (const auto& key : testData) {
        storeAndFlush(key);
    }
    auto scan = getEPBucket().createAndScheduleRangeScan(
            vbid,
            makeStoredDocKey("\0", collection),
            makeStoredDocKey("\xFF", collection));
    ASSERT_EQ(cb::engine_errc::success, scan.first);
    EXPECT_NE(0, scan.second);
    // change state when the KVStore::scan begins
    runBackfill([this]() {
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    });
}

INSTANTIATE_TEST_SUITE_P(FullAndvalueEviction,
                         RangeScanTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);