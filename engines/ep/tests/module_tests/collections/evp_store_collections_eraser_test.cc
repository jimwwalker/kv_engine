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

#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"

class CollectionsEraserTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
        vb = store->getVBucket(vbid);
    }

    void TearDown() override {
        vb.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    void runEraser() {
        if (persistent()) {
            runCompaction();
        } else {
            EphemeralVBucket* evb = dynamic_cast<EphemeralVBucket*>(vb.get());

            evb->purgeStaleItems();
        }
    }

    void flush_vbucket_to_disk(Vbid vbid, size_t expected = 1) {
        flushVBucketToDiskIfPersistent(vbid, expected);
    }

    /// Override so we recreate the vbucket for ephemeral tests
    void resetEngineAndWarmup() {
        SingleThreadedKVBucketTest::resetEngineAndWarmup();
        if (!persistent()) {
            // Persistent will recreate the VB from the disk metadata so for
            // ephemeral do an explicit set state.
            EXPECT_EQ(ENGINE_SUCCESS,
                      store->setVBucketState(vbid, vbucket_state_active));
        }
    }

    VBucketPtr vb;
};

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, basic) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    EXPECT_EQ(2, vb->getNumItems());

    // Evict one of the keys, we should still erase it
    if (persistent()) {
        evict_key(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});
    }
    // delete the collection
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        // The system event still exists as a tombstone and will reside in the
        // system until tombstone purging removes it.
        EXPECT_EQ(1, vb->getNumSystemItems());
    }
}

TEST_P(CollectionsEraserTest, basic_2_collections) {
    // add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});

    flush_vbucket_to_disk(vbid, 2 /* 2 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    flush_vbucket_to_disk(vbid, 4);

    EXPECT_EQ(4, vb->getNumItems());

    // delete the collections
    vb->updateFromManifest(
            {cm.remove(CollectionEntry::dairy).remove(CollectionEntry::fruit)});

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flush_vbucket_to_disk(vbid, 2 /* 2 x system */);

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserTest, basic_3_collections) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    flush_vbucket_to_disk(vbid, 4 /* 2x items */);

    EXPECT_EQ(4, vb->getNumItems());

    // delete one of the 3 collections
    vb->updateFromManifest({cm.remove(CollectionEntry::fruit)});

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runCollectionsEraser();

    EXPECT_EQ(2, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserTest, basic_4_collections) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    flush_vbucket_to_disk(vbid, 4 /* 2x items */);

    // delete the collections and re-add a new dairy
    vb->updateFromManifest({cm.remove(CollectionEntry::fruit)
                                    .remove(CollectionEntry::dairy)
                                    .add(CollectionEntry::dairy2)});

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flush_vbucket_to_disk(vbid, 3 /* 3x system (2 deletes, 1 create) */);

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserTest, default_Destroy) {
    // add some items
    store_item(vbid,
               StoredDocKey{"dairy:milk", CollectionEntry::defaultC},
               "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::defaultC},
               "lovely");
    store_item(vbid,
               StoredDocKey{"fruit:apple", CollectionEntry::defaultC},
               "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::defaultC},
               "lovely");

    flush_vbucket_to_disk(vbid, 4);

    EXPECT_EQ(4, vb->getNumItems());

    // delete the default collection
    CollectionsManifest cm;
    vb->updateFromManifest({cm.remove(CollectionEntry::defaultC)});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    // Add default back - so we don't get collection unknown errors
    vb->updateFromManifest({cm.add(CollectionEntry::defaultC)});

    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue gv =
            store->get(StoredDocKey{"dairy:milk", CollectionEntry::defaultC},
                       vbid,
                       cookie,
                       options);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());
}

// Test that following a full drop (compaction completes the deletion), warmup
// reloads the VB::Manifest and the dropped collection stays dropped.
TEST_P(CollectionsEraserTest, erase_and_reset) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    flush_vbucket_to_disk(vbid, 4 /* 2x items */);

    // delete the collections and re-add a new dairy
    vb->updateFromManifest({cm.remove(CollectionEntry::fruit)
                                    .remove(CollectionEntry::dairy)
                                    .add(CollectionEntry::dairy2)});

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flush_vbucket_to_disk(vbid, 3 /* 3x system (2 deletes, 1 create) */);

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    vb.reset();
    resetEngineAndWarmup();

    if (!persistent()) {
        // The final expects only apply to persistent buckets that will remember
        // the collection state
        return;
    }

    // Now reset and warmup and expect the manifest to come back with the same
    // correct view of collections
    vb = store->getVBucket(vbid);
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, basic_deleted_items) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    delete_item(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    EXPECT_EQ(1, vb->getNumItems());

    // delete the collection
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        // The system event still exists as a tombstone and will reside in the
        // system until tombstone purging removes it.
        EXPECT_EQ(1, vb->getNumSystemItems());
    }
}

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, tombstone_cleaner) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    EXPECT_EQ(2, vb->getNumItems());

    // delete the collection
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        // The system event still exists as a tombstone and will reside in the
        // system until tombstone purging removes it.
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    // We're gonna have to kick ephemeral a bit to mark the collection tombstone
    // as stale. Travel forward in time then run the HTTombstonePurger.
    TimeTraveller c(10000000);
    if (!persistent()) {
        EphemeralVBucket::HTTombstonePurger purger(0);
        auto vbptr = store->getVBucket(vbid);
        EphemeralVBucket* evb = dynamic_cast<EphemeralVBucket*>(vbptr.get());
        purger.setCurrentVBucket(*evb);
        evb->ht.visit(purger);
    }

    // Now that we've run the tasks, we won't have any system events in the hash
    // table. The collection drop system event should still exist in the backing
    // store because it's the last item, but it won't be accounted for in the
    // NumSystemItems stat that looks at the hash table.
    EXPECT_EQ(0, vb->getNumSystemItems());
}

// Test that a collection erase "resumes" after a restart/warmup
TEST_P(CollectionsEraserTest, erase_after_warmup) {
    if (!persistent()) {
        return;
    }

    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    EXPECT_EQ(2, vb->getNumItems());

    // Evict one of the keys, we should still erase it
    evict_key(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});

    // delete the collection
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);
    vb.reset();

    store->cancelCompaction(vbid);
    resetEngineAndWarmup();

    // Now the eraser should ready to run, warmup will have noticed a dropped
    // collection in the manifest and schedule the eraser
    runCollectionsEraser();
    vb = store->getVBucket(vbid);
    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
}

static auto allConfigValues = ::testing::Values(
        std::make_tuple(std::string("ephemeral"), std::string("auto_delete")),
        std::make_tuple(std::string("ephemeral"), std::string("fail_new_data")),
        std::make_tuple(std::string("persistent"),
                        std::string("full_eviction")),
        std::make_tuple(std::string("persistent"), std::string("value_only")));

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_CASE_P(CollectionsEraserTests,
                        CollectionsEraserTest,
                        allConfigValues,
                        STParameterizedBucketTestPrintName());
