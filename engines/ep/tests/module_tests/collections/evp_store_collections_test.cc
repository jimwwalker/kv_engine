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

/**
 * Tests for Collection functionality in EPStore.
 */

#include "collections_test.h"

#include "bgfetcher.h"
#include "checkpoint_manager.h"
#include "collections/collections_types.h"
#include "ep_time.h"
#include "item.h"
#include "kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_couch_kvstore.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/evp_store_test.h"
#include "tests/module_tests/test_helpers.h"
#include <programs/engine_testapp/mock_cookie.h>

#include <folly/portability/GMock.h>

#include <engines/ep/tests/module_tests/vbucket_utils.h>
#include <spdlog/fmt/fmt.h>
#include <functional>
#include <thread>

TEST_P(CollectionsParameterizedTest, uid_increment) {
    CollectionsManifest cm{CollectionEntry::meat};
    EXPECT_EQ(store->setCollections(std::string{cm}).code(),
              cb::engine_errc::success);
    cm.add(CollectionEntry::vegetable);
    EXPECT_EQ(store->setCollections(std::string{cm}).code(),
              cb::engine_errc::success);
}

TEST_P(CollectionsParameterizedTest, uid_decrement) {
    CollectionsManifest cm{CollectionEntry::meat};
    EXPECT_EQ(store->setCollections(std::string{cm}).code(),
              cb::engine_errc::success);
    CollectionsManifest newCm{};
    EXPECT_EQ(store->setCollections(std::string{newCm}).code(),
              cb::engine_errc::out_of_range);
}

TEST_P(CollectionsParameterizedTest, uid_equal) {
    CollectionsManifest cm{CollectionEntry::meat};
    EXPECT_EQ(store->setCollections(std::string{cm}).code(),
              cb::engine_errc::success);

    // An equal manifest is tolerated (and ignored)
    EXPECT_EQ(store->setCollections(std::string{cm}).code(),
              cb::engine_errc::success);
}

TEST_P(CollectionsParameterizedTest, manifest_uid_equal_with_differences) {
    CollectionsManifest cm{CollectionEntry::meat};
    EXPECT_EQ(store->setCollections(std::string{cm}).code(),
              cb::engine_errc::success);

    auto uid = cm.getUid();
    cm.add(CollectionEntry::fruit);
    // force the uid back
    cm.updateUid(uid);
    // manifest is equal, but contains an extra collection, unexpected diversion
    EXPECT_EQ(store->setCollections(std::string{cm}).code(),
              cb::engine_errc::cannot_apply_collections_manifest);
}

// This test stores a key which matches what collections internally uses, but
// in a different namespace.
TEST_F(CollectionsTest, namespace_separation) {
    // Use the event factory to get an event which we'll borrow the key from
    auto se = SystemEventFactory::makeCollectionEvent(
            CollectionEntry::meat, {}, {});
    DocKey key(se->getKey().data(),
               se->getKey().size(),
               DocKeyEncodesCollectionId::No);

    store_item(vbid, key, "value");
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(std::string{cm});
    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flush_vbucket_to_disk(vbid, 2);

    // evict and load - should not see the system key for create collections
    evict_key(vbid, key);
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    GetValue gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    runBGFetcherTask();

    gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(0, strncmp("value", gv.item->getData(), gv.item->getNBytes()));
}

TEST_P(CollectionsParameterizedTest, collections_basic) {
    // Default collection is open for business
    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
    store_item(vbid,
               StoredDocKey{"meat:beef", CollectionEntry::meat},
               "value",
               0,
               {cb::engine_errc::unknown_collection});

    VBucketPtr vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(std::string{cm});

    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flushVBucketToDiskIfPersistent(vbid, 2);

    // System event not counted
    // Note: for persistent buckets, that is because
    // 1) It doesn't go in the hash-table
    // 2) It will only be accounted for on Full-Evict buckets after flush
    EXPECT_EQ(1, vb->getNumItems());

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    // Now we can write to beef
    store_item(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat}, "value");

    flushVBucketToDiskIfPersistent(vbid, 1);

    // And read a document from beef
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue gv = store->get(StoredDocKey{"meat:beef", CollectionEntry::meat},
                             vbid,
                             cookie,
                             options);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());

    // A key in meat that doesn't exist
    auto key1 = StoredDocKey{"meat:sausage", CollectionEntry::meat};
    EXPECT_EQ(ENGINE_KEY_ENOENT, checkKeyExists(key1, vbid, options));

    // Begin the deletion
    vb->updateFromManifest({cm.remove(CollectionEntry::meat)});

    // We should have deleted the create marker
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Access denied (although the item still exists)
    gv = store->get(StoredDocKey{"meat:beef", CollectionEntry::meat},
                    vbid,
                    cookie,
                    options);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, gv.getStatus());
}

// BY-ID update: This test was created for MB-25344 and is no longer relevant as
// we cannot 'hit' a logically deleted key from the front-end. This test has
// been adjusted to still provide some value.
TEST_F(CollectionsTest, unknown_collection_errors) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(std::string{cm});
    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 1);

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item1, cookie));
    flush_vbucket_to_disk(vbid, 1);

    auto item2 = make_item(vbid,
                           StoredDocKey{"dairy:cream", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item2, cookie));
    flush_vbucket_to_disk(vbid, 1);

    // Delete the dairy collection (so all dairy keys become logically deleted)
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

    // Re-add the dairy collection
    vb->updateFromManifest({cm.add(CollectionEntry::dairy2)});

    // Trigger a flush to disk. Flushes the dairy2 create event, dairy delete.
    flush_vbucket_to_disk(vbid, 2);

    // Expect that we cannot add item1 again, item1 has no collection
    item1.setCas(0);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, store->add(item1, cookie));

    // Replace should fail, item2 has no collection
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, store->replace(item2, cookie));

    // Delete should fail, item2 has no collection
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->deleteItem(item2.getKey(),
                                cas,
                                vbid,
                                cookie,
                                {},
                                nullptr,
                                mutation_descr));

    // Unlock should fail 'unknown-col' rather than an unlock error
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->unlockKey(
                      item2.getKey(), vbid, 0, ep_current_time(), cookie));

    EXPECT_EQ("collection_unknown",
              store->validateKey(
                      StoredDocKey{"meat:sausage", CollectionEntry::meat},
                      vbid,
                      item2));
    EXPECT_EQ("collection_unknown",
              store->validateKey(item2.getKey(), vbid, item2));

    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->statsVKey(
                      StoredDocKey{"meat:sausage", CollectionEntry::meat},
                      vbid,
                      cookie));
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->statsVKey(item2.getKey(), vbid, cookie));

    // GetKeyStats
    struct key_stats ks;
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->getKeyStats(
                      item2.getKey(), vbid, cookie, ks, WantsDeleted::No));
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->getKeyStats(
                      item2.getKey(), vbid, cookie, ks, WantsDeleted::Yes));

    uint32_t deleted = 0;
    uint8_t dtype = 0;
    ItemMetaData meta;
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->getMetaData(
                      item2.getKey(), vbid, cookie, meta, deleted, dtype));

    cas = 0;
    meta.cas = 1;
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->deleteWithMeta(item2.getKey(),
                                    cas,
                                    nullptr,
                                    vbid,
                                    cookie,
                                    {vbucket_state_active},
                                    CheckConflicts::No,
                                    meta,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::No,
                                    0,
                                    nullptr,
                                    DeleteSource::Explicit));

    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
              store->setWithMeta(item2,
                                 0,
                                 nullptr,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 false,
                                 GenerateBySeqno::Yes,
                                 GenerateCas::No));

    const char* msg = nullptr;
    EXPECT_EQ(cb::mcbp::Status::UnknownCollection,
              store->evictKey(item2.getKey(), vbid, &msg));
}

// BY-ID update: This test was created for MB-25344 and is no longer relevant as
// we cannot 'hit' a logically deleted key from the front-end. This test has
// been adjusted to still provide some value.
TEST_P(CollectionsParameterizedTest, GET_unknown_collection_errors) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(std::string{cm});
    // Trigger a flush to disk. Flushes the dairy create event.
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, addItem(item1, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Delete the dairy collection (so all dairy keys become logically deleted)
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

    // Re-add the dairy collection
    vb->updateFromManifest({cm.add(CollectionEntry::dairy2)});

    // Trigger a flush to disk. Flushes the dairy2 create event, dairy delete
    flushVBucketToDiskIfPersistent(vbid, 2);

    // The dairy:2 collection is empty
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);

    // Get deleted can't get it
    auto gv = store->get(StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                         vbid,
                         cookie,
                         options);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, gv.getStatus());

    options = static_cast<get_options_t>(QUEUE_BG_FETCH | HONOR_STATES |
                                         TRACK_REFERENCE | DELETE_TEMP |
                                         HIDE_LOCKED_CAS | TRACK_STATISTICS);

    // Normal Get can't get it
    gv = store->get(StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                    vbid,
                    cookie,
                    options);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, gv.getStatus());

    // Same for getLocked
    gv = store->getLocked(StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                          vbid,
                          ep_current_time(),
                          10,
                          cookie);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, gv.getStatus());

    // Same for getAndUpdateTtl
    gv = store->getAndUpdateTtl(
            StoredDocKey{"dairy:milk", CollectionEntry::dairy},
            vbid,
            cookie,
            ep_current_time() + 20);
    EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, gv.getStatus());
}

TEST_P(CollectionsParameterizedTest, get_collection_id) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::dairy);
    cm.add(ScopeEntry::shop2);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    std::string json = cm;
    store->setCollections(json);
    // Check bad 'paths'
    auto rv = store->getCollectionID("");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getCollectionID("..");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getCollectionID("a.b.c");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getCollectionID("dairy");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    // valid path, just illegal scope
    rv = store->getCollectionID("#illegal*.meat");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    // valid path, just illegal collection
    rv = store->getCollectionID("_default.#illegal*");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);

    // Unknowns
    rv = store->getCollectionID("shoppe.dairy");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);
    rv = store->getCollectionID(".unknown");
    EXPECT_EQ(cb::engine_errc::unknown_collection, rv.result);

    // Success cases next
    rv = store->getCollectionID(".");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(5, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::defaultC.getId(), rv.getCollectionId());

    rv = store->getCollectionID("_default.");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(5, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::defaultC.getId(), rv.getCollectionId());

    rv = store->getCollectionID("_default._default");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(5, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::defaultC.getId(), rv.getCollectionId());

    rv = store->getCollectionID(".dairy");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(5, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::dairy.getId(), rv.getCollectionId());

    rv = store->getCollectionID("_default.dairy");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(5, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::dairy.getId(), rv.getCollectionId());

    rv = store->getCollectionID("minimart.meat");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(5, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::meat.getId(), rv.getCollectionId());

    // Now we should fail getting _default
    cm.remove(CollectionEntry::defaultC);
    json = cm;
    store->setCollections(json);
    rv = store->getCollectionID(".");
    EXPECT_EQ(cb::engine_errc::unknown_collection, rv.result);
    rv = store->getCollectionID("._default");
    EXPECT_EQ(cb::engine_errc::unknown_collection, rv.result);
}

TEST_P(CollectionsParameterizedTest, get_scope_id) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::dairy, ScopeEntry::shop1);
    cm.add(ScopeEntry::shop2);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    std::string json = cm;
    store->setCollections(json);

    // Check bad 'paths', require 0 or 1 dot
    auto rv = store->getScopeID("..");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    // Check bad 'paths', require 0 or 1 dot
    rv = store->getScopeID("a.b.c");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);

    // Illegal scope names
    rv = store->getScopeID(" .");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getScopeID("#illegal*.");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getScopeID("#illegal*.ignored");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);

    // Valid path, unknown scopes
    rv = store->getScopeID("megamart");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);
    rv = store->getScopeID("megamart.collection");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);

    // Success cases next
    rv = store->getScopeID(""); // no dot = _default
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(6, rv.getManifestId());
    EXPECT_EQ(ScopeEntry::defaultS.getId(), rv.getScopeId());

    rv = store->getScopeID("."); // 1 dot = _default
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(6, rv.getManifestId());
    EXPECT_EQ(ScopeEntry::defaultS.getId(), rv.getScopeId());

    rv = store->getScopeID(ScopeEntry::shop1.name);
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(6, rv.getManifestId());
    EXPECT_EQ(ScopeEntry::shop1.getId(), rv.getScopeId());

    rv = store->getScopeID(ScopeEntry::shop2.name);
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(6, rv.getManifestId());
    EXPECT_EQ(ScopeEntry::shop2.getId(), rv.getScopeId());

    // Test the collection/vbucket lookup
    auto sid = store->getScopeID(CollectionEntry::dairy);
    EXPECT_TRUE(sid.second.has_value());
    EXPECT_EQ(ScopeEntry::shop1.uid, sid.second.value());

    sid = store->getScopeID(CollectionEntry::fruit);
    EXPECT_FALSE(sid.second.has_value());
}

// Test high seqno values
TEST_F(CollectionsTest, PersistedHighSeqno) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(std::string{cm});
    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 1);

    EXPECT_EQ(1,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item1, cookie));
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(2,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Mock a change in this document incrementing the high seqno
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item1, cookie));
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(3,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Check the set of a new item in the same collection increments the high
    // seqno for this collection
    auto item2 = make_item(vbid,
                           StoredDocKey{"dairy:cream", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item2, cookie));
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(4,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Check a deletion
    item2.setDeleted();
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item2, cookie));
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(5,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // No test of dropped collection as manifest removes the entry, so no seqno
    // is available for the dropped collection.
}

// Test persisted high seqno values with multiple collections
TEST_F(CollectionsTest, PersistedHighSeqnoMultipleCollections) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(std::string{cm});
    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 1);

    EXPECT_EQ(1,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item1, cookie));
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(2,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Add the meat collection
    cm.add(CollectionEntry::meat);
    vb->updateFromManifest(std::string{cm});
    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 1);

    EXPECT_EQ(3,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::meat.getId()));

    // Dairy should remain unchanged
    EXPECT_EQ(2,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Set a new item in meat
    auto item2 = make_item(vbid,
                           StoredDocKey{"meat:beef", CollectionEntry::meat},
                           "beefy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item2, cookie));
    flush_vbucket_to_disk(vbid, 1);
    // Skip 1 seqno for creation of meat
    EXPECT_EQ(4,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::meat.getId()));

    // Dairy should remain unchanged
    EXPECT_EQ(2,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Now, set a new high seqno in both collections in a single flush
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item1, cookie));
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item2, cookie));
    flush_vbucket_to_disk(vbid, 2);
    EXPECT_EQ(5,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));
    EXPECT_EQ(6,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::meat.getId()));

    // No test of dropped collection as manifest removes the entry, so no seqno
    // is available for the dropped collection.
}

// Test high seqno values
TEST_P(CollectionsParameterizedTest, HighSeqno) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm});

    // Flushing the manifest to disk guarantees that the database file
    // is written and exists, any subsequent bgfetches (e.g. during
    // addItem) will definitely be executed.
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, addItem(item1, cookie));
    EXPECT_EQ(2,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Mock a change in this document incrementing the high seqno
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item1, cookie));
    EXPECT_EQ(3,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Check the set of a new item in the same collection increments the high
    // seqno for this collection
    auto item2 = make_item(vbid,
                           StoredDocKey{"dairy:cream", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, addItem(item2, cookie));
    EXPECT_EQ(4,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Check a deletion
    item2.setDeleted();
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item2, cookie));
    EXPECT_EQ(5,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));
}

// Test high seqno values with multiple collections
TEST_P(CollectionsParameterizedTest, HighSeqnoMultipleCollections) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm});

    // Flushing the manifest to disk guarantees that the database file
    // is written and exists, any subsequent bgfetches (e.g. during
    // addItem) will definitely be executed.
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, addItem(item1, cookie));

    EXPECT_EQ(2,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Add the meat collection
    cm.add(CollectionEntry::meat);
    vb->updateFromManifest({cm});

    EXPECT_EQ(3,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::meat.getId()));

    // Dairy should remain unchanged
    EXPECT_EQ(2,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Set a new item in meat
    auto item2 = make_item(vbid,
                           StoredDocKey{"meat:beef", CollectionEntry::meat},
                           "beefy",
                           0,
                           0);
    EXPECT_EQ(ENGINE_SUCCESS, addItem(item2, cookie));

    // Skip 1 seqno for creation of meat
    EXPECT_EQ(4,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::meat.getId()));

    // Dairy should remain unchanged
    EXPECT_EQ(2,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Now, set a new high seqno in both collections in a single flush
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item1, cookie));
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item2, cookie));

    EXPECT_EQ(5,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));
    EXPECT_EQ(6,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::meat.getId()));
}

// Check that getRandomKey works correctly when given a random value of zero
TEST_P(CollectionsParameterizedTest, GetRandomKey) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm});
    flushVBucketToDiskIfPersistent(vbid, 1);
    StoredDocKey key{"milk", CollectionEntry::dairy};
    auto item = store_item(vbid, key, "1", 0);
    store_item(vbid, StoredDocKey{"stuff", CollectionEntry::defaultC}, "2", 0);
    flushVBucketToDiskIfPersistent(vbid, 2);
    auto gv = store->getRandomKey(CollectionEntry::dairy.getId(), cookie);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(item, *gv.item);
}

class CollectionsFlushTest : public CollectionsTest {
public:
    void SetUp() override {
        CollectionsTest::SetUp();
    }

    void collectionsFlusher(int items);

private:
    Collections::KVStore::Manifest createCollectionAndFlush(
            const std::string& json, CollectionID collection, int items);
    Collections::KVStore::Manifest dropCollectionAndFlush(
            const std::string& json, CollectionID collection, int items);

    void storeItems(CollectionID collection,
                    int items,
                    cb::engine_errc = cb::engine_errc::success);

    /**
     * Create manifest object from persisted manifest and validate if we can
     * write to the collection.
     * @param manifest Manifest to check
     * @param collection - a collection name to test for writing
     *
     * @return true if the collection can be written
     */
    static bool canWrite(const Collections::VB::Manifest& manifest,
                         CollectionID collection);

    /**
     * Create manifest object from persisted manifest and validate if we can
     * write to the collection.
     * @param manifest Manifest to check
     * @param collection - a collection name to test for writing
     *
     * @return true if the collection cannot be written
     */
    static bool cannotWrite(const Collections::VB::Manifest& manifest,
                            CollectionID collection);
};

void CollectionsFlushTest::storeItems(CollectionID collection,
                                      int items,
                                      cb::engine_errc expected) {
    for (int ii = 0; ii < items; ii++) {
        std::string key = "key" + std::to_string(ii);
        store_item(vbid, StoredDocKey{key, collection}, "value", 0, {expected});
    }
}

Collections::KVStore::Manifest CollectionsFlushTest::createCollectionAndFlush(
        const std::string& json, CollectionID collection, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    // cannot write to collection
    storeItems(collection, items, cb::engine_errc::unknown_collection);
    vb->updateFromManifest(json);
    storeItems(collection, items);
    flush_vbucket_to_disk(vbid, 1 + items); // create event + items
    EXPECT_EQ(items, vb->lockCollections().getItemCount(collection));
    return getManifest(vbid);
}

Collections::KVStore::Manifest CollectionsFlushTest::dropCollectionAndFlush(
        const std::string& json, CollectionID collection, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    storeItems(collection, items);
    vb->updateFromManifest(json);
    // cannot write to collection
    storeItems(collection, items, cb::engine_errc::unknown_collection);
    flush_vbucket_to_disk(vbid, 1 + items); // 1x del(create event) + items
    runCompaction();

    // Default is still ok
    storeItems(CollectionID::Default, items);
    flush_vbucket_to_disk(vbid, items); // just the items
    return getManifest(vbid);
}

bool CollectionsFlushTest::canWrite(const Collections::VB::Manifest& manifest,
                                    CollectionID collection) {
    std::string key = std::to_string(collection);
    return manifest.lock().doesKeyContainValidCollection(
            StoredDocKey{key, collection});
}

bool CollectionsFlushTest::cannotWrite(
        const Collections::VB::Manifest& manifest, CollectionID collection) {
    return !canWrite(manifest, collection);
}

/**
 * Drive manifest state changes through the test's vbucket
 *  1. Validate the flusher flushes the expected items
 *  2. Validate the updated collections manifest changes
 *  3. Use a validator function to check if a collection is (or is not)
 *     writeable
 */
void CollectionsFlushTest::collectionsFlusher(int items) {
    struct testFuctions {
        std::function<Collections::KVStore::Manifest()> function;
        std::function<bool(const Collections::VB::Manifest&)> validator;
    };

    CollectionsManifest cm(CollectionEntry::meat);
    using std::placeholders::_1;
    // Setup the test using a vector of functions to run
    std::vector<testFuctions> test{
            // First 2 steps - add,delete for the meat collection
            {// 0
             std::bind(&CollectionsFlushTest::createCollectionAndFlush,
                       this,
                       cm,
                       CollectionEntry::meat,
                       items),
             std::bind(&CollectionsFlushTest::canWrite,
                       _1,
                       CollectionEntry::meat)},

            {// 1
             std::bind(&CollectionsFlushTest::dropCollectionAndFlush,
                       this,
                       cm.remove(CollectionEntry::meat),
                       CollectionEntry::meat,
                       items),
             std::bind(&CollectionsFlushTest::cannotWrite,
                       _1,
                       CollectionEntry::meat)},

            // Final 3 steps - add,delete,add for the fruit collection
            {// 2
             std::bind(&CollectionsFlushTest::createCollectionAndFlush,
                       this,
                       cm.add(CollectionEntry::dairy),
                       CollectionEntry::dairy,
                       items),
             std::bind(&CollectionsFlushTest::canWrite,
                       _1,
                       CollectionEntry::dairy)},
            {// 3
             std::bind(&CollectionsFlushTest::dropCollectionAndFlush,
                       this,
                       cm.remove(CollectionEntry::dairy),
                       CollectionEntry::dairy,
                       items),
             std::bind(&CollectionsFlushTest::cannotWrite,
                       _1,
                       CollectionEntry::dairy)},
            {// 4
             std::bind(&CollectionsFlushTest::createCollectionAndFlush,
                       this,
                       cm.add(CollectionEntry::dairy2),
                       CollectionEntry::dairy2,
                       items),
             std::bind(&CollectionsFlushTest::canWrite,
                       _1,
                       CollectionEntry::dairy2)}};

    auto m1 = std::make_unique<Collections::VB::Manifest>();
    int step = 0;
    for (auto& f : test) {
        auto m2 = std::make_unique<Collections::VB::Manifest>(f.function());
        // The manifest should change for each step
        EXPECT_NE(*m1, *m2) << "Failed step:" + std::to_string(step) << "\n"
                            << *m1 << "\n should not match " << *m2 << "\n";
        EXPECT_TRUE(f.validator(*m2))
                << "Failed at step:" << std::to_string(step) << " validating "
                << *m2;
        m1.swap(m2);
        step++;
    }
}

TEST_F(CollectionsFlushTest, collections_flusher_no_items) {
    collectionsFlusher(0);
}

TEST_F(CollectionsFlushTest, collections_flusher_with_items) {
    collectionsFlusher(3);
}

class CollectionsWarmupTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override {
        // Enable collections (which will enable namespace persistence).
        config_string += "collections_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }
};

// Test item counting when we store/delete flush and store again
TEST_P(CollectionsParameterizedTest, MB_31212) {
    CollectionsManifest cm;
    auto vb = store->getVBucket(vbid);

    vb->updateFromManifest({cm.add(CollectionEntry::meat)});
    auto key = StoredDocKey{"beef", CollectionEntry::meat};
    // Now we can write to meat
    store_item(vbid, key, "value");
    delete_item(vbid, key);

    // Trigger a flush to disk. Flushes the meat create event and the delete
    flushVBucketToDiskIfPersistent(vbid, 2);

    // 0 items, we only have a delete on disk
    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::meat));

    // Store the same key again and expect 1 item
    store_item(vbid, StoredDocKey{"beef", CollectionEntry::meat}, "value");

    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));
}

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmup) {
    CollectionsManifest cm;
    uint32_t uid = 0xface2;
    cm.setUid(uid);
    {
        auto vb = store->getVBucket(vbid);

        // add performs a +1 on the manifest uid
        vb->updateFromManifest({cm.add(CollectionEntry::meat)});

        // Trigger a flush to disk. Flushes the meat create event
        flush_vbucket_to_disk(vbid, 1);

        // Now we can write to beef
        store_item(vbid,
                   StoredDocKey{"meat:beef", CollectionEntry::meat},
                   "value");
        // But not dairy
        store_item(vbid,
                   StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                   "value",
                   0,
                   {cb::engine_errc::unknown_collection});

        flush_vbucket_to_disk(vbid, 1);

        EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));
        EXPECT_EQ(2,
                  vb->lockCollections().getPersistedHighSeqno(
                          CollectionEntry::meat));
        EXPECT_EQ(2, vb->lockCollections().getHighSeqno(CollectionEntry::meat));
        EXPECT_EQ(2,
                  store->getVBucket(vbid)->lockCollections().getHighSeqno(
                          CollectionEntry::meat));

        // create an extra collection which we do not write to (note uid++)
        vb->updateFromManifest({cm.add(CollectionEntry::fruit)});
        flush_vbucket_to_disk(vbid, 1);

        // The high-seqno of the collection is the start, the seqno of the
        // creation event
        EXPECT_EQ(3,
                  store->getVBucket(vbid)->lockCollections().getHighSeqno(
                          CollectionEntry::fruit));
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    // validate the manifest uid comes back as expected
    EXPECT_EQ(uid + 2,
              store->getVBucket(vbid)->lockCollections().getManifestUid());

    // validate we warmup the item count and high seqnos
    EXPECT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionEntry::meat));
    EXPECT_EQ(2,
              store->getVBucket(vbid)->lockCollections().getPersistedHighSeqno(
                      CollectionEntry::meat));
    EXPECT_EQ(2,
              store->getVBucket(vbid)->lockCollections().getHighSeqno(
                      CollectionEntry::meat));

    {
        Item item(StoredDocKey{"meat:beef", CollectionEntry::meat},
                  /*flags*/ 0,
                  /*exp*/ 0,
                  "rare",
                  sizeof("rare"));
        item.setVBucketId(vbid);
        uint64_t cas;
        EXPECT_EQ(ENGINE_SUCCESS,
                  engine->storeInner(cookie, item, cas, OPERATION_SET, false));
    }
    {
        Item item(StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                  /*flags*/ 0,
                  /*exp*/ 0,
                  "skimmed",
                  sizeof("skimmed"));
        item.setVBucketId(vbid);
        uint64_t cas;
        EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
                  engine->storeInner(cookie, item, cas, OPERATION_SET, false));
    }

    EXPECT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionEntry::meat));

    // Now what about the other collections, we still have the default and fruit
    // They were never written to but should come back with sensible state
    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionEntry::fruit));
    EXPECT_EQ(3,
              store->getVBucket(vbid)->lockCollections().getPersistedHighSeqno(
                      CollectionEntry::fruit));
    EXPECT_EQ(3,
              store->getVBucket(vbid)->lockCollections().getHighSeqno(
                      CollectionEntry::fruit));

    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionEntry::defaultC));
    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getPersistedHighSeqno(
                      CollectionEntry::defaultC));
    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getHighSeqno(
                      CollectionEntry::defaultC));
}

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmupIgnoreLogicallyDeleted) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection
        CollectionsManifest cm(CollectionEntry::meat);
        vb->updateFromManifest({cm});

        // Trigger a flush to disk. Flushes the meat create event
        flush_vbucket_to_disk(vbid, 1);
        const int nitems = 10;
        for (int ii = 0; ii < nitems; ii++) {
            // Now we can write to beef
            std::string key = "meat:" + std::to_string(ii);
            store_item(vbid, StoredDocKey{key, CollectionEntry::meat}, "value");
        }

        flush_vbucket_to_disk(vbid, nitems);

        // Remove the meat collection
        vb->updateFromManifest({cm.remove(CollectionEntry::meat)});

        flush_vbucket_to_disk(vbid, 1);

        // Items still exist until the eraser runs
        EXPECT_EQ(nitems, vb->ht.getNumInMemoryItems());

        // Ensure collection purge has executed
        runCollectionsEraser();

        EXPECT_EQ(0, vb->ht.getNumInMemoryItems());
    } // VBucketPtr scope ends


    resetEngineAndWarmup();

    EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumInMemoryItems());
    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::meat));
}

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmupIgnoreLogicallyDeletedDefault) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection
        CollectionsManifest cm(CollectionEntry::meat);
        vb->updateFromManifest({cm});

        // Trigger a flush to disk. Flushes the meat create event
        flush_vbucket_to_disk(vbid, 1);
        const int nitems = 10;
        for (int ii = 0; ii < nitems; ii++) {
            std::string key = "key" + std::to_string(ii);
            store_item(vbid,
                       StoredDocKey{key, CollectionEntry::defaultC},
                       "value");
        }

        flush_vbucket_to_disk(vbid, nitems);

        // Remove the default collection
        vb->updateFromManifest({cm.remove(CollectionEntry::defaultC)});

        flush_vbucket_to_disk(vbid, 1);

        // Items still exist until the eraser runs
        EXPECT_EQ(nitems, vb->ht.getNumInMemoryItems());

        // But no manifest level stats exist
        EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(
                CollectionEntry::defaultC));

        // Ensure collection purge has executed
        runCollectionsEraser();

        EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumInMemoryItems());
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumInMemoryItems());

    // meat collection still exists
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::meat));
}

TEST_F(CollectionsWarmupTest, warmupManifestUidLoadsOnCreate) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection
        CollectionsManifest cm;
        cm.setUid(0xface2); // cm.add will +1 this uid
        vb->updateFromManifest({cm.add(CollectionEntry::meat)});

        flush_vbucket_to_disk(vbid, 1);
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    // validate the manifest uid comes back
    EXPECT_EQ(0xface2 + 1,
              store->getVBucket(vbid)->lockCollections().getManifestUid());
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::meat));
}

TEST_F(CollectionsWarmupTest, warmupManifestUidLoadsOnDelete) {
    {
        auto vb = store->getVBucket(vbid);

        // Delete the $default collection
        CollectionsManifest cm;
        cm.setUid(0xface2); // cm.remove will +1 this uid
        vb->updateFromManifest({cm.remove(CollectionEntry::defaultC)});

        flush_vbucket_to_disk(vbid, 1);
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    // validate the manifest uid comes back
    EXPECT_EQ(0xface2 + 1,
              store->getVBucket(vbid)->lockCollections().getManifestUid());
}

// Set the manifest before warmup runs, without the fix, the manifest wouldn't
// get applied to the active vbucket
TEST_F(CollectionsWarmupTest, MB_38125) {
    resetEngineAndEnableWarmup();

    CollectionsManifest cm(CollectionEntry::fruit);
    store->setCollections(std::string{cm});

    // Now get the engine warmed up
    runReadersUntilWarmedUp();

    auto vb = store->getVBucket(vbid);

    // Fruit is enabled
    EXPECT_TRUE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"grape", CollectionEntry::fruit}));
}

/**
 * Test checks that setCollections propagates the collection data to active
 * vbuckets.
 */
TEST_P(CollectionsParameterizedTest, basic) {
    // Add some more VBuckets just so there's some iteration happening
    const int extraVbuckets = 2;
    for (int vb = vbid.get() + 1; vb <= (vbid.get() + extraVbuckets); vb++) {
        store->setVBucketState(Vbid(vb), vbucket_state_active);
    }

    CollectionsManifest cm(CollectionEntry::meat);
    store->setCollections(std::string{cm});

    // Check all vbuckets got the collections
    for (int vb = vbid.get(); vb <= (vbid.get() + extraVbuckets); vb++) {
        auto vbp = store->getVBucket(Vbid(vb));
        EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                StoredDocKey{"meat:bacon", CollectionEntry::meat}));
        EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                StoredDocKey{"anykey", CollectionEntry::defaultC}));
    }
}

/**
 * Test checks that setCollections propagates the collection data to active
 * vbuckets and not the replicas
 */
TEST_P(CollectionsParameterizedTest, basic2) {
    // Add some more VBuckets just so there's some iteration happening
    const int extraVbuckets = 2;
    // Add active and replica
    for (int vb = vbid.get() + 1; vb <= (vbid.get() + extraVbuckets); vb++) {
        if (vb & 1) {
            store->setVBucketState(Vbid(vb), vbucket_state_active);
        } else {
            store->setVBucketState(Vbid(vb), vbucket_state_replica);
        }
    }

    CollectionsManifest cm(CollectionEntry::meat);
    store->setCollections(std::string{cm});

    // Check all vbuckets got the collections
    for (int vb = vbid.get(); vb <= (vbid.get() + extraVbuckets); vb++) {
        auto vbp = store->getVBucket(Vbid(vb));
        if (vbp->getState() == vbucket_state_active) {
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    StoredDocKey{"meat:bacon", CollectionEntry::meat}));
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    StoredDocKey{"anykey", CollectionEntry::defaultC}));
        } else {
            // Replica will be in default constructed settings
            EXPECT_FALSE(vbp->lockCollections().doesKeyContainValidCollection(
                    StoredDocKey{"meat:bacon", CollectionEntry::meat}));
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    StoredDocKey{"anykey", CollectionEntry::defaultC}));
        }
    }
}

// Test the compactor doesn't generate expired items for a dropped collection
TEST_F(CollectionsTest, collections_expiry_after_drop_collection_compaction) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add the meat collection + 1 item with TTL (and flush it all out)
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest({cm});
    StoredDocKey key{"lamb", CollectionEntry::meat};
    store_item(vbid, key, "value", ep_real_time() + 100);
    flush_vbucket_to_disk(vbid, 2);
    // And now drop the meat collection
    vb->updateFromManifest({cm.remove(CollectionEntry::meat)});
    flush_vbucket_to_disk(vbid, 1);

    // Time travel
    TimeTraveller docBrown(2000);

    // Now compact to force expiry of our little lamb
    runCompaction();

    std::vector<queued_item> items;
    vb->checkpointManager->getNextItemsForPersistence(items);

    // No mutation of the original key is allowed as it would invalidate the
    // ordering of create @x, item @y, drop @z  x < y < z
    for (auto& i : items) {
        EXPECT_NE(key, i->getKey());
    }
}

TEST_F(CollectionsTest, CollectionAddedAndRemovedBeforePersistence) {
    /**
     * MB-38528: Test that setPersistedHighSeqno when called when persisting a
     * collection creation event does not throw if the collection is not
     * found.
     * In the noted MB a replica received a collection creation and collection
     * drop very quickly after. By the time the creation had persisted, the drop
     * had already removed the collection from the vb manifest.
     */
    replaceCouchKVStoreWithMock();
    VBucketPtr vb = store->getVBucket(vbid);

    // Add the dairy collection, but don't flush it just yet.
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(std::string{cm});

    // set a hook to be called immediately before the flusher commits to disk.
    // This is after items have been read from the checkpoint manager, but
    // before the items are persisted - importantly in this case, before
    // saveDocsCallback is invoked (which calls setPersistedHighSeqno())
    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));
    kvstore.setPreCommitHook([&cm, &vb] {
        // now remove the collection. This will remove it from the vb manifest
        // _before_ the creation event tries to call setPersistedHighSeqno()
        cm.remove(CollectionEntry::dairy);
        vb->updateFromManifest(std::string{cm});
    });
    // flushing the creation to disk should not throw, even though the
    // collection was not found in the manifest
    EXPECT_NO_THROW(flush_vbucket_to_disk(vbid, 1));
}

// Test the pager doesn't generate expired items for a dropped collection
TEST_P(CollectionsParameterizedTest,
       collections_expiry_after_drop_collection_pager) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add the meat collection + 1 item with TTL (and flush it all out)
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest({cm});
    StoredDocKey key{"lamb", CollectionEntry::meat};
    store_item(vbid, key, "value", ep_real_time() + 100);
    flushVBucketToDiskIfPersistent(vbid, 2);
    // And now drop the meat collection
    vb->updateFromManifest({cm.remove(CollectionEntry::meat)});
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Time travel
    TimeTraveller docBrown(2000);

    // Now run the pager to force expiry of our little lamb
    auto task = std::make_shared<ExpiredItemPager>(
            engine.get(), engine->getEpStats(), 0);
    static_cast<ExpiredItemPager*>(task.get())->run();
    runNextTask(*task_executor->getLpTaskQ()[NONIO_TASK_IDX],
                "Expired item remover on vb:0");

    std::vector<queued_item> items;
    vb->checkpointManager->getNextItemsForPersistence(items);

    // No mutation of the original key is allowed as it would invalidate the
    // ordering of create @x, item @y, drop @z  x < y < z
    for (auto& i : items) {
        EXPECT_NE(key, i->getKey());
    }
}

// Test to ensure the callback passed to engine->get_connection_manifest(...)
// will track any allocations against "non-bucket"
TEST_P(CollectionsParameterizedTest,
       GetCollectionManifestResponseCBAllocsUnderNonBucket) {
    auto addResponseFn = [](std::string_view key,
                            std::string_view extras,
                            std::string_view body,
                            uint8_t datatype,
                            cb::mcbp::Status status,
                            uint64_t cas,
                            const void* cookie) -> bool {
        // This callback should run in the memcached-context - there should be
        // no associated engine.
        EXPECT_FALSE(ObjectRegistry::getCurrentEngine());
        return true;
    };
    engine->get_collection_manifest(cookie, addResponseFn);
}

class CollectionsExpiryLimitTest : public CollectionsTest,
                                   public ::testing::WithParamInterface<bool> {
public:
    void SetUp() override {
        config_string += "max_ttl=86400";
        CollectionsTest::SetUp();
    }

    void operation_test(
            std::function<void(Vbid, DocKey, std::string)> storeFunc,
            bool warmup);
};

void CollectionsExpiryLimitTest::operation_test(
        std::function<void(Vbid, DocKey, std::string)> storeFunc, bool warmup) {
    CollectionsManifest cm;
    // meat collection defines no expiry (overriding bucket ttl)
    cm.add(CollectionEntry::meat, std::chrono::seconds(0));
    // fruit defines nothing, gets bucket ttl
    cm.add(CollectionEntry::fruit);
    // dairy has its own expiry, greater than bucket
    cm.add(CollectionEntry::dairy, std::chrono::seconds(500000));
    // vegetable has its own expiry, less than bucket
    cm.add(CollectionEntry::vegetable, std::chrono::seconds(380));

    {
        VBucketPtr vb = store->getVBucket(vbid);
        vb->updateFromManifest({cm});
    }

    flush_vbucket_to_disk(vbid, 4);

    if (warmup) {
        resetEngineAndWarmup();
    }

    StoredDocKey meaty{"lamb", CollectionEntry::meat};
    StoredDocKey fruity{"apple", CollectionEntry::fruit};
    StoredDocKey milky{"milk", CollectionEntry::dairy};
    StoredDocKey potatoey{"potato", CollectionEntry::vegetable};

    storeFunc(vbid, meaty, "meaty");
    storeFunc(vbid, fruity, "fruit");
    storeFunc(vbid, milky, "milky");
    storeFunc(vbid, potatoey, "potatoey");

    auto f = [](const item_info&) { return true; };

    // verify meaty has 0 expiry
    auto rval = engine->getIfInner(cookie, meaty, vbid, f);
    ASSERT_EQ(cb::engine_errc::success, rval.first);
    Item* i = reinterpret_cast<Item*>(rval.second.get());
    auto info = engine->getItemInfo(*i);
    EXPECT_EQ(0, info.exptime);

    // Now the rest, we expect fruity to have the bucket ttl
    // we can expect milky to be > fruity
    // we can expect potatoey to be < fruity
    auto fruityValue = engine->getIfInner(cookie, fruity, vbid, f);
    auto milkyValue = engine->getIfInner(cookie, milky, vbid, f);
    auto potatoeyValue = engine->getIfInner(cookie, potatoey, vbid, f);
    ASSERT_EQ(cb::engine_errc::success, fruityValue.first);
    ASSERT_EQ(cb::engine_errc::success, milkyValue.first);
    ASSERT_EQ(cb::engine_errc::success, potatoeyValue.first);

    auto fruityInfo = engine->getItemInfo(
            *reinterpret_cast<Item*>(fruityValue.second.get()));
    auto milkyInfo = engine->getItemInfo(
            *reinterpret_cast<Item*>(milkyValue.second.get()));
    auto potatoeyInfo = engine->getItemInfo(
            *reinterpret_cast<Item*>(potatoeyValue.second.get()));

    EXPECT_NE(0, fruityInfo.exptime);
    EXPECT_NE(0, milkyInfo.exptime);
    EXPECT_NE(0, potatoeyInfo.exptime);
    EXPECT_GT(milkyInfo.exptime, fruityInfo.exptime);
    EXPECT_LT(potatoeyInfo.exptime, fruityInfo.exptime);
}

TEST_P(CollectionsExpiryLimitTest, set) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        auto item = make_item(vb, k, v);
        EXPECT_EQ(0, item.getExptime());
        EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsExpiryLimitTest, add) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        auto item = make_item(vb, k, v);
        EXPECT_EQ(0, item.getExptime());
        EXPECT_EQ(ENGINE_SUCCESS, store->add(item, cookie));
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsExpiryLimitTest, replace) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        auto item = make_item(vb, k, v);
        EXPECT_EQ(0, item.getExptime());
        EXPECT_EQ(ENGINE_SUCCESS, store->add(item, cookie));
        EXPECT_EQ(ENGINE_SUCCESS, store->replace(item, cookie));
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsExpiryLimitTest, set_with_meta) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        auto item = make_item(vb, k, v);
        item.setCas(1);
        EXPECT_EQ(0, item.getExptime());
        uint64_t cas = 0;
        uint64_t seqno = 0;
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->setWithMeta(item,
                                     cas,
                                     &seqno,
                                     cookie,
                                     {vbucket_state_active},
                                     CheckConflicts::No,
                                     true,
                                     GenerateBySeqno::Yes,
                                     GenerateCas::No,
                                     nullptr));
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsExpiryLimitTest, gat) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        Item item = store_item(vb, k, v, 0);

        // re touch to 0
        auto rval = engine->getAndTouchInner(cookie, k, vb, 0);
        ASSERT_EQ(cb::engine_errc::success, rval.first);
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsParameterizedTest, item_counting) {
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest({cm});

    // Default collection is open for business
    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");

    // 1 system event + 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::meat));

    store_item(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat}, "value");
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));

    // Now modify our two items
    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));

    store_item(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat}, "value");
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));

    // Now delete our two items
    delete_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC});
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));

    delete_item(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat});
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::meat));
}

TEST_F(CollectionsTest, CollectionStatsIncludesScope) {
    // Test that stats returned for key "collections" includes what scope
    // the collection is in
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::dairy, ScopeEntry::shop1);
    cm.add(ScopeEntry::shop2);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    cm.add(CollectionEntry::fruit, ScopeEntry::shop2);
    std::string json = cm;
    store->setCollections(json);

    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 5);

    const auto makeStatPair = [](const ScopeEntry::Entry& scope,
                                 const CollectionEntry::Entry& collection) {
        // scope name is present in all collection stats, arbitrarily check the
        // ID stat exists and contains the scope name.
        return std::make_pair(fmt::format("{}:{}:scope_name",
                                          scope.getId().to_string(),
                                          collection.getId().to_string()),
                              scope.name);
    };

    std::map<std::string, std::string> expected{
            makeStatPair(ScopeEntry::defaultS, CollectionEntry::defaultC),
            makeStatPair(ScopeEntry::shop1, CollectionEntry::dairy),
            makeStatPair(ScopeEntry::shop2, CollectionEntry::meat),
            makeStatPair(ScopeEntry::shop2, CollectionEntry::fruit)};

    std::map<std::string, std::string> actual;
    const auto addStat = [&actual](std::string_view key,
                                   std::string_view value,
                                   gsl::not_null<const void*> cookie) {
        actual[std::string(key)] = value;
    };

    auto cookie = create_mock_cookie();
    engine->doCollectionStats(cookie, addStat, "collections");
    destroy_mock_cookie(cookie);

    using namespace testing;

    for (const auto& exp : expected) {
        // newer GTest brings IsSubsetOf which could replace this
        EXPECT_THAT(actual, Contains(exp));
    }
}

/**
 * RAII helper to check the per-collection memory usage changes in the expected
 * manner.
 *
 *
 * Checks that the memory usage when the helper is destroyed vs when it was
 * created meets the given invariant. E.g.,
 *  {
 *      auto x = MemChecker(*vb, CollectionEntry::defaultC, std::greater<>());
 *      // do something which should change the mem used of the default
 *      // collection as tracked by the hash table statistics.
 *  }
 *
 * This checks that when `x` goes out of scope, the memory usage of the default
 * collection is _greater_ than when the checker was constructed.
 */
class MemChecker {
public:
    using Func = std::function<bool(size_t, size_t)>;

    MemChecker(VBucket& vb,
               const CollectionEntry::Entry& entry,
               Func postCondition)
        : vb(vb), entry(entry), postCondition(std::move(postCondition)) {
        initialMemUsed = getCollectionMemUsed();
    };
    ~MemChecker() {
        auto newMemUsed = getCollectionMemUsed();
        EXPECT_TRUE(postCondition(newMemUsed, initialMemUsed))
                << "Memory usage for collection: " << entry.name
                << " did not meet expected condition";
    }

private:
    size_t getCollectionMemUsed() {
        const auto& stats = VBucketTestIntrospector::getStats(vb);
        return stats.getCollectionMemUsed(entry.uid);
    }

    VBucket& vb;
    const CollectionEntry::Entry& entry;
    Func postCondition;
    size_t initialMemUsed = 0;
};

TEST_F(CollectionsTest, PerCollectionMemUsed) {
    // test that the per-collection memory usage (tracked by the hash table
    // statistics) changes when items in the collection are
    // added/updated/deleted/evicted and does not change when items in other
    // collections are similarly changed.
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest({cm});

    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    {
        SCOPED_TRACE("new item added to collection");
        // default collection memory usage should _increase_
        auto d = MemChecker(*vb, CollectionEntry::defaultC, std::greater<>());
        // meta collection memory usage should _stay the same_
        auto m = MemChecker(*vb, CollectionEntry::meat, std::equal_to<>());

        store_item(
                vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("new item added to collection");
        auto d = MemChecker(*vb, CollectionEntry::defaultC, std::equal_to<>());
        auto m = MemChecker(*vb, CollectionEntry::meat, std::greater<>());

        store_item(vbid,
                   StoredDocKey{"meat:beef", CollectionEntry::meat},
                   "value");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("update item with larger value");
        auto d = MemChecker(*vb, CollectionEntry::defaultC, std::greater<>());
        auto m = MemChecker(*vb, CollectionEntry::meat, std::equal_to<>());

        store_item(vbid,
                   StoredDocKey{"key", CollectionEntry::defaultC},
                   "valuesdfasdfasdfasdfasdfsadf");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("delete item");
        auto d = MemChecker(*vb, CollectionEntry::defaultC, std::less<>());
        auto m = MemChecker(*vb, CollectionEntry::meat, std::equal_to<>());

        delete_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC});
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("evict item");
        auto d = MemChecker(*vb, CollectionEntry::defaultC, std::equal_to<>());
        auto m = MemChecker(*vb, CollectionEntry::meat, std::less<>());

        evict_key(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat});
    }
}

// Test to ensure we use the vbuckets manifest when passing a vbid to
// EventuallyPersistentEngine::get_scope_id()
TEST_F(CollectionsTest, GetScopeIdForGivenKeyAndVbucket) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection to vbid(0)
    CollectionsManifest cmDairyVb;
    cmDairyVb.add(ScopeEntry::shop1)
            .add(CollectionEntry::dairy, ScopeEntry::shop1);
    vb->updateFromManifest(std::string{cmDairyVb});

    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(vbid, 2);

    StoredDocKey keyDairy{"dairy:milk", CollectionEntry::dairy};
    StoredDocKey keyMeat{"meat:beef", CollectionEntry::meat};

    auto result = engine->get_scope_id(cookie, keyDairy, vbid);
    EXPECT_EQ(cb::engine_errc::success, result.result);
    EXPECT_EQ(cmDairyVb.getUid(), result.getManifestId());
    EXPECT_EQ(ScopeID(ScopeEntry::shop1), result.getScopeId());

    result = engine->get_scope_id(cookie, keyMeat, vbid);
    EXPECT_EQ(cb::engine_errc::unknown_collection, result.result);
    EXPECT_EQ(0, result.getManifestId());

    StoredDocKey keyFruit{"fruit:apple", CollectionEntry::fruit};
    // Add the meat collection to vbid(1)
    Vbid meatVbid(1);

    ASSERT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(meatVbid, vbucket_state_replica));
    auto replicaVb = store->getVBucket(meatVbid);

    result = engine->get_scope_id(cookie, keyDairy, meatVbid);
    EXPECT_EQ(cb::engine_errc::unknown_collection, result.result);
    EXPECT_EQ(0, result.getManifestId());

    replicaVb->checkpointManager->createSnapshot(
            0, 2, std::nullopt, CheckpointType::Memory, 3);
    replicaVb->replicaAddScope(1, ScopeUid::shop1, ScopeName::shop1, 1);
    replicaVb->replicaAddCollection(
            2,
            {ScopeUid::shop1, CollectionEntry::meat.getId()},
            CollectionEntry::meat.name,
            {},
            2);
    // Trigger a flush to disk. Flushes the dairy create event.
    flush_vbucket_to_disk(meatVbid, 2);

    result = engine->get_scope_id(cookie, keyMeat, meatVbid);
    EXPECT_EQ(cb::engine_errc::success, result.result);
    EXPECT_EQ(2, result.getManifestId());
    EXPECT_EQ(ScopeUid::shop1, result.getScopeId());

    result = engine->get_scope_id(cookie, keyFruit, meatVbid);
    EXPECT_EQ(cb::engine_errc::unknown_collection, result.result);
    EXPECT_EQ(0, result.getManifestId());

    // check vbucket that doesnt exist
    result = engine->get_scope_id(cookie, keyDairy, Vbid(10));
    EXPECT_EQ(cb::engine_errc::not_my_vbucket, result.result);
}

INSTANTIATE_TEST_SUITE_P(CollectionsExpiryLimitTests,
                         CollectionsExpiryLimitTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(CollectionsEphemeralOrPersistent,
                         CollectionsParameterizedTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
