/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include "checkpoint_test.h"
#include "checkpoint_test_impl.h"

#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "ep_types.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "tests/module_tests/test_helpers.h"
#include "thread_gate.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <valgrind/valgrind.h>

#include <thread>

#define NUM_DCP_THREADS 3
#define NUM_DCP_THREADS_VG 2
#define NUM_SET_THREADS 4
#define NUM_SET_THREADS_VG 2

#define NUM_ITEMS 500
#define NUM_ITEMS_VG 10

#define DCP_CURSOR_PREFIX "dcp-client-"

template <typename V>
CheckpointTest<V>::CheckpointTest()
    : callback(new DummyCB()),
      vbucket(new V(Vbid(0),
                    vbucket_state_active,
                    global_stats,
                    checkpoint_config,
                    /*kvshard*/ NULL,
                    /*lastSeqno*/ 1000,
                    /*lastSnapStart*/ 0,
                    /*lastSnapEnd*/ 0,
                    std::make_unique<FailoverTable>(1),
                    callback,
                    /*newSeqnoCb*/ nullptr,
                    SyncWriteResolvedCallback{},
                    NoopSyncWriteCompleteCb,
                    NoopSeqnoAckCb,
                    config,
                    EvictionPolicy::Value,
                    std::make_unique<Collections::VB::Manifest>())) {
    createManager();
}

template <typename V>
void CheckpointTest<V>::createManager(int64_t last_seqno) {
    manager.reset(new MockCheckpointManager(global_stats,
                                        this->vbucket->getId(),
                                        checkpoint_config,
                                        last_seqno,
                                        /*lastSnapStart*/ 0,
                                        /*lastSnapEnd*/ 0,
                                        callback));

    // Sanity check initial state.
    ASSERT_EQ(1, this->manager->getNumOfCursors());
    ASSERT_EQ(0, this->manager->getNumOpenChkItems());
    ASSERT_EQ(1, this->manager->getNumCheckpoints());
}

template <typename V>
bool CheckpointTest<V>::queueNewItem(const std::string& key) {
    queued_item qi{new Item(makeStoredDocKey(key),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 0,
                            /*bySeq*/ 0)};
    return this->manager->queueDirty(*this->vbucket,
                                     qi,
                                     GenerateBySeqno::Yes,
                                     GenerateCas::Yes,
                                     /*preLinkDocCtx*/ nullptr);
}

struct thread_args {
    VBucket* vbucket;
    MockCheckpointManager *checkpoint_manager;
    Cursor cursor;
    ThreadGate& gate;
};

static void launch_persistence_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    args->gate.threadUp();

    bool flush = false;
    while(true) {
        size_t itemPos;
        std::vector<queued_item> items;
        auto* cursor = args->checkpoint_manager->getPersistenceCursor();
        args->checkpoint_manager->getNextItemsForCursor(cursor, items);
        for(itemPos = 0; itemPos < items.size(); ++itemPos) {
            queued_item qi = items.at(itemPos);
            if (qi->getOperation() == queue_op::flush) {
                flush = true;
                break;
            }
        }
        if (flush) {
            // Checkpoint start and end operations may have been introduced in
            // the items queue after the "flush" operation was added. Ignore
            // these. Anything else will be considered an error.
            for(size_t i = itemPos + 1; i < items.size(); ++i) {
                queued_item qi = items.at(i);
                EXPECT_TRUE(queue_op::checkpoint_start == qi->getOperation() ||
                            queue_op::checkpoint_end == qi->getOperation())
                    << "Unexpected operation:" << to_string(qi->getOperation());
            }
            break;
        }
        // yield to allow set thread to actually do some useful work.
        std::this_thread::yield();
    }
    EXPECT_TRUE(flush);
}

static void launch_dcp_client_thread(void* arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    args->gate.threadUp();

    bool flush = false;
    bool isLastItem = false;
    while(true) {
        queued_item qi = args->checkpoint_manager->nextItem(
                args->cursor.lock().get(), isLastItem);
        if (qi->getOperation() == queue_op::flush) {
            flush = true;
            break;
        }
        // yield to allow set thread to actually do some useful work.
        std::this_thread::yield();
    }
    EXPECT_TRUE(flush);
}

static void launch_checkpoint_cleanup_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    args->gate.threadUp();

    while (args->checkpoint_manager->getNumOfCursors() > 1) {
        bool newCheckpointCreated;
        args->checkpoint_manager->removeClosedUnrefCheckpoints(
                *args->vbucket, newCheckpointCreated);
        // yield to allow set thread to actually do some useful work.
        std::this_thread::yield();
    }
}

static void launch_set_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    args->gate.threadUp();

    int i(0);
    for (i = 0; i < NUM_ITEMS; ++i) {
        std::string key = "key-" + std::to_string(i);
        queued_item qi(new Item(makeStoredDocKey(key),
                                args->vbucket->getId(),
                                queue_op::mutation,
                                0,
                                0));
        args->checkpoint_manager->queueDirty(*args->vbucket,
                                             qi,
                                             GenerateBySeqno::Yes,
                                             GenerateCas::Yes,
                                             /*preLinkDocCtx*/ nullptr);
    }
}

TYPED_TEST_CASE(CheckpointTest, VBucketTypes);

TYPED_TEST(CheckpointTest, basic_chk_test) {
    std::shared_ptr<Callback<Vbid> > cb(new DummyCB());
    this->vbucket.reset(
            new TypeParam(Vbid(0),
                          vbucket_state_active,
                          this->global_stats,
                          this->checkpoint_config,
                          NULL,
                          0,
                          0,
                          0,
                          NULL,
                          cb,
                          /*newSeqnoCb*/ nullptr,
                          SyncWriteResolvedCallback{},
                          NoopSyncWriteCompleteCb,
                          NoopSeqnoAckCb,
                          this->config,
                          EvictionPolicy::Value,
                          std::make_unique<Collections::VB::Manifest>()));

    this->manager.reset(new MockCheckpointManager(
            this->global_stats, Vbid(0), this->checkpoint_config, 1, 0, 0, cb));

    const size_t n_set_threads = RUNNING_ON_VALGRIND ? NUM_SET_THREADS_VG :
                                                       NUM_SET_THREADS;

    const size_t n_dcp_threads =
            RUNNING_ON_VALGRIND ? NUM_DCP_THREADS_VG : NUM_DCP_THREADS;

    std::vector<cb_thread_t> dcp_threads(n_dcp_threads);
    std::vector<cb_thread_t> set_threads(n_set_threads);
    cb_thread_t persistence_thread;
    cb_thread_t checkpoint_cleanup_thread;
    int rc(0);

    const size_t n_threads{n_set_threads + n_dcp_threads + 2};
    ThreadGate gate{n_threads};
    thread_args t_args{this->vbucket.get(), this->manager.get(), {}, gate};

    std::vector<thread_args> dcp_t_args;
    for (size_t i = 0; i < n_dcp_threads; ++i) {
        std::string name(DCP_CURSOR_PREFIX + std::to_string(i));
        auto cursorRegResult = this->manager->registerCursorBySeqno(name, 0);
        dcp_t_args.emplace_back(thread_args{this->vbucket.get(),
                                            this->manager.get(),
                                            cursorRegResult.cursor,
                                            gate});
    }

    rc = cb_create_thread(&persistence_thread, launch_persistence_thread, &t_args, 0);
    EXPECT_EQ(0, rc);

    rc = cb_create_thread(&checkpoint_cleanup_thread,
                        launch_checkpoint_cleanup_thread, &t_args, 0);
    EXPECT_EQ(0, rc);

    for (size_t i = 0; i < n_dcp_threads; ++i) {
        rc = cb_create_thread(
                &dcp_threads[i], launch_dcp_client_thread, &dcp_t_args[i], 0);
        EXPECT_EQ(0, rc);
    }

    for (size_t i = 0; i < n_set_threads; ++i) {
        rc = cb_create_thread(&set_threads[i], launch_set_thread, &t_args, 0);
        EXPECT_EQ(0, rc);
    }

    for (size_t i = 0; i < n_set_threads; ++i) {
        rc = cb_join_thread(set_threads[i]);
        EXPECT_EQ(0, rc);
    }

    // Push the flush command into the queue so that all other threads can be terminated.
    queued_item qi(new Item(makeStoredDocKey("flush"),
                            this->vbucket->getId(),
                            queue_op::flush,
                            0xffff,
                            0));
    this->manager->queueDirty(*this->vbucket,
                              qi,
                              GenerateBySeqno::Yes,
                              GenerateCas::Yes,
                              /*preLinkDocCtx*/ nullptr);

    rc = cb_join_thread(persistence_thread);
    EXPECT_EQ(0, rc);

    for (size_t i = 0; i < n_dcp_threads; ++i) {
        rc = cb_join_thread(dcp_threads[i]);
        EXPECT_EQ(0, rc);
        this->manager->removeCursor(dcp_t_args[i].cursor.lock().get());
    }

    rc = cb_join_thread(checkpoint_cleanup_thread);
    EXPECT_EQ(0, rc);
}

// Sanity check test fixture
TYPED_TEST(CheckpointTest, CheckFixture) {
    // Initially have a single cursor (persistence).
    EXPECT_EQ(1, this->manager->getNumOfCursors());
    EXPECT_EQ(0, this->manager->getNumOpenChkItems());
    // Should initially be zero items to persist.
    EXPECT_EQ(0, this->manager->getNumItemsForPersistence());

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForPersistence(items);
    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(0, result.range.getEnd());
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
}

// Basic test of a single, open checkpoint.
TYPED_TEST(CheckpointTest, OneOpenCkpt) {
    // Queue a set operation.
    queued_item qi(new Item(makeStoredDocKey("key1"),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 20,
                            /*bySeq*/ 0));

    // No set_ops in queue, expect queueDirty to return true (increase
    // persistence queue size).
    EXPECT_TRUE(this->manager->queueDirty(*this->vbucket,
                                          qi,
                                          GenerateBySeqno::Yes,
                                          GenerateCas::Yes,
                                          /*preLinkDocCtx*/ nullptr));
    EXPECT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    // 1x op_set
    EXPECT_EQ(1, this->manager->getNumOpenChkItems());
    EXPECT_EQ(1001, qi->getBySeqno());
    EXPECT_EQ(20, qi->getRevSeqno());
    EXPECT_EQ(1, this->manager->getNumItemsForPersistence());

    // Adding the same key again shouldn't increase the size.
    queued_item qi2(new Item(makeStoredDocKey("key1"),
                             this->vbucket->getId(),
                             queue_op::mutation,
                             /*revSeq*/ 21,
                             /*bySeq*/ 0));
    EXPECT_FALSE(this->manager->queueDirty(*this->vbucket,
                                           qi2,
                                           GenerateBySeqno::Yes,
                                           GenerateCas::Yes,
                                           /*preLinkDocCtx*/ nullptr));
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(1, this->manager->getNumOpenChkItems());
    EXPECT_EQ(1002, qi2->getBySeqno());
    EXPECT_EQ(21, qi2->getRevSeqno());
    EXPECT_EQ(1, this->manager->getNumItemsForPersistence());

    // Adding a different key should increase size.
    queued_item qi3(new Item(makeStoredDocKey("key2"),
                             this->vbucket->getId(),
                             queue_op::mutation,
                             /*revSeq*/ 0,
                             /*bySeq*/ 0));
    EXPECT_TRUE(this->manager->queueDirty(*this->vbucket,
                                          qi3,
                                          GenerateBySeqno::Yes,
                                          GenerateCas::Yes,
                                          /*preLinkDocCtx*/ nullptr));
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());
    EXPECT_EQ(1003, qi3->getBySeqno());
    EXPECT_EQ(0, qi3->getRevSeqno());
    EXPECT_EQ(2, this->manager->getNumItemsForPersistence());

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForPersistence(items);
    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1003, result.range.getEnd());
    EXPECT_EQ(3, items.size());
    EXPECT_THAT(items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::mutation)));
}

// Test that enqueuing a single delete works.
TYPED_TEST(CheckpointTest, Delete) {
    // Enqueue a single delete.
    queued_item qi{new Item{makeStoredDocKey("key1"),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 10,
                            /*byseq*/ 0}};
    qi->setDeleted();
    EXPECT_TRUE(this->manager->queueDirty(*this->vbucket,
                                          qi,
                                          GenerateBySeqno::Yes,
                                          GenerateCas::Yes,
                                          /*preLinkDocCtx*/ nullptr));

    EXPECT_EQ(1, this->manager->getNumCheckpoints());  // Single open checkpoint.
    EXPECT_EQ(1, this->manager->getNumOpenChkItems()); // 1x op_del
    EXPECT_EQ(1001, qi->getBySeqno());
    EXPECT_EQ(10, qi->getRevSeqno());

    // Check that the items fetched matches what was enqueued.
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForPersistence(items);

    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1001, result.range.getEnd());
    ASSERT_EQ(2, items.size());
    EXPECT_THAT(items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation)));
    EXPECT_TRUE(items[1]->isDeleted());
}


// Test with one open and one closed checkpoint.
TYPED_TEST(CheckpointTest, OneOpenOneClosed) {
    // Add some items to the initial (open) checkpoint.
    for (auto i : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    // 2x op_set
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());
    const uint64_t ckpt_id1 = this->manager->getOpenCheckpointId();

    // Create a new checkpoint (closing the current open one).
    const uint64_t ckpt_id2 = this->manager->createNewCheckpoint();
    EXPECT_NE(ckpt_id1, ckpt_id2) << "New checkpoint ID should differ from old";
    EXPECT_EQ(ckpt_id1, this->manager->getLastClosedCheckpointId());
    EXPECT_EQ(0, this->manager->getNumOpenChkItems()); // no items yet

    // Add some items to the newly-opened checkpoint (note same keys as 1st
    // ckpt).
    for (auto ii : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    // 2x op_set
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());

    // Examine the items - should be 2 lots of two keys.
    EXPECT_EQ(4, this->manager->getNumItemsForPersistence());

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForPersistence(items);
    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1004, result.range.getEnd());
    EXPECT_EQ(7, items.size());
    EXPECT_THAT(items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::checkpoint_end),
                                     HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::mutation)));
}

// Test the automatic creation of checkpoints based on the number of items.
TYPED_TEST(CheckpointTest, ItemBasedCheckpointCreation) {
    // Size down the default number of items to create a new checkpoint and
    // recreate the manager
    this->checkpoint_config = CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                                               MIN_CHECKPOINT_ITEMS,
                                               /*numCheckpoints*/ 2,
                                               /*itemBased*/ true,
                                               /*keepClosed*/ false,
                                               /*persistenceEnabled*/ true);
    // TODO: ^^ Consider a variant for Ephemeral testing -
    // persistenceEnabled:false

    this->createManager();

    // Create one less than the number required to create a new checkpoint.
    queued_item qi;
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_EQ(ii, this->manager->getNumOpenChkItems());

        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
    }

    // Add one more - should create a new checkpoint.
    EXPECT_TRUE(this->queueNewItem("key_epoch"));
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(1, this->manager->getNumOpenChkItems()); // 1x op_set

    // Fill up this checkpoint also - note loop for MIN_CHECKPOINT_ITEMS - 1
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS - 1; ii++) {
        EXPECT_EQ(ii + 1,
                  this->manager->getNumOpenChkItems()); /* +1 initial set */

        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));

        EXPECT_EQ(2, this->manager->getNumCheckpoints());
    }

    // Add one more - as we have hit maximum checkpoints should *not* create a
    // new one.
    EXPECT_TRUE(this->queueNewItem("key_epoch2"));
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(11, // 1x key_epoch, 9x key_X, 1x key_epoch2
              this->manager->getNumOpenChkItems());

    // Fetch the items associated with the persistence cursor. This
    // moves the single cursor registered outside of the initial checkpoint,
    // allowing a new open checkpoint to be created.
    EXPECT_EQ(1, this->manager->getNumOfCursors());
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForPersistence(items);

    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1021, result.range.getEnd());
    EXPECT_EQ(24, items.size());

    // Should still have the same number of checkpoints and open items.
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(11, this->manager->getNumOpenChkItems());

    // But adding a new item will create a new one.
    EXPECT_TRUE(this->queueNewItem("key_epoch3"));
    EXPECT_EQ(3, this->manager->getNumCheckpoints());
    EXPECT_EQ(1, this->manager->getNumOpenChkItems()); // 1x op_set
}

// Test checkpoint and cursor accounting - when checkpoints are closed the
// offset of cursors is updated as appropriate.
TYPED_TEST(CheckpointTest, CursorOffsetOnCheckpointClose) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    // 2x op_set
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());

    // Use the existing persistence cursor for this test:
    EXPECT_EQ(2, this->manager->getNumItemsForPersistence())
            << "Cursor should initially have two items pending";

    // Check de-dupe counting - after adding another item with the same key,
    // should still see two items.
    EXPECT_FALSE(this->queueNewItem("key1")) << "Adding a duplicate key to "
                                                "open checkpoint should not "
                                                "increase queue size";

    EXPECT_EQ(2, this->manager->getNumItemsForPersistence())
            << "Expected 2 items for cursor (2x op_set) after adding a "
               "duplicate.";

    // Create a new checkpoint (closing the current open one).
    this->manager->createNewCheckpoint();
    EXPECT_EQ(0, this->manager->getNumOpenChkItems());
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(2, this->manager->getNumItemsForPersistence())
            << "Expected 2 items for cursor after creating new checkpoint";

    // Advance persistence cursor - first to get the 'checkpoint_start' meta
    // item, and a second time to get the a 'proper' mutation.
    bool isLastMutationItem;
    auto item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                        isLastMutationItem);
    EXPECT_TRUE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    EXPECT_EQ(2, this->manager->getNumItemsForPersistence())
            << "Expected 2 items for cursor after advancing one item";

    item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                   isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    EXPECT_EQ(1, this->manager->getNumItemsForPersistence())
            << "Expected 1 item for cursor after advancing by 1";

    // Add two items to the newly-opened checkpoint. Same keys as 1st ckpt,
    // but cannot de-dupe across checkpoints.
    for (auto ii : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    EXPECT_EQ(3, this->manager->getNumItemsForPersistence())
            << "Expected 3 items for cursor after adding 2 more to new "
               "checkpoint";

    // Advance the cursor 'out' of the first checkpoint.
    item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                   isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_TRUE(isLastMutationItem);

    // Now at the end of the first checkpoint, move into the next checkpoint.
    item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                   isLastMutationItem);
    EXPECT_TRUE(item->isCheckPointMetaItem());
    EXPECT_TRUE(isLastMutationItem);
    item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                   isLastMutationItem);
    EXPECT_TRUE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);

    // Tell Checkpoint manager the items have been persisted, so it advances
    // pCursorPreCheckpointId, which will allow us to remove the closed
    // unreferenced checkpoints.
    this->manager->itemsPersisted();

    // Both previous checkpoints are unreferenced. Close them. This will
    // cause the offset of this cursor to be recalculated.
    bool new_open_ckpt_created;
    EXPECT_EQ(2,
              this->manager->removeClosedUnrefCheckpoints(
                      *this->vbucket, new_open_ckpt_created));

    EXPECT_EQ(1, this->manager->getNumCheckpoints());

    EXPECT_EQ(2, this->manager->getNumItemsForPersistence());

    // Drain the remaining items.
    item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                   isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                   isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_TRUE(isLastMutationItem);

    EXPECT_EQ(0, this->manager->getNumItemsForPersistence());
}

// Test the getNextItemsForCursor()
TYPED_TEST(CheckpointTest, ItemsForCheckpointCursor) {
    /* We want to have items across 2 checkpoints. Size down the default number
       of items to create a new checkpoint and recreate the manager */
    this->checkpoint_config = CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                                               MIN_CHECKPOINT_ITEMS,
                                               /*numCheckpoints*/ 2,
                                               /*itemBased*/ true,
                                               /*keepClosed*/ false,
                                               /*persistenceEnabled*/ true);
    // TODO: ^^ Consider a variant for Ephemeral testing -
    // persistenceEnabled:false

    this->createManager();

    /* Add items such that we have 2 checkpoints */
    queued_item qi;
    for (unsigned int ii = 0; ii < 2 * MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /* Check if we have desired number of checkpoints and desired number of
       items */
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS, this->manager->getNumOpenChkItems());

    /* Register DCP replication cursor */
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor =
            this->manager->registerCursorBySeqno(dcp_cursor.c_str(), 0);

    /* Get items for persistence*/
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForPersistence(items);

    /* We should have got (2 * MIN_CHECKPOINT_ITEMS + 3) items. 3 additional are
       op_ckpt_start, op_ckpt_end and op_ckpt_start */
    EXPECT_EQ(2 * MIN_CHECKPOINT_ITEMS + 3, items.size());
    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1000 + 2 * MIN_CHECKPOINT_ITEMS, result.range.getEnd());

    /* Get items for DCP replication cursor */
    items.clear();
    result = this->manager->getNextItemsForCursor(dcpCursor.cursor.lock().get(),
                                                  items);
    EXPECT_EQ(2 * MIN_CHECKPOINT_ITEMS + 3, items.size());
    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1000 + 2 * MIN_CHECKPOINT_ITEMS, result.range.getEnd());
}

// Test getNextItemsForCursor() when it is limited to fewer items than exist
// in total. Cursor should only advanced to the start of the 2nd checkpoint.
TYPED_TEST(CheckpointTest, ItemsForCheckpointCursorLimited) {
    /* We want to have items across 2 checkpoints. Size down the default number
       of items to create a new checkpoint and recreate the manager */
    this->checkpoint_config = CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                                               MIN_CHECKPOINT_ITEMS,
                                               /*numCheckpoints*/ 2,
                                               /*itemBased*/ true,
                                               /*keepClosed*/ false,
                                               /*persistenceEnabled*/ true);

    this->createManager();

    /* Add items such that we have 2 checkpoints */
    queued_item qi;
    for (unsigned int ii = 0; ii < 2 * MIN_CHECKPOINT_ITEMS; ii++) {
        ASSERT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /* Verify we have desired number of checkpoints and desired number of
       items */
    ASSERT_EQ(2, this->manager->getNumCheckpoints());
    ASSERT_EQ(MIN_CHECKPOINT_ITEMS, this->manager->getNumOpenChkItems());

    /* Get items for persistence. Specify a limit of 1 so we should only
     * fetch the first checkpoints' worth.
     */
    std::vector<queued_item> items;
    auto result = this->manager->getItemsForPersistence(items, 1);
    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.range.getEnd());
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS + 2, items.size())
            << "Should have MIN_CHECKPOINT_ITEMS + 2 (ckpt start & end) items";
    EXPECT_EQ(2, this->manager->getPersistenceCursor()->getId())
            << "Cursor should have moved into second checkpoint.";
}

// Test the checkpoint cursor movement
TYPED_TEST(CheckpointTest, CursorMovement) {
    /* We want to have items across 2 checkpoints. Size down the default number
     of items to create a new checkpoint and recreate the manager */
    this->checkpoint_config = CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                                               MIN_CHECKPOINT_ITEMS,
                                               /*numCheckpoints*/ 2,
                                               /*itemBased*/ true,
                                               /*keepClosed*/ false,
                                               /*persistenceEnabled*/true);
    // TODO: ^^ Consider a variant for Ephemeral testing -
    // persistenceEnabled:false

    this->createManager();

    /* Add items such that we have 1 full (max items as per config) checkpoint.
       Adding another would open new checkpoint */
    queued_item qi;
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /* Check if we have desired number of checkpoints and desired number of
       items */
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS, this->manager->getNumOpenChkItems());

    /* Register DCP replication cursor */
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor =
            this->manager->registerCursorBySeqno(dcp_cursor.c_str(), 0);

    /* Get items for persistence cursor */
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForPersistence(items);

    /* We should have got (MIN_CHECKPOINT_ITEMS + op_ckpt_start) items. */
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS + 1, items.size());
    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.range.getEnd());

    /* Get items for DCP replication cursor */
    items.clear();
    result = this->manager->getNextItemsForCursor(dcpCursor.cursor.lock().get(),
                                                  items);
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS + 1, items.size());
    EXPECT_EQ(0, result.range.getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.range.getEnd());

    uint64_t curr_open_chkpt_id = this->manager->getOpenCheckpointId();

    /* Run the checkpoint remover so that new open checkpoint is created */
    bool newCheckpointCreated;
    this->manager->removeClosedUnrefCheckpoints(*this->vbucket,
                                                newCheckpointCreated);
    EXPECT_EQ(curr_open_chkpt_id + 1, this->manager->getOpenCheckpointId());

    /* Get items for persistence cursor */
    EXPECT_EQ(0, this->manager->getNumItemsForPersistence())
            << "Expected to have no normal (only meta) items";
    items.clear();
    result = this->manager->getNextItemsForPersistence(items);

    /* We should have got op_ckpt_start item */
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.range.getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.range.getEnd());

    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());

    /* Get items for DCP replication cursor */
    EXPECT_EQ(0, this->manager->getNumItemsForPersistence())
            << "Expected to have no normal (only meta) items";
    items.clear();
    this->manager->getNextItemsForCursor(dcpCursor.cursor.lock().get(), items);
    /* Expecting only 1 op_ckpt_start item */
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
}

// MB-25056 - Regression test replicating situation where the seqno returned by
// registerCursorBySeqno minus one is greater than the input parameter
// startBySeqno but a backfill is not required.
TYPED_TEST(CheckpointTest, MB25056_backfill_not_required) {
    std::vector<queued_item> items;
    this->vbucket->setState(vbucket_state_replica);

    ASSERT_TRUE(this->queueNewItem("key0"));
    // Add duplicate items, which should cause de-duplication to occur.
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_FALSE(this->queueNewItem("key0"));
    }
    // Add a number of non duplicate items to the same checkpoint
    for (unsigned int ii = 1; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    // Register DCP replication cursor
    std::string dcp_cursor(DCP_CURSOR_PREFIX);
    // Request to register the cursor with a seqno that has been de-duped away
    CursorRegResult result =
            this->manager->registerCursorBySeqno(dcp_cursor.c_str(), 1005);
    EXPECT_EQ(1011, result.seqno) << "Returned seqno is not expected value.";
    EXPECT_FALSE(result.tryBackfill) << "Backfill is unexpectedly required.";
}

//
// It's critical that the HLC (CAS) is ordered with seqno generation
// otherwise XDCR may drop a newer bySeqno mutation because the CAS is not
// higher.
//
TYPED_TEST(CheckpointTest, SeqnoAndHLCOrdering) {
    const int n_threads = 8;
    const int n_items = 1000;

    // configure so we can store a large number of items
    // configure with 1 checkpoint to ensure the time-based closing
    // does not split the items over many checkpoints and muddy the final
    // data checks.
    this->checkpoint_config = CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                                               n_threads * n_items,
                                               /*numCheckpoints*/ 1,
                                               /*itemBased*/ true,
                                               /*keepClosed*/ false,
                                               /*persistenceEnabled*/true);
    // TODO: ^^ Consider a variant for Ephemeral testing -
    // persistenceEnabled:false

    this->createManager();

    std::vector<std::thread> threads;

    // vector of pairs, first is seqno, second is CAS
    // just do a scatter gather over n_threads
    std::vector<std::vector<std::pair<uint64_t, uint64_t> > > threadData(n_threads);
    for (int ii = 0; ii < n_threads; ii++) {
        auto& threadsData = threadData[ii];
        threads.push_back(std::thread([this, ii, n_items, &threadsData](){
            std::string key = "key" + std::to_string(ii);
            for (int item  = 0; item < n_items; item++) {
                queued_item qi(
                        new Item(makeStoredDocKey(key + std::to_string(item)),
                                 this->vbucket->getId(),
                                 queue_op::mutation,
                                 /*revSeq*/ 0,
                                 /*bySeq*/ 0));
                EXPECT_TRUE(
                        this->manager->queueDirty(*this->vbucket,
                                                  qi,
                                                  GenerateBySeqno::Yes,
                                                  GenerateCas::Yes,
                                                  /*preLinkDocCtx*/ nullptr));

                // Save seqno/cas
                threadsData.push_back(std::make_pair(qi->getBySeqno(), qi->getCas()));
            }
        }));
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Now combine the data and check HLC is increasing with seqno
    std::map<uint64_t, uint64_t> finalData;
    for (auto t : threadData) {
        for (auto pair : t) {
            EXPECT_EQ(finalData.end(), finalData.find(pair.first));
            finalData[pair.first] = pair.second;
        }
    }

    auto itr = finalData.begin();
    EXPECT_NE(itr, finalData.end());
    uint64_t previousCas = (itr++)->second;
    EXPECT_NE(itr, finalData.end());
    for (; itr != finalData.end(); itr++) {
        EXPECT_LT(previousCas, itr->second);
        previousCas = itr->second;
    }

    // Now a final check, iterate the checkpoint and also check for increasing
    // HLC.
    std::vector<queued_item> items;
    this->manager->getNextItemsForPersistence(items);

    /* We should have got (n_threads*n_items + op_ckpt_start) items. */
    EXPECT_EQ(n_threads*n_items + 1, items.size());

    previousCas = items[1]->getCas();
    for (size_t ii = 2; ii < items.size(); ii++) {
        EXPECT_LT(previousCas, items[ii]->getCas());
        previousCas = items[ii]->getCas();
    }
}

// Test cursor is correctly updated when enqueuing a key which already exists
// in the checkpoint (and needs de-duping), where the cursor points at a
// meta-item at the head of the checkpoint:
//
//  Before:
//      Checkpoint [ 0:EMPTY(), 1:CKPT_START(), 1:SET(key), 2:SET_VBSTATE() ]
//                                                               ^
//                                                            Cursor
//
//  After:
//      Checkpoint [ 0:EMPTY(), 1:CKPT_START(), 2:SET_VBSTATE(), 2:SET(key) ]
//                                                     ^
//                                                   Cursor
//
TYPED_TEST(CheckpointTest, CursorUpdateForExistingItemWithMetaItemAtHead) {
    // Setup the checkpoint and cursor.
    ASSERT_EQ(1, this->manager->getNumItems());
    ASSERT_TRUE(this->queueNewItem("key"));
    ASSERT_EQ(2, this->manager->getNumItems());
    this->manager->queueSetVBState(*this->vbucket);

    ASSERT_EQ(3, this->manager->getNumItems());

    // Advance persistence cursor so all items have been consumed.
    std::vector<queued_item> items;
    this->manager->getNextItemsForPersistence(items);
    ASSERT_EQ(3, items.size());
    ASSERT_EQ(0, this->manager->getNumItemsForPersistence());

    // Queue an item with a duplicate key.
    this->queueNewItem("key");

    // Test: Should have one item for cursor (the one we just added).
    EXPECT_EQ(1, this->manager->getNumItemsForPersistence());

    // Should have another item to read (new version of 'key')
    items.clear();
    this->manager->getNextItemsForPersistence(items);
    EXPECT_EQ(1, items.size());
}

// Test cursor is correctly updated when enqueuing a key which already exists
// in the checkpoint (and needs de-duping), where the cursor points at a
// meta-item *not* at the head of the checkpoint:
//
//  Before:
//      Checkpoint [ 0:EMPTY(), 1:CKPT_START(), 1:SET_VBSTATE(key), 1:SET() ]
//                                                     ^
//                                                    Cursor
//
//  After:
//      Checkpoint [ 0:EMPTY(), 1:CKPT_START(), 1:SET_VBSTATE(key), 2:SET() ]
//                                                     ^
//                                                   Cursor
//
TYPED_TEST(CheckpointTest, CursorUpdateForExistingItemWithNonMetaItemAtHead) {
    // Setup the checkpoint and cursor.
    ASSERT_EQ(1, this->manager->getNumItems());
    this->manager->queueSetVBState(*this->vbucket);
    ASSERT_EQ(2, this->manager->getNumItems());

    // Advance persistence cursor so all items have been consumed.
    std::vector<queued_item> items;
    this->manager->getNextItemsForPersistence(items);
    ASSERT_EQ(2, items.size());
    ASSERT_EQ(0, this->manager->getNumItemsForPersistence());

    // Queue a set (cursor will now be one behind).
    ASSERT_TRUE(this->queueNewItem("key"));
    ASSERT_EQ(1, this->manager->getNumItemsForPersistence());

    // Test: queue an item with a duplicate key.
    this->queueNewItem("key");

    // Test: Should have one item for cursor (the one we just added).
    EXPECT_EQ(1, this->manager->getNumItemsForPersistence());

    // Should an item to read (new version of 'key')
    items.clear();
    this->manager->getNextItemsForPersistence(items);
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(1002, items.at(0)->getBySeqno());
    EXPECT_EQ(makeStoredDocKey("key"), items.at(0)->getKey());
}

// Regression test for MB-21925 - when a duplicate key is queued and the
// persistence cursor is still positioned on the initial dummy key,
// should return SuccessExistingItem.
TYPED_TEST(CheckpointTest,
           MB21925_QueueDuplicateWithPersistenceCursorOnInitialMetaItem) {
    // Need a manager starting from seqno zero.
    this->createManager(0);
    ASSERT_EQ(0, this->manager->getHighSeqno());
    ASSERT_EQ(1, this->manager->getNumItems())
            << "Should start with queue_op::empty on checkpoint.";

    // Add an item with some new key.
    ASSERT_TRUE(this->queueNewItem("key"));

    // Test - second item (duplicate key) should return false.
    EXPECT_FALSE(this->queueNewItem("key"));
}

/*
 * Test modified following formal removal of backfill queue. Now the test
 * demonstrates an initial disk backfill being received and completed and that
 * all items enter the checkpoint. On completion of the snapshot no new
 * checkpoint is created, only a new snapshot will do that.
 */
TEST_F(SingleThreadedCheckpointTest, CloseReplicaCheckpointOnDiskSnapshotEnd) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* ckptMgr =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    ASSERT_TRUE(ckptMgr);

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    // We must have only 1 open checkpoint with id=0 (set by setVBucketState)
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 0);
    // We must have only one cursor (the persistence cursor), as there is no
    // DCP producer for vbid
    EXPECT_EQ(ckptMgr->getNumOfCursors(), 1);
    // We must have only the checkpoint-open and the vbucket-state
    // meta-items in the open checkpoint
    EXPECT_EQ(ckptList.back()->getNumItems(), 0);
    EXPECT_EQ(ckptMgr->getNumItems(), 2);

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test-consumer");
    auto passiveStream = std::static_pointer_cast<MockPassiveStream>(
            consumer->makePassiveStream(
                    *engine,
                    consumer,
                    "test-passive-stream",
                    0 /* flags */,
                    0 /* opaque */,
                    vbid,
                    0 /* startSeqno */,
                    std::numeric_limits<uint64_t>::max() /* endSeqno */,
                    0 /* vbUuid */,
                    0 /* snapStartSeqno */,
                    0 /* snapEndSeqno */,
                    0 /* vb_high_seqno */,
                    {} /* vb_manifest_uid */));

    uint64_t snapshotStart = 1;
    const uint64_t snapshotEnd = 10;

    uint32_t flags = dcp_marker_flag_t::MARKER_FLAG_DISK;

    // 1) the consumer receives the snapshot-marker
    SnapshotMarker snapshotMarker(0 /* opaque */,
                                  vbid,
                                  snapshotStart,
                                  snapshotEnd,
                                  flags,
                                  {} /*HCS*/,
                                  {});
    passiveStream->processMarker(&snapshotMarker);

    // We must have 1 open checkpoint with id=1
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 1);

    // 2) the consumer receives the mutations until (snapshotEnd -1)
    processMutations(*passiveStream, snapshotStart, snapshotEnd - 1);

    // We must have again 1 open checkpoint with id=1
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 1);
    EXPECT_EQ(snapshotEnd - 1, ckptMgr->getNumOpenChkItems());

    // 3) the consumer receives the snapshotEnd mutation
    processMutations(*passiveStream, snapshotEnd, snapshotEnd);

    // We must have again 1 open checkpoint with id=1
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 1);
    EXPECT_EQ(snapshotEnd, ckptMgr->getNumOpenChkItems());

    // 4) the consumer receives a second snapshot-marker
    SnapshotMarker snapshotMarker2(0 /* opaque */,
                                   vbid,
                                   snapshotEnd + 1,
                                   snapshotEnd + 2,
                                   dcp_marker_flag_t::MARKER_FLAG_CHK,
                                   {} /*HCS*/,
                                   {} /*SID*/);
    passiveStream->processMarker(&snapshotMarker2);
    EXPECT_EQ(ckptList.size(), 2);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 2);
    EXPECT_EQ(0, ckptMgr->getNumOpenChkItems());

    store->deleteVBucket(vb->getId(), cookie);
}

/*
 * Only if (mem_used > high_wat), then we expect that a Consumer closes the
 * open checkpoint and creates a new one when a PassiveStream receives the
 * snapshotEnd mutation for both:
 *     - memory-snapshot
 *     - disk-snapshot && vbHighSeqno > 0, which is processed as memory-snapshot
 *
 * Note that the test executes 4 combinations in total:
 *     {mem-snap, disk-snap} x {lowMemUsed, highMemUsed}
 */
void SingleThreadedCheckpointTest::closeReplicaCheckpointOnMemorySnapshotEnd(
        bool highMemUsed, uint32_t flags) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* ckptMgr =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    ASSERT_TRUE(ckptMgr);

    EPStats& stats = engine->getEpStats();
    if (highMemUsed) {
        // Simulate (mem_used > high_wat) by setting high_wat=0
        stats.mem_high_wat.store(0);
    }
    int openedCheckPoints = 1;
    // We must have only 1 open checkpoint
    EXPECT_EQ(openedCheckPoints, ckptMgr->getNumCheckpoints());
    // We must have only one cursor (the persistence cursor), as there
    // is no DCP producer for vbid
    EXPECT_EQ(ckptMgr->getNumOfCursors(), 1);
    // We must have only the checkpoint-open and the vbucket-state
    // meta-items in the open checkpoint
    EXPECT_EQ(ckptMgr->getNumItems(), 2);
    EXPECT_EQ(ckptMgr->getNumOpenChkItems(), 0);

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test-consumer");
    auto passiveStream = std::static_pointer_cast<MockPassiveStream>(
            consumer->makePassiveStream(
                    *engine,
                    consumer,
                    "test-passive-stream",
                    0 /* flags */,
                    0 /* opaque */,
                    vbid,
                    0 /* startSeqno */,
                    std::numeric_limits<uint64_t>::max() /* endSeqno */,
                    0 /* vbUuid */,
                    0 /* snapStartSeqno */,
                    0 /* snapEndSeqno */,
                    0 /* vb_high_seqno */,
                    {} /* vb_manifest_uid */));

    uint64_t snapshotStart = 1;
    const uint64_t snapshotEnd = 10;

    // Note: for a DcpConsumer only the vbHighSeqno=0 disk-snapshot
    //     exists (so it is the only disk-snapshot for which the
    //     consumer enqueues incoming mutation to the backfill-queue).
    //     All the subsequent disk-snapshots (vbHighSeqno>0) are
    //     actually processed as memory-snapshot, so the incoming
    //     mutations are queued to the mutable checkpoint. Here we are
    //     testing checkpoints, that is why for the disk-snapshot case:
    //     1) we process a first disk-snapshot; this sets the
    //     vbHighSeqno
    //         to something > 0; we don't care about the status of
    //         checkpoints here
    //     2) we carry on with processing a second disk-snapshot, which
    //         involves checkpoints
    int firstCheckpointSize = snapshotEnd - snapshotStart + 1;
    int openCheckpointSize = snapshotEnd - snapshotStart;
    if (flags & dcp_marker_flag_t::MARKER_FLAG_DISK) {
        // Just process the first half of mutations as vbSeqno-0
        // disk-snapshot
        const uint64_t diskSnapshotEnd = (snapshotEnd - snapshotStart) / 2;
        SnapshotMarker snapshotMarker(0 /* opaque */,
                                      vbid,
                                      snapshotStart,
                                      diskSnapshotEnd,
                                      flags,
                                      {} /*HCS*/,
                                      {} /*SID*/);
        passiveStream->processMarker(&snapshotMarker);
        processMutations(*passiveStream, snapshotStart, diskSnapshotEnd);
        snapshotStart = diskSnapshotEnd + 1;

        // snapshot end was hit, expect a new CP if high_mem
        if (highMemUsed) {
            openedCheckPoints++;
            firstCheckpointSize = diskSnapshotEnd;
            openCheckpointSize = 0;
        } else {
            // checkpoint extended
            openCheckpointSize = diskSnapshotEnd;
        }
        EXPECT_EQ(openCheckpointSize, ckptMgr->getNumOpenChkItems());
    }

    // adjust expected size because disk case has adjusted the next snapshot
    // if (!highMemUsed) {}
    // openCheckpointSize = snapshotEnd - snapshotStart;

    // 1) the consumer receives the snapshot-marker
    SnapshotMarker snapshotMarker(
            0 /* opaque */, vbid, snapshotStart, snapshotEnd, flags, {} /*HCS*/, {} /*SID*/);
    passiveStream->processMarker(&snapshotMarker);

    // 2) the consumer receives the mutations until (snapshotEnd -1)
    processMutations(*passiveStream, snapshotStart, snapshotEnd - 1);

    if (flags & dcp_marker_flag_t::MARKER_FLAG_DISK) {
        if (highMemUsed) {
            openCheckpointSize = snapshotEnd - snapshotStart;
        } else {
            // checkpoint contains intial backfill and second snapshot
            openCheckpointSize = snapshotEnd - 1;
        }
    }

    // We must have exactly (snapshotEnd - snapshotStart) items in the
    // checkpoint
    EXPECT_EQ(openCheckpointSize, ckptMgr->getNumOpenChkItems());

    EXPECT_EQ(openedCheckPoints, ckptMgr->getNumCheckpoints());

    // 3) the consumer receives the snapshotEnd mutation
    processMutations(*passiveStream, snapshotEnd, snapshotEnd);

    // If high-mem, the snapshot end opens a CP
    if (highMemUsed) {
        openedCheckPoints++;
    }

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    if (highMemUsed) {
        // Check that (mem_used > high_wat) when we processed the
        // snapshotEnd mutation
        ASSERT_GT(stats.getEstimatedTotalMemoryUsed(),
                  stats.mem_high_wat.load());

        // The consumer has received the snapshotEnd mutation, now we
        // expect that a new (empty) open checkpoint has been created.
        // So we must have 3 checkpoints in total (the closed and the
        // new open one).
        EXPECT_EQ(openedCheckPoints, ckptMgr->getNumCheckpoints());
        // Also, the new open checkpoint must be empty (all mutations
        // are in the closed one)
        EXPECT_EQ(openedCheckPoints, ckptList.back()->getId());
        EXPECT_EQ(checkpoint_state::CHECKPOINT_CLOSED,
                  ckptList.front()->getState());
        EXPECT_EQ(firstCheckpointSize, ckptList.front()->getNumItems());
        EXPECT_EQ(checkpoint_state::CHECKPOINT_OPEN,
                  ckptList.back()->getState());
        EXPECT_EQ(0, ckptList.back()->getNumItems());
    } else {
        // Check that (mem_used < high_wat) when we processed the
        // snapshotEnd mutation
        ASSERT_LT(stats.getEstimatedTotalMemoryUsed(),
                  stats.mem_high_wat.load());

        // The consumer has received the snapshotEnd mutation, but
        // mem_used<high_wat, so we must still have 1 open checkpoint
        // that store all mutations
        EXPECT_EQ(openedCheckPoints, ckptMgr->getNumCheckpoints());
        EXPECT_EQ(checkpoint_state::CHECKPOINT_OPEN,
                  ckptList.back()->getState());
        EXPECT_EQ(ckptList.back()->getNumItems(), snapshotEnd);
    }

    store->deleteVBucket(vb->getId(), cookie);
}

TEST_F(SingleThreadedCheckpointTest,
       CloseReplicaCheckpointOnMemorySnapshotEnd_HighMemDisk) {
    closeReplicaCheckpointOnMemorySnapshotEnd(
            true, dcp_marker_flag_t::MARKER_FLAG_DISK);
}

TEST_F(SingleThreadedCheckpointTest,
       CloseReplicaCheckpointOnMemorySnapshotEnd_Disk) {
    closeReplicaCheckpointOnMemorySnapshotEnd(
            false, dcp_marker_flag_t::MARKER_FLAG_DISK);
}

TEST_F(SingleThreadedCheckpointTest,
       CloseReplicaCheckpointOnMemorySnapshotEnd_HighMem) {
    closeReplicaCheckpointOnMemorySnapshotEnd(
            true, dcp_marker_flag_t::MARKER_FLAG_MEMORY);
}

TEST_F(SingleThreadedCheckpointTest,
       CloseReplicaCheckpointOnMemorySnapshotEnd) {
    closeReplicaCheckpointOnMemorySnapshotEnd(
            false, dcp_marker_flag_t::MARKER_FLAG_MEMORY);
}

// Test that when the same client registers twice, the first cursor 'dies'
TYPED_TEST(CheckpointTest, reRegister) {
    auto dcpCursor1 = this->manager->registerCursorBySeqno("name", 0);
    EXPECT_NE(nullptr, dcpCursor1.cursor.lock().get());
    auto dcpCursor2 = this->manager->registerCursorBySeqno("name", 0);
    EXPECT_EQ(nullptr, dcpCursor1.cursor.lock().get());
    EXPECT_NE(nullptr, dcpCursor2.cursor.lock().get());
    EXPECT_EQ(2, this->manager->getNumOfCursors());
}

TYPED_TEST(CheckpointTest, takeAndResetCursors) {
    auto dcpCursor1 = this->manager->registerCursorBySeqno("name1", 0);
    auto dcpCursor2 = this->manager->registerCursorBySeqno("name2", 0);
    auto dcpCursor3 = this->manager->registerCursorBySeqno("name3", 0);

    EXPECT_EQ(0, this->manager->getNumItemsForPersistence());
    this->queueNewItem("key");

    const auto* c1 = dcpCursor1.cursor.lock().get();
    const auto* c2 = dcpCursor2.cursor.lock().get();
    const auto* c3 = dcpCursor3.cursor.lock().get();
    EXPECT_NE(nullptr, c1);
    EXPECT_NE(nullptr, c2);
    EXPECT_NE(nullptr, c3);
    EXPECT_EQ(4, this->manager->getNumOfCursors());
    EXPECT_EQ(1, this->manager->getNumItemsForPersistence()); // +key
    EXPECT_EQ(1,
              this->manager->getNumItemsForCursor(
                      dcpCursor2.cursor.lock().get()));

    // Second manager
    auto manager2 = std::make_unique<MockCheckpointManager>(this->global_stats,
                                                        this->vbucket->getId(),
                                                        this->checkpoint_config,
                                                        0,
                                                        /*lastSnapStart*/ 0,
                                                        /*lastSnapEnd*/ 0,
                                                        this->callback);

    manager2->takeAndResetCursors(*this->manager);

    EXPECT_EQ(c1, dcpCursor1.cursor.lock().get());
    EXPECT_EQ(c2, dcpCursor2.cursor.lock().get());
    EXPECT_EQ(c3, dcpCursor3.cursor.lock().get());

    EXPECT_EQ(4, manager2->getNumOfCursors());
    EXPECT_EQ(0, this->manager->getNumOfCursors());

    // Destroy first checkpoint manager
    this->createManager(0);

    EXPECT_EQ(c1, dcpCursor1.cursor.lock().get());
    EXPECT_EQ(c2, dcpCursor2.cursor.lock().get());
    EXPECT_EQ(c3, dcpCursor3.cursor.lock().get());

    EXPECT_EQ(4, manager2->getNumOfCursors());
    // Cursors move, but checkpoints don't
    EXPECT_EQ(0, manager2->getNumItemsForPersistence());
    EXPECT_EQ(0,
              manager2->getNumItemsForCursor(dcpCursor2.cursor.lock().get()));
}

// Test that if we add 2 cursors with the same name the first one is removed.
TYPED_TEST(CheckpointTest, DuplicateCheckpointCursor) {
    auto* ckptMgr = this->manager.get();
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);
    // The persistent cursor means we have one cursor in the checkpoint
    ASSERT_EQ(1, ckptList.back()->getNumCursorsInCheckpoint());

    // Register a DCP cursor.
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor =
            this->manager->registerCursorBySeqno(dcp_cursor.c_str(), 0);

    EXPECT_EQ(2, ckptList.back()->getNumCursorsInCheckpoint());

    // Register a 2nd DCP cursor with the same name.
    auto dcpCursor2 =
            this->manager->registerCursorBySeqno(dcp_cursor.c_str(), 0);

    // Adding the 2nd DCP cursor should not have increased the number of
    // cursors in the checkpoint, as the previous one will have been removed
    // when the new one was added.
    EXPECT_EQ(2,ckptList.back()->getNumCursorsInCheckpoint());
}

// Test that if we add 2 cursors with the same name the first one is removed.
// even if the 2 cursors are in different checkpoints.
TYPED_TEST(CheckpointTest, DuplicateCheckpointCursorDifferentCheckpoints) {
    // Size down the default number of items to create a new checkpoint and
    // recreate the manager
    this->checkpoint_config = CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                                               MIN_CHECKPOINT_ITEMS,
                                               /*numCheckpoints*/ 2,
                                               /*itemBased*/ true,
                                               /*keepClosed*/ false,
                                               /*persistenceEnabled*/ true);
    this->createManager();
    auto* ckptMgr = this->manager.get();

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);
    // The persistent cursor means we have one cursor in the checkpoint
    ASSERT_EQ(1, ckptList.back()->getNumCursorsInCheckpoint());

    // Register a DCP cursor.
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor =
            this->manager->registerCursorBySeqno(dcp_cursor.c_str(), 0);

    // Adding the following items will result in 2 checkpoints, with
    // both cursors in the first checkpoint.
    for (int ii = 0; ii < 2 * MIN_CHECKPOINT_ITEMS; ++ii) {
        this->queueNewItem("key" + std::to_string(ii));
    }
    EXPECT_EQ(2, ckptList.size());
    EXPECT_EQ(2, ckptList.front()->getNumCursorsInCheckpoint());

    // Register a 2nd DCP cursor with the same name but this time into the
    // 2nd checkpoint
    auto dcpCursor2 = this->manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 1000 + MIN_CHECKPOINT_ITEMS + 2);

    // Adding the 2nd DCP cursor should not have increased the number of
    // cursors as the previous cursor will have been removed when the new one
    // was added.  The persistence cursor will still be in the first
    // checkpoint however the dcpCursor will have been deleted from the first
    // checkpoint and adding to the 2nd checkpoint.
    EXPECT_EQ(1, ckptList.front()->getNumCursorsInCheckpoint());
    EXPECT_EQ(1, ckptList.back()->getNumCursorsInCheckpoint());
}

// Test that when adding duplicate queued_items (of the same size) it
// does not increase the size of the checkpoint.
TYPED_TEST(CheckpointTest, dedupeMemoryTest) {
    // Get the intial size of the checkpoint.
    auto memoryUsage1 = this->manager->getMemoryUsage();

    ASSERT_TRUE(this->queueNewItem("key0"));

    // Get checkpoint size again after adding a queued_item.
    auto memoryUsage2 = this->manager->getMemoryUsage();
    EXPECT_LT(memoryUsage1, memoryUsage2);

    // Add duplicate items, which should cause de-duplication to occur
    // and so the checkpoint should not increase in size
    for (auto ii = 0; ii < MIN_CHECKPOINT_ITEMS; ++ii) {
        EXPECT_FALSE(this->queueNewItem("key0"));
    }

    // Get checkpoint size again after adding duplicate items.
    auto memoryUsage3 = this->manager->getMemoryUsage();
    EXPECT_EQ(memoryUsage2, memoryUsage3);

    // Add a number of non duplicate items to the same checkpoint so the
    // checkpoint should increase in size.
    for (auto ii = 1; ii < MIN_CHECKPOINT_ITEMS; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    // Get checkpoint size again after adding non-duplicate items.
    auto memoryUsage4 = this->manager->getMemoryUsage();
    EXPECT_LT(memoryUsage3, memoryUsage4);
}

// Test that the checkpoint memory stat is correctly maintained when
// de-duplication occurs and also when the checkpoint containing the
// mutation is removed.
TYPED_TEST(CheckpointTest, checkpointMemoryTest) {
    // Get the intial size of the checkpoint.
    auto initialSize = this->manager->getMemoryUsage();

    // Allocator used for tracking memory used by the CheckpointQueue
    checkpoint_index::allocator_type memoryTrackingAllocator;
    // Emulate the Checkpoint keyIndex so we can determine the number
    // of bytes that should be allocated during its use.
    checkpoint_index keyIndex(memoryTrackingAllocator);
    // Grab the initial size of the keyIndex because on Windows an empty
    // std::unordered_map allocated 200 bytes.
    const auto initialKeyIndexSize =
            *(keyIndex.get_allocator().getBytesAllocated());
    ChkptQueueIterator iterator =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *(this->manager))
                    .front()
                    ->begin();
    index_entry entry{iterator, 0};

    // Create a queued_item with a 'small' value
    std::string value("value");
    queued_item qiSmall(new Item(makeStoredDocKey("key"),
                                 0,
                                 0,
                                 value.c_str(),
                                 value.size(),
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0,
                                 -1,
                                 Vbid(0)));

    // Add the queued_item to the checkpoint
    this->manager->queueDirty(*this->vbucket,
                              qiSmall,
                              GenerateBySeqno::Yes,
                              GenerateCas::Yes,
                              /*preLinkDocCtx*/ nullptr);

    // The queue (toWrite) is implemented as std:list, therefore
    // when we add an item it results in the creation of 3 pointers -
    // forward ptr, backward ptr and ptr to object.
    const size_t perElementOverhead = 3 * sizeof(uintptr_t);

    // Check that checkpoint size is the initial size plus the addition of
    // qiSmall.
    auto expectedSize = initialSize;
    // Add the size of the item
    expectedSize += qiSmall->size();
    // Add the size of adding to the queue
    expectedSize += perElementOverhead;
    // Add to the emulated keyIndex
    keyIndex.emplace(
            CheckpointIndexKey(qiSmall->getKey(),
                               qiSmall->isCommitted()
                                       ? CheckpointIndexKeyNamespace::Committed
                                       : CheckpointIndexKeyNamespace::Prepared),
            entry);

    auto keyIndexSize = *(keyIndex.get_allocator().getBytesAllocated());
    expectedSize += (keyIndexSize - initialKeyIndexSize);

    EXPECT_EQ(expectedSize, this->manager->getMemoryUsage());

    // Create a queued_item with a 'big' value
    std::string bigValue(1024, 'a');
    queued_item qiBig(new Item(makeStoredDocKey("key"),
                               0,
                               0,
                               bigValue.c_str(),
                               bigValue.size(),
                               PROTOCOL_BINARY_RAW_BYTES,
                               0,
                               -1,
                               Vbid(0)));

    // Add the queued_item to the checkpoint
    this->manager->queueDirty(*this->vbucket,
                              qiBig,
                              GenerateBySeqno::Yes,
                              GenerateCas::Yes,
                              /*preLinkDocCtx*/ nullptr);

    // Check that checkpoint size is the initial size plus the addition of
    // qiBig.
    expectedSize = initialSize;
    // Add the size of the item
    expectedSize += qiBig->size();
    // Add the size of adding to the queue
    expectedSize += perElementOverhead;
    // Add to the keyIndex
    keyIndex.emplace(
            CheckpointIndexKey(qiBig->getKey(),
                               qiBig->isCommitted()
                                       ? CheckpointIndexKeyNamespace::Committed
                                       : CheckpointIndexKeyNamespace::Prepared),
            entry);

    keyIndexSize = *(keyIndex.get_allocator().getBytesAllocated());
    expectedSize += (keyIndexSize - initialKeyIndexSize);

    EXPECT_EQ(expectedSize, this->manager->getMemoryUsage());

    bool isLastMutationItem;
    // Move cursor to checkpoint start
    auto item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                        isLastMutationItem);
    EXPECT_FALSE(isLastMutationItem);
    // Move cursor to the mutation
    item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                   isLastMutationItem);
    EXPECT_TRUE(isLastMutationItem);

    // Create a new checkpoint, which will close the old checkpoint
    // and move the persistence cursor to the new checkpoint.
    this->manager->createNewCheckpoint();

    // Tell Checkpoint manager the items have been persisted, so it
    // advances pCursorPreCheckpointId, which will allow us to remove
    // the closed unreferenced checkpoints.
    this->manager->itemsPersisted();

    // We are now in a position to remove the checkpoint that had the
    // mutation in it.
    bool new_open_ckpt_created;
    EXPECT_EQ(1,
              this->manager->removeClosedUnrefCheckpoints(
                      *this->vbucket, new_open_ckpt_created));

    // Should be back to the initialSize
    EXPECT_EQ(initialSize, this->manager->getMemoryUsage());
}

// Test the tracking of memory overhead by adding a single element to the
// CheckpointQueue.
TYPED_TEST(CheckpointTest, checkpointTrackingMemoryOverheadTest) {
    // Get the intial size of the checkpoint overhead.
    const auto initialOverhead = this->manager->getMemoryOverhead();

    // Allocator used for tracking memory used by the CheckpointQueue
    checkpoint_index::allocator_type memoryTrackingAllocator;
    // Emulate the Checkpoint keyIndex so we can determine the number
    // of bytes that should be allocated during its use.
    checkpoint_index keyIndex(memoryTrackingAllocator);
    // Grab the initial size of the keyIndex because on Windows an empty
    // std::unordered_map allocated 200 bytes.
    const auto initialKeyIndexSize =
            *(keyIndex.get_allocator().getBytesAllocated());

    ChkptQueueIterator iterator =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *(this->manager))
                    .front()
                    ->begin();
    index_entry entry{iterator, 0};

    // Create a queued_item
    std::string value("value");
    queued_item qiSmall(new Item(makeStoredDocKey("key"),
                                 0,
                                 0,
                                 value.c_str(),
                                 value.size(),
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0,
                                 -1,
                                 Vbid(0)));

    // Add the queued_item to the checkpoint
    this->manager->queueDirty(*this->vbucket,
                              qiSmall,
                              GenerateBySeqno::Yes,
                              GenerateCas::Yes,
                              /*preLinkDocCtx*/ nullptr);

    // Re-measure the checkpoint overhead
    const auto updatedOverhead = this->manager->getMemoryOverhead();
    // Three pointers - forward, backward and pointer to item
    const auto perElementListOverhead = sizeof(uintptr_t) * 3;
    // Add entry into keyIndex
    keyIndex.emplace(
            CheckpointIndexKey(qiSmall->getKey(),
                               qiSmall->isCommitted()
                                       ? CheckpointIndexKeyNamespace::Committed
                                       : CheckpointIndexKeyNamespace::Prepared),
            entry);

    const auto keyIndexSize = *(keyIndex.get_allocator().getBytesAllocated());
    EXPECT_EQ(perElementListOverhead + (keyIndexSize - initialKeyIndexSize),
              updatedOverhead - initialOverhead);

    bool isLastMutationItem;
    // Move cursor to checkpoint start
    auto item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                        isLastMutationItem);
    EXPECT_FALSE(isLastMutationItem);
    // Move cursor to the mutation
    item = this->manager->nextItem(this->manager->getPersistenceCursor(),
                                   isLastMutationItem);
    EXPECT_TRUE(isLastMutationItem);

    // Create a new checkpoint, which will close the old checkpoint
    // and move the persistence cursor to the new checkpoint.
    this->manager->createNewCheckpoint();

    // Tell Checkpoint manager the items have been persisted, so it
    // advances pCursorPreCheckpointId, which will allow us to remove
    // the closed unreferenced checkpoints.
    this->manager->itemsPersisted();

    // We are now in a position to remove the checkpoint that had the
    // mutation in it.
    bool new_open_ckpt_created;
    EXPECT_EQ(1,
              this->manager->removeClosedUnrefCheckpoints(
                      *this->vbucket, new_open_ckpt_created));

    // Should be back to the initialOverhead
    EXPECT_EQ(initialOverhead, this->manager->getMemoryOverhead());
}

// Test that can expel items and that we have the correct behaviour when we
// register cursors for items that have been expelled.
TYPED_TEST(CheckpointTest, expelCheckpointItemsTest) {
    const int itemCount{3};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    ASSERT_EQ(itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount, this->manager->getNumItemsForPersistence());
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start
     * 1001 - 1st item (key0)
     * 1002 - 2nd item (key1) <<<<<<< persistenceCursor
     * 1003 - 3rd item (key2)
     */

    CheckpointManager::ExpelResult expelResult =
            this->manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(itemCount, expelResult.expelCount);
    EXPECT_LT(0, expelResult.estimateOfFreeMemory);
    EXPECT_EQ(itemCount, this->global_stats.itemsExpelledFromCheckpoints);

    /*
     * After expelling checkpoint now looks as follows:
     * 1000 - dummy Item <<<<<<< persistenceCursor
     * 1003 - 3rd item (key 2)
     */

    /*
     * We have expelled:
     * 1001 - checkpoint start
     * 1001 - 1st item (key 0)
     * 1002 - 2nd item (key 1)
     */

    // The full checkpoint still contains the 3 items added.
    EXPECT_EQ(itemCount, this->manager->getNumOpenChkItems());

    // Try to register a DCP replication cursor from 1001 - an expelled item.
    std::string dcp_cursor1(DCP_CURSOR_PREFIX + std::to_string(1));
    CursorRegResult regResult =
            this->manager->registerCursorBySeqno(dcp_cursor1.c_str(), 1001);
    EXPECT_EQ(1003, regResult.seqno);
    EXPECT_TRUE(regResult.tryBackfill);

    // Try to register a DCP replication cursor from 1002 - the dummy item.
    std::string dcp_cursor2(DCP_CURSOR_PREFIX + std::to_string(2));
    regResult = this->manager->registerCursorBySeqno(dcp_cursor2.c_str(), 1002);
    EXPECT_EQ(1003, regResult.seqno);
    EXPECT_TRUE(regResult.tryBackfill);

    // Try to register a DCP replication cursor from 1003 - the first
    // valid in-checkpoint item.
    std::string dcp_cursor3(DCP_CURSOR_PREFIX + std::to_string(3));
    regResult = this->manager->registerCursorBySeqno(dcp_cursor3.c_str(), 1003);
    EXPECT_EQ(1004, regResult.seqno);
    EXPECT_FALSE(regResult.tryBackfill);
}

// Test that we correctly handle duplicates, where the initial version of the
// document has been expelled.
TYPED_TEST(CheckpointTest, expelCheckpointItemsWithDuplicateTest) {
    const int itemCount{3};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    ASSERT_EQ(itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount, this->manager->getNumItemsForPersistence());
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    CheckpointManager::ExpelResult expelResult =
            this->manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(itemCount, expelResult.expelCount);
    EXPECT_LT(0, expelResult.estimateOfFreeMemory);
    EXPECT_EQ(itemCount, this->global_stats.itemsExpelledFromCheckpoints);

    /*
     * After expelling checkpoint now looks as follows:
     * 1000 - dummy Item <<<<<<< persistenceCursor
     * 1003 - 3rd item (key2)
     */

    // Add another item which has been expelled.
    // Should not find the duplicate and so will re-add.
    EXPECT_TRUE(this->queueNewItem("key0"));

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy Item <<<<<<< persistenceCursor
     * 1003 - 3rd item (key2)
     * 1004 - 4th item (key0)  << The New item added >>
     */

    // The full checkpoint still contains the 4 items added.
    EXPECT_EQ(itemCount + 1, this->manager->getNumOpenChkItems());
}

// Test that when the first cursor we come across is pointing to the last
// item we do not evict this item.  Instead we walk backwards find the
// first non-meta item and evict from there.
TYPED_TEST(CheckpointTest, expelCursorPointingToLastItem) {
    const int itemCount{2};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    ASSERT_EQ(itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount, this->manager->getNumItemsForPersistence());
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount + 1; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
    }

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start
     * 1001 - 1st item
     * 1002 - 2nd item  <<<<<<< persistenceCursor
     */

    // Don't expel anything because the cursor points to item that has the
    // highest seqno for the checkpoint so we move the expel point back
    // one, but now it has a previous entry with the same seqno so again
    // move back one.  The expel point now points to a metadata item so
    // move back again.  We have now reached the dummy item and so we
    // don't expel anything.
    CheckpointManager::ExpelResult expelResult =
            this->manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(0, expelResult.expelCount);
    EXPECT_EQ(0, expelResult.estimateOfFreeMemory);
    EXPECT_EQ(0, this->global_stats.itemsExpelledFromCheckpoints);
}


// Test that when the first cursor we come across is pointing to the checkpoint
// start we do not evict this item.  Instead we walk backwards and find the
// the dummy item, so do not expel any items.
TYPED_TEST(CheckpointTest, expelCursorPointingToChkptStart) {
    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.

    bool isLastMutationItem{true};
    auto item = this->manager->nextItem(
            this->manager->getPersistenceCursor(), isLastMutationItem);

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start  <<<<<<< persistenceCursor
     */

    CheckpointManager::ExpelResult expelResult =
            this->manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(0, expelResult.expelCount);
    EXPECT_EQ(0, expelResult.estimateOfFreeMemory);
    EXPECT_EQ(0, this->global_stats.itemsExpelledFromCheckpoints);
}

// Test that if we want to evict items from seqno X, but have a meta-data item
// also with seqno X, and a cursor is pointing to this meta data item, we do not
// evict.
TYPED_TEST(CheckpointTest, dontExpelIfCursorAtMetadataItemWithSameSeqno) {
    const int itemCount{2};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    // Move the persistence cursor to the end to get it of the way.
    bool isLastMutationItem{true};
    for (auto ii = 0; ii < 3; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
    }

    // Add a cursor pointing to the dummy
    std::string dcpCursor1(DCP_CURSOR_PREFIX + std::to_string(1));
    CursorRegResult regResult =
            this->manager->registerCursorBySeqno(dcpCursor1.c_str(), 1000);

    // Move the cursor forward one step so that it now points to the checkpoint
    // start.
    auto item = this->manager->nextItem(regResult.cursor.lock().get(),
                                        isLastMutationItem);

    // Add a cursor to point to the 1st mutation we added.  Note that when
    // registering the cursor we walk backwards from the checkpoint end until we
    // reach the item with the seqno we are requesting.  Hence we register the
    // cursor at the mutation and not the metadata item (checkpoint start) which
    // has the same seqno.
    std::string dcpCursor2(DCP_CURSOR_PREFIX + std::to_string(2));
    CursorRegResult regResult2 =
            this->manager->registerCursorBySeqno(dcpCursor2.c_str(), 1001);

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start  <<<<<<< dcpCursor1
     * 1001 - 1st item  <<<<<<< dcpCursor2
     * 1002 - 2nd item  <<<<<<< persistenceCursor
     */

    // We should not expel any items due to dcpCursor1
    CheckpointManager::ExpelResult expelResult =
            this->manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(0, expelResult.expelCount);
    EXPECT_EQ(0, expelResult.estimateOfFreeMemory);
    EXPECT_EQ(0, this->global_stats.itemsExpelledFromCheckpoints);
}

// Test that if we have a item after a mutation with the same seqno
// then we will move the expel point backwards to the mutation
// (and possibly further).
TYPED_TEST(CheckpointTest, doNotExpelIfHaveSameSeqnoAfterMutation) {
    this->checkpoint_config = CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                                               /*maxItemsInCheckpoint*/ 1,
                                               /*maxCheckpoints*/ 2,
                                               /*itemBased*/ true,
                                               /*keepClosed*/ false,
                                               /*persistenceEnabled*/ true);
    this->createManager();

    // Add a meta data operation
    this->manager->queueSetVBState(*this->vbucket);

    const int itemCount{2};
    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /*
     * First checkpoint (closed) is as follows:
     * 1000 - dummy item   <<<<<<< persistenceCursor
     * 1001 - checkpoint start
     * 1001 - set VB state
     * 1001 - mutation
     * 1001 - checkpoint end
     *
     * Second checkpoint (open) is as follows:
     * 1001 - dummy item
     * 1002 - checkpoint start
     * 1002 - mutation
     */

    // Move the persistence cursor to the second mutation.
    bool isLastMutationItem{false};
    for (auto ii = 0; ii < 6; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
    }

    std::string dcpCursor1(DCP_CURSOR_PREFIX + std::to_string(1));
    CursorRegResult regResult =
            this->manager->registerCursorBySeqno(dcpCursor1.c_str(), 1000);

    // Move the dcp cursor to the checkpoint end.
    for (auto ii = 0; ii < 4; ++ii) {
        auto item = this->manager->nextItem(regResult.cursor.lock().get(),
                                            isLastMutationItem);
    }

    /*
     * First checkpoint (closed) is as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start
     * 1001 - set VB state
     * 1001 - mutation
     * 1001 - checkpoint end  <<<<<<< dcpCursor1
     *
     * Second checkpoint (open) is as follows:
     * 1001 - dummy item
     * 1002 - checkpoint start
     * 1002 - mutation   <<<<<<< persistenceCursor
     */

    // We should not expel any items due to dcpCursor1 as we end up
    // moving the expel point back to the dummy item.
    CheckpointManager::ExpelResult expelResult =
            this->manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(0, expelResult.expelCount);
    EXPECT_EQ(0, expelResult.estimateOfFreeMemory);
    EXPECT_EQ(0, this->global_stats.itemsExpelledFromCheckpoints);
}

// Test estimate for the amount of memory recovered by expelling is correct.
TYPED_TEST(CheckpointTest, expelCheckpointItemsMemoryRecoveredTest) {
    const int itemCount{3};
    size_t sizeOfItem{0};

    for (auto ii = 0; ii < itemCount; ++ii) {
        std::string value("value");
        queued_item item(new Item(makeStoredDocKey("key" + std::to_string(ii)),
                                  0,
                                  0,
                                  value.c_str(),
                                  value.size(),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  0,
                                  -1,
                                  Vbid(0)));

        sizeOfItem = item->size();

        // Add the queued_item to the checkpoint
        this->manager->queueDirty(*this->vbucket,
                                  item,
                                  GenerateBySeqno::Yes,
                                  GenerateCas::Yes,
                                  /*preLinkDocCtx*/ nullptr);
    }

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    ASSERT_EQ(itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount, this->manager->getNumItemsForPersistence());
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < 3; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start
     * 1001 - 1st item (key0)
     * 1002 - 2nd item (key1) <<<<<<< persistenceCursor
     * 1003 - 3rd item (key2)
     */

    // Get the memory usage before expelling
    const auto checkpointMemoryUsageBeforeExpel =
            this->manager->getMemoryUsage();

    CheckpointManager::ExpelResult expelResult =
            this->manager->expelUnreferencedCheckpointItems();

    /*
     * After expelling checkpoint now looks as follows:
     * 1000 - dummy Item <<<<<<< persistenceCursor
     * 1003 - 3rd item (key 2)
     */

    /*
     * We have expelled:
     * 1001 - checkpoint start
     * 1001 - 1st item (key 0)
     * 1002 - 2nd item (key 1)
     */

    // Get the memory usage after expelling
    auto checkpointMemoryUsageAfterExpel = this->manager->getMemoryUsage();

    size_t extra = 0;
    // A list is comprised of 3 pointers (forward, backwards and
    // pointer to the element).
    const size_t perElementOverhead = extra + (3 * sizeof(uintptr_t));
#if WIN32
    // On windows for an empty list we still allocate space for
    // containing one element.
    extra = perElementOverhead;
#endif

    const size_t reductionInCheckpointMemoryUsage =
            checkpointMemoryUsageBeforeExpel - checkpointMemoryUsageAfterExpel;
    const size_t checkpointListSaving =
            (perElementOverhead * expelResult.expelCount);
    const auto& checkpointStartItem =
            this->manager->public_createCheckpointItem(
                    0, Vbid(0), queue_op::checkpoint_start);
    const size_t queuedItemSaving =
            checkpointStartItem->size() + (sizeOfItem * (itemCount - 1));
    const size_t expectedMemoryRecovered =
            checkpointListSaving + queuedItemSaving;

    EXPECT_EQ(3, expelResult.expelCount);
    EXPECT_EQ(expectedMemoryRecovered,
              expelResult.estimateOfFreeMemory - extra);
    EXPECT_EQ(expectedMemoryRecovered, reductionInCheckpointMemoryUsage);
    EXPECT_EQ(3, this->global_stats.itemsExpelledFromCheckpoints);
}
