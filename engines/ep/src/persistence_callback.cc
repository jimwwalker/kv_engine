/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "persistence_callback.h"

#include "item.h"
#include "stats.h"

PersistenceCallback::PersistenceCallback(const queued_item& qi, uint64_t c)
    : queuedItem(qi), cas(c) {
}

PersistenceCallback::~PersistenceCallback() = default;

void PersistenceCallback::updateStats(VBucket& vbucket,
                                      const DocKey& key,
                                      uint64_t cas,
                                      uint64_t revSeqno,
                                      InsertUpdateDelete how) {
    auto hbl = vbucket.ht.getLockedBucket(key);
    StoredValue* v = vbucket.fetchValidValue(
            hbl, key, WantsDeleted::Yes, TrackReference::No, QueueExpired::Yes);
    if (v) {
        if (v->getCas() == cas) {
            // mark this item clean only if current and stored cas
            // value match
            v->markClean();
        }
        if (v->isNewCacheItem()) {
            switch (how) {
            case InsertUpdateDelete::Insert: {
                // Insert in value-only or full eviction mode.
                ++vbucket.opsCreate;
                vbucket.incrNumTotalItems();
                vbucket.incrMetaDataDisk(key);
                break;
            }
            case InsertUpdateDelete::Update: {
                // Update for non-resident item in full eviction mode.
                ++vbucket.opsUpdate;
                break;
            case InsertUpdateDelete::Delete: {
                vbucket.deletedOnDiskCbk(key, revSeqno);
                break;
            }
            }
            }

            v->setNewCacheItem(false);
        } else { // Update in value-only or full eviction mode.
            ++vbucket.opsUpdate;
        }
    }
}

// This callback is invoked for set only.
void PersistenceCallback::callback(TransactionContext& txCtx) {
    auto& epCtx = dynamic_cast<EPTransactionContext&>(txCtx);
    auto& vbucket = epCtx.vbucket;
    vbucket.doStatsForFlushing(*queuedItem, queuedItem->size());
    --epCtx.stats.diskQueueSize;
    epCtx.stats.totalPersisted++;
}

// This callback is invoked for deletions only.
//
// The boolean indicates whether the underlying storage
// successfully deleted the item.
void PersistenceCallback::callback(TransactionContext& txCtx, int& value) {
    auto& epCtx = dynamic_cast<EPTransactionContext&>(txCtx);
    auto& vbucket = epCtx.vbucket;

    // > 1 would be bad.  We were only trying to delete one row.
    if (value > 1) {
        throw std::logic_error(
                "PersistenceCallback::callback: value "
                "(which is " +
                std::to_string(value) + ") should be <= 1 for deletions");
    }
    // -1 means fail
    // 1 means we deleted one row
    // 0 means we did not delete a row, but did not fail (did not exist)
    if (value >= 0) {
        // We have successfully removed an item from the disk, we
        // may now remove it from the hash table.
        // vbucket.deletedOnDiskCbk(*queuedItem, (value > 0));
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "PersistenceCallback::callback: Fatal error in persisting "
            "DELETE on vb:%" PRIu16,
            queuedItem->getVBucketId());
        redirty(epCtx.stats, vbucket);
    }
}

void PersistenceCallback::redirty(EPStats& stats, VBucket& vbucket) {
    if (vbucket.isDeletionDeferred()) {
        // updating the member stats for the vbucket is not really necessary
        // as the vbucket is about to be deleted
        vbucket.doStatsForFlushing(*queuedItem, queuedItem->size());
        // the following is a global stat and so is worth updating
        --stats.diskQueueSize;
        return;
    }
    ++stats.flushFailed;
    vbucket.markDirty(queuedItem->getKey());
    vbucket.rejectQueue.push(queuedItem);
    ++vbucket.opsReject;
}
