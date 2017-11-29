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

PersistenceCallback::PersistenceCallback(const queued_item& qi,
                                         VBucketPtr& vb,
                                         EPStats& s,
                                         uint64_t c)
    : queuedItem(qi), vbucket(vb), stats(s), cas(c) {
    if (!vb) {
        throw std::invalid_argument("PersistenceCallback(): vb is NULL");
    }
}

PersistenceCallback::~PersistenceCallback() = default;

// This callback is invoked for set only.
void PersistenceCallback::callback(mutation_result& value) {
    if (value.first == 1) {
        auto hbl = vbucket->ht.getLockedBucket(queuedItem->getKey());
        StoredValue* v = vbucket->fetchValidValue(hbl,
                                                  queuedItem->getKey(),
                                                  WantsDeleted::Yes,
                                                  TrackReference::No,
                                                  QueueExpired::Yes);
        if (v) {
            if (v->getCas() == cas) {
                // mark this item clean only if current and stored cas
                // value match
                v->markClean();
            }
            if (v->isNewCacheItem()) {
                if (value.second) {
                    // Insert in value-only or full eviction mode.
                    ++vbucket->opsCreate;
                    vbucket->incrNumTotalItems();
                    vbucket->incrMetaDataDisk(*queuedItem);
                    vbucket->incrementCollectionItemCount(queuedItem->getKey());
                } else { // Update in full eviction mode.
                    ++vbucket->opsUpdate;
                }

                v->setNewCacheItem(false);
            } else { // Update in value-only or full eviction mode.
                ++vbucket->opsUpdate;
            }
        }

        vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
        --stats.diskQueueSize;
        stats.totalPersisted++;
    } else {
        // If the return was 0 here, we're in a bad state because
        // we do not know the rowid of this object.
        if (value.first == 0) {
            auto hbl = vbucket->ht.getLockedBucket(queuedItem->getKey());
            StoredValue* v = vbucket->fetchValidValue(hbl,
                                                      queuedItem->getKey(),
                                                      WantsDeleted::Yes,
                                                      TrackReference::No,
                                                      QueueExpired::Yes);
            if (v) {
                LOG(EXTENSION_LOG_WARNING,
                    "PersistenceCallback::callback: Persisting on "
                    "vb:%" PRIu16 ", seqno:%" PRIu64 " returned 0 updates",
                    queuedItem->getVBucketId(),
                    v->getBySeqno());
            } else {
                LOG(EXTENSION_LOG_WARNING,
                    "PersistenceCallback::callback: Error persisting, a key"
                    "is missing from vb:%" PRIu16,
                    queuedItem->getVBucketId());
            }

            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            --stats.diskQueueSize;
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "PersistenceCallback::callback: Fatal error in persisting "
                "SET on vb:%" PRIu16,
                queuedItem->getVBucketId());
            redirty();
        }
    }
}

// This callback is invoked for deletions only.
//
// The boolean indicates whether the underlying storage
// successfully deleted the item.
void PersistenceCallback::callback(int& value) {
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
        vbucket->deletedOnDiskCbk(*queuedItem, (value > 0));
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "PersistenceCallback::callback: Fatal error in persisting "
            "DELETE on vb:%" PRIu16,
            queuedItem->getVBucketId());
        redirty();
    }
}

void PersistenceCallback::redirty() {
    if (vbucket->isDeletionDeferred()) {
        // updating the member stats for the vbucket is not really necessary
        // as the vbucket is about to be deleted
        vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
        // the following is a global stat and so is worth updating
        --stats.diskQueueSize;
        return;
    }
    ++stats.flushFailed;
    vbucket->markDirty(queuedItem->getKey());
    vbucket->rejectQueue.push(queuedItem);
    ++vbucket->opsReject;
}
