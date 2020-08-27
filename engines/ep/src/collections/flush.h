/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#pragma once

#include "collections/collection_persisted_stats.h"
#include "collections/collections_types.h"
#include "collections/vbucket_manifest_handles.h"

#include <unordered_set>

class KVBucket;

namespace Collections {
namespace VB {
class Manifest;

/**
 * The Collections::VB::Flush object maintains data used in a single run of the
 * disk flusher for 1) Collection item counting and 2) persisted metadata
 * updates (when the flusher is flushing collection config changes).
 */
class Flush {
public:
    explicit Flush(Manifest& manifest) : manifest(manifest) {
    }

    Manifest& getManifest() const {
        return manifest;
    }

    /**
     * Run the specified callback against the set of collections which have
     * changed their item count during the run of the flusher.
     */
    void saveCollectionStats(
            std::function<void(CollectionID, PersistedStats)> cb) const;

    /**
     * Update collection stats from the flusher for a insert only operation.
     * We can be inserting a delete or a live document.
     *
     * @param key The key of the item flushed
     * @param seqno The seqno of the item flushed
     * @param isCommitted the prepare/commit state of the item flushed
     * @param isDelete alive/delete state of the item flushed
     * @param size bytes used on disk of the item flushed
     */
    void updateStats(const DocKey& key,
                     uint64_t seqno,
                     bool isCommitted,
                     bool isDelete,
                     size_t size);

    /**
     * Update collection stats from the flusher when an old 'version' of the
     * item already exists. This covers updates or deletes of items
     *
     * @param key The key of the item flushed
     * @param seqno The seqno of the item flushed
     * @param isCommitted the prepare/commit state of the item flushed
     * @param isDelete alive/delete stats of the item flushed
     * @param size bytes used on disk of the item flushed
     * @param oldSeqno The seqno of the old 'version' of the item
     * @param oldIsDelete alive/delete state of the old 'version' of the item
     * @param oldSize bytes used on disk of the old 'version' of the item
     */
    void updateStats(const DocKey& key,
                     uint64_t seqno,
                     bool isCommitted,
                     bool isDelete,
                     size_t size,
                     uint64_t oldSeqno,
                     bool oldIsDelete,
                     size_t oldSize);

    /**
     * Check to see if this flush should trigger a collection purge which if
     * true schedules a task which will iterate the vbucket's documents removing
     * those of any dropped collections. The actual task currently scheduled is
     * compaction.
     */
    void checkAndTriggerPurge(Vbid vbid, KVBucket& bucket) const;

    static void triggerPurge(Vbid vbid, KVBucket& bucket);

    void setNeedsPurge() {
        needsPurge = true;
    }

private:
    // Private helper class for doing collection stat updates
    class StatsUpdate {
    public:
        explicit StatsUpdate(CachingReadHandle&& handle)
            : handle(std::move(handle)) {
        }

        /**
         * an item is being inserted into the collection
         * @param isCommitted the prepare/commit state of the item inserted
         * @param isDelete alive/delete stats of the item inserted
         * @param diskSizeDelta the +/- bytes the insert changes disk-used by
         */
        void insert(bool isCommitted, bool isDelete, ssize_t diskSizeDelta);

        /**
         * an item is being updated in the collection
         * @param isCommitted the prepare/commit state of the item updated
         * @param diskSizeDelta the +/- bytes the update changes disk-used by
         */
        void update(bool isCommitted, ssize_t diskSizeDelta);

        /**
         * an item is being removed (deleted) from the collection
         * @param isCommitted the prepare/commit state of the item removed
         * @param diskSizeDelta the +/- bytes the delete changes disk-used by
         */
        void remove(bool isCommitted, ssize_t diskSizeDelta);

        /**
         * @return true if the seqno represents a logically deleted item for the
         *         locked collection.
         */
        bool isLogicallyDeleted(uint64_t seqno) const;

        /**
         * Increment the 'disk' count for the collection associated with the key
         */
        void incrementDiskCount();

        /**
         * Decrement the 'disk' count for the collection associated with the key
         */
        void decrementDiskCount();

        /**
         * Update the on disk size (bytes) for the collection associated with
         * the key
         */
        void updateDiskSize(ssize_t delta);

    private:
        /// handle on the collection
        CachingReadHandle handle;
    };

    /**
     * For collection events and mutations/deletions lock/obtain a handle on
     * the collection for stat updates. For scope events do nothing.
     * @param key The key of the item being flushed
     * @param seqno Sequence number of the item being flushed
     * @param isCommitted the commit/prepare state of the item being flushed
     * @return StatsUpdate 'proxy' for updating the collection or nothing for
     *         scopes
 `   */
    std::optional<StatsUpdate> tryTolockAndSetPersistedSeqno(const DocKey& key,
                                                             uint64_t seqno,
                                                             bool isCommitted);

    bool needsPurge = false;

    /**
     * Keep track of only the collections that have had a insert/delete in
     * this run of the flusher so we can flush only those collections whose
     * item count may have changed.
     */
    std::unordered_set<CollectionID> mutated;

    /**
     * ref to the 'parent' manifest for this VB::Flusher, this will receive item
     * count updates
     */
    Manifest& manifest;
};

} // end namespace VB
} // end namespace Collections
