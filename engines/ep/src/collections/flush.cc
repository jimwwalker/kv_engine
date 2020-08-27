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

#include "collections/flush.h"
#include "../kvstore.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "item.h"

namespace Collections::VB {

void Flush::saveCollectionStats(
        std::function<void(CollectionID, PersistedStats)> cb) const {
    for (const auto c : mutated) {
        PersistedStats stats;
        {
            auto lock = manifest.lock(c);
            if (!lock.valid()) {
                // Can be flushing for a dropped collection (no longer in the
                // manifest)
                continue;
            }
            stats = lock.getPersistedStats();
        }
        cb(c, stats);
    }
}

bool Flush::StatsUpdate::isLogicallyDeleted(uint64_t seqno) const {
    return handle.isLogicallyDeleted(seqno);
}

void Flush::StatsUpdate::incrementDiskCount() {
    if (!handle.getKey().isInSystemCollection()) {
        handle.incrementDiskCount();
    }
}

void Flush::StatsUpdate::decrementDiskCount() {
    if (!handle.getKey().isInSystemCollection()) {
        handle.decrementDiskCount();
    }
}

void Flush::StatsUpdate::updateDiskSize(ssize_t delta) {
    handle.updateDiskSize(delta);
}

std::optional<Flush::StatsUpdate> Flush::tryTolockAndSetPersistedSeqno(
        const DocKey& key, uint64_t seqno, bool isCommitted) {
    if (key.isInSystemCollection()) {
        // Is it a collection system event?
        auto [event, id] = SystemEventFactory::getTypeAndID(key);
        switch (event) {
        case SystemEvent::Collection: {
            auto handle = manifest.lock(key, Manifest::AllowSystemKeys{});
            if (handle.setPersistedHighSeqno(seqno)) {
                // Update the 'mutated' set, stats are changing
                mutated.insert(CollectionID(id));
            } else {
                // Cannot set the seqno (flushing dropped items) no more updates
                return {};
            }
            return Flush::StatsUpdate{std::move(handle)};
        }
        case SystemEvent::Scope:
            break;
        }
        return {};
    }

    auto handle = manifest.lock(key);

    if (isCommitted) {
        if (handle.setPersistedHighSeqno(seqno)) {
            // Update the 'mutated' set, stats are changing
            mutated.insert(key.getCollectionID());
            return Flush::StatsUpdate{std::move(handle)};
        } else {
            // Cannot set the seqno (flushing dropped items) no more updates
            return {};
        }
    }

    return Flush::StatsUpdate{std::move(handle)};
}

void Flush::StatsUpdate::insert(bool isCommitted,
                                bool isDelete,
                                ssize_t diskSizeDelta) {
    if (!isDelete && isCommitted) {
        incrementDiskCount();
    } // else inserting a tombstone or it's a prepare

    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void Flush::StatsUpdate::update(bool isCommitted, ssize_t diskSizeDelta) {
    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void Flush::StatsUpdate::remove(bool isCommitted, ssize_t diskSizeDelta) {
    if (isCommitted) {
        decrementDiskCount();
    } // else inserting a tombstone or it's a prepare

    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void Flush::checkAndTriggerPurge(Vbid vbid, KVBucket& bucket) const {
    if (needsPurge) {
        triggerPurge(vbid, bucket);
    }
}

void Flush::triggerPurge(Vbid vbid, KVBucket& bucket) {
    CompactionConfig config;
    config.db_file_id = vbid;
    bucket.scheduleCompaction(vbid, config, nullptr);
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        bool isCommitted,
                        bool isDelete,
                        size_t size) {
    auto update = tryTolockAndSetPersistedSeqno(key, seqno, isCommitted);
    if (update) {
        update->insert(isCommitted, isDelete, size);
    }
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        bool isCommitted,
                        bool isDelete,
                        size_t size,
                        uint64_t oldSeqno,
                        bool oldIsDelete,
                        size_t oldSize) {
    auto update = tryTolockAndSetPersistedSeqno(key, seqno, isCommitted);
    if (update) {
        if (update->isLogicallyDeleted(oldSeqno) || oldIsDelete) {
            update->insert(isCommitted, isDelete, size);
        } else if (!oldIsDelete && isDelete) {
            update->remove(isCommitted, size - oldSize);
        } else {
            update->update(isCommitted, size - oldSize);
        }
    }
}

} // namespace Collections::VB