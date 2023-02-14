/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/scan_context.h"

#include "collections/kvstore.h"
#include "systemevent_factory.h"

namespace Collections::VB {

ScanContext::ScanContext(
        const std::vector<Collections::KVStore::OpenCollection>*
                openCollections,
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections) {
    for (const auto& droppedCollection : droppedCollections) {
        dropped.insert(droppedCollection.collectionId);
        // Find the full extent of dropped collections, we will only map lookup
        // for keys inside the range.
        startSeqno =
                std::min<uint64_t>(startSeqno, droppedCollection.startSeqno);
        endSeqno = std::max<uint64_t>(endSeqno, droppedCollection.endSeqno);
    }

    if (openCollections) {
        canCheckAliveMap = true;
        for (const auto& collection : *openCollections) {
            Expects(alive.emplace(collection.metaData.cid,
                                  collection.startSeqno)
                            .second);
        }
    }
}

bool ScanContext::isLogicallyDeleted(const DocKey& key,
                                     bool isDeleted,
                                     uint64_t seqno) const {
    if (alive.empty() && dropped.empty()) {
        return false;
    }

    // Need to process - extract the CollectionID of this key
    CollectionID cid;
    if (key.isInSystemCollection()) {
        // For a system event key extract the type and id
        auto [event, id] = SystemEventFactory::getTypeAndID(key);

        // For Scope events return false, they don't require processing
        // or the event is a dropped collection "marker"
        if (event == SystemEvent::Scope || isDeleted) {
            return false;
        }
        cid = CollectionID(id);
    } else {
        cid = key.getCollectionID();
    }

    if (!dropped.empty() && isLogicallyDeleted(cid, isDeleted, seqno)) {
        return true;
    } else if (canCheckAliveMap) {
        // Check open collections
        auto itr = alive.find(cid);
        if (itr != alive.end()) {
            // if collection starts above seqno, this item is gone
            return itr->second > seqno;
        }

        // Not in the alive map, thus is dropped
        return true;
    }
    return false;
}

bool ScanContext::isLogicallyDeleted(CollectionID cid,
                                     bool isDeleted,
                                     uint64_t seqno) const {
    // Is the key in a range which contains dropped collections and in the set?
    return (seqno >= startSeqno && seqno <= endSeqno) && dropped.count(cid) > 0;
}

std::ostream& operator<<(std::ostream& os, const ScanContext& scanContext) {
    os << "ScanContext: startSeqno:" << scanContext.startSeqno
       << ", endSeqno:" << scanContext.endSeqno;
    os << " dropped:[";
    for (CollectionID cid : scanContext.dropped) {
        os << cid.to_string() << ", ";
    }
    os << "]\nalive:[";
    for (const auto& entry : scanContext.alive) {
        os << entry.first.to_string() << ":" << entry.second << ", ";
    }
    os << "]";
    return os;
}

} // namespace Collections::VB
