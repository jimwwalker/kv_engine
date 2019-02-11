/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "collections/collections_types.h"
#include "item.h"

namespace Collections {
namespace KVStore {

/**
 * KVStore will return the start-seqno of the collection
 */
struct OpenCollection {
    int64_t startSeqno;
    CollectionMetaData meta;
};

/**
 * Data that KVStore is required return to through
 * KVStore::getCollectionsManifest in order to create a
 * Collections::VB::Manifest
 */
struct Manifest {
    /// The uid of the last manifest to change the collection state
    ManifestUid manifestUid{0};

    /// A vector of collections that are available
    std::vector<OpenCollection> collections;

    /// A vector of scopes that are available
    std::vector<ScopeID> scopes;

    /// A bool indicating if dropped collections exist in storage
    bool droppedCollectionsExist;
};

struct DroppedCollection {
    int64_t startSeqno;
    int64_t endSeqno;
    CollectionID collectionId;
};

struct OpenedCollection {
    int64_t startSeqno;
    CollectionMetaData metaData;
};

/**
 * Data that KVStore will maintain as the EPBucket flusher writes system events
 * The underlying implementation of KVStore can optionally persist this data
 * in any format to allow for a simple implementation of:
 *   -  KVStore::getCollectionsManifest
 *   -  KVStore::getDroppedCollections
 */
struct CommitMetaData {
    void clear() {
        needsCommit = false;
        collections.clear();
        scopes.clear();
        droppedCollections.clear();
        droppedScopes.clear();
        manifestUid = 0;
    }

    void setUid(ManifestUid in) {
        manifestUid = std::max<ManifestUid>(manifestUid, in);
    }

    /// The most recent manifest committed
    ManifestUid manifestUid{0};

    // The following vectors store any items that are creating/dropping scopes
    // and collections. The contents of each are used in maintaining data
    // for the subsequent generation of Collections::KVStore::Manifest
    std::vector<OpenedCollection> collections;
    std::vector<ScopeID> scopes;

    std::vector<DroppedCollection> droppedCollections;
    std::vector<ScopeID> droppedScopes;
    bool needsCommit{false};
};

/// callback to inform KV-engine that KVStore dropped key@seqno
using DroppedCb = std::function<void(const DocKey&, int64_t)>;

} // end namespace KVStore
} // end namespace Collections