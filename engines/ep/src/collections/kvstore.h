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

/**
 * This file contains collection data structures that KVStore must maintain.
 *  - Manifest is a structure that KVStore returns (from getCollectionsManifest)
 *  - CommitMetaData is a structure that KVStore maintains in response to system
 *    events being stored.
 */

#pragma once

#include "collections/collections_types.h"
#include <flatbuffers/flatbuffers.h>
#include <unordered_map>
#include <vector>

class DiskDocKey;
class Item;

namespace Collections {
namespace KVStore {

/**
 * KVStore will store the start-seqno of the collection and the meta-data of
 * the collection.
 */
struct OpenCollection {
    int64_t startSeqno;
    CollectionMetaData metaData;
    bool operator==(const OpenCollection& other) const;
};

/**
 * KVStore will store the start-seqno of the scope and the meta-data of
 * the scope.
 */
struct OpenScope {
    int64_t startSeqno;
    ScopeMetaData metaData;
    bool operator==(const OpenScope& other) const;
};

/**
 * Data that KVStore is required return to from KVStore::getCollectionsManifest
 * This data can be used to construct a Collections::VB::Manifest
 *
 * Default construction will result in the "default" manifest
 * - Default collection exists (since the beginning of time)
 * - Default Scope exists
 * - manifest UID of 0
 * - no dropped collections
 */
struct Manifest {
    struct Default {};
    struct Empty {};

    /**
     * Default results in the "default" manifest
     * - Default collection exists (since the beginning of time)
     * - Default Scope exists (since the beginning of time)
     * - manifest UID of 0
     * - no dropped collections
     */
    explicit Manifest(Default)
        : collections{{CollectionID::Default, {0, {}}}},
          scopes{{ScopeID::Default, {0, {}}}} {
    }

    /**
     * Empty results in
     * - no collections
     * - no scopes
     * - manifest UID of 0
     * - no dropped collections
     */
    explicit Manifest(Empty) {
    }

    ~Manifest(){};

    /// The uid of the last manifest to change the collection state
    ManifestUid manifestUid{0};

    /// A vector of collections that are available
    std::vector<OpenCollection> collections;

    /// A vector of scopes that are available
    std::vector<OpenScope> scopes;

    /**
     * true if KVStore has collection data belonging to dropped collections.
     * i.e. a collection was dropped but has not yet been 100% erased from
     * storage. KVBucket can decide to schedule purging based on this bool
     */
    bool droppedCollectionsExist{false};
};

/**
 * A dropped collection stores the seqno range it spans and the collection-ID
 */
struct DroppedCollection {
    int64_t startSeqno;
    int64_t endSeqno;
    CollectionID collectionId;
};

/**
 * Data that KVStore will maintain as the EPBucket flusher writes system events
 * The underlying implementation of KVStore can optionally persist this data
 * in any format to allow for an implementation of:
 *   -  KVStore::getCollectionsManifest
 *   -  KVStore::getDroppedCollections
 */
struct CommitMetaData {
    void clear();

    /**
     * Set the ManifestUid from the create/drop events (but only the greatest
     * observed).
     */
    void setManifestUid(ManifestUid in);

    /**
     * Record that a create collection was present in a commit batch
     */
    void recordCreateCollection(const Item& item);

    /**
     * Record that a drop collection was present in a commit batch
     */
    void recordDropCollection(const Item& item);

    /**
     * Record that a create scope was present in a commit batch
     */
    void recordCreateScope(const Item& item);

    /**
     * Record that a drop scope was present in a commit batch
     */
    void recordDropScope(const Item& item);

    /**
     * The most recent manifest committed, if needsCommit is true this value
     * must be stored by the underlying KVStore.
     */
    ManifestUid manifestUid{0};

    struct CollectionSpan {
        OpenCollection low;
        OpenCollection high;
    };
    /**
     * For each collection created in the batch, we record meta data of the
     * first and last (high/low by-seqno). If the collection was created once,
     * both entries are the same.
     */
    std::unordered_map<CollectionID, CollectionSpan> collections;

    /**
     * For each scope created in the batch, we record meta data for the greatest
     * by-seqno.
     */
    std::unordered_map<ScopeID, OpenScope> scopes;

    /**
     * For each collection dropped in the batch, we record the metadata of the
     * greatest
     */
    std::unordered_map<CollectionID, DroppedCollection> droppedCollections;

    /**
     * For each scope dropped in the batch, we record the greatest seqno
     */
    std::unordered_map<ScopeID, int64_t> droppedScopes;

    /**
     * Set to true when any of the fields in this structure have data which
     * should be saved in the KVStore update/commit. The underlying KVStore
     * reads this data and stores it in any suitable format (e.g. flatbuffers).
     */
    bool needsCommit{false};
};

/**
 * Decode the buffers from the local doc store into the collections
 * data structures.
 * @param manifest buffer containing manifest meta data
 * @param collections buffer containing open collections
 * @param scopes buffer containing open scopes
 * @param dropped buffer containing dropped collections
 */
Manifest decodeManifest(cb::const_byte_buffer manifest,
                        cb::const_byte_buffer collections,
                        cb::const_byte_buffer scopes,
                        cb::const_byte_buffer dropped);

/**
 * Decode the local doc buffer into the dropped collections data structure.
 * @param dc buffer containing dropped collections
 */
std::vector<Collections::KVStore::DroppedCollection> decodeDroppedCollections(
        cb::const_byte_buffer dc);

/**
 * Encode the manifest commit meta data into a flatbuffer
 * @param meta manifest commit meta data
 */
flatbuffers::DetachedBuffer encodeManifestUid(
        Collections::KVStore::CommitMetaData& meta);

/**
 * Encode the open collections list into a flatbuffer. Includes merging
 * with what was read off disk.
 * @param droppedCollections dropped collections list (read from KVStore)
 * @param collectionsMeta manifest commit meta data
 * @param collections existing flatbuffer data for open collections
 */
flatbuffers::DetachedBuffer encodeOpenCollections(
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections,
        Collections::KVStore::CommitMetaData& collectionsMeta,
        cb::const_byte_buffer collections);

/**
 * Encode the dropped collection list as flatbuffer.
 *
 * @param collectionsMeta manifest commit meta data generated by the current
 *        flush batch.
 * @param dropped list of collections that are already dropped (read from
 *        storage)
 * @return The dropped list (as a flatbuffer type)
 */
flatbuffers::DetachedBuffer encodeDroppedCollections(
        Collections::KVStore::CommitMetaData& collectionsMeta,
        std::vector<Collections::KVStore::DroppedCollection>& dropped);

/**
 * Encode open scopes list into flat buffer.
 * @param collectionsMeta manifest commit meta data
 * @param scopes open scopes list
 */
flatbuffers::DetachedBuffer encodeOpenScopes(
        const Collections::KVStore::CommitMetaData& collectionsMeta,
        cb::const_byte_buffer scopes);

/**
 * Callback to inform KV-engine that KVStore dropped key@seqno and whether or
 * not it is an abort. Also passes the PCS (Persisted Completed Seqno) which is
 * used to avoid calling into the DM to drop keys that won't exist in the DM.
 */
/**
 * Callback to inform kv_engine that KVStore dropped key@seqno.
 *
 * @param key - the key
 * @param seqno - the seqno
 * @param aborted - if the key is for an aborted SyncWrite
 * @param pcs - the Persisted Completed Seqno in the compaction context. Used
 *              to avoid calling into the DM to drop keys that won't exist.
 */
using DroppedCb = std::function<void(
        const DiskDocKey& key, int64_t seqno, bool aborted, int64_t pcs)>;

} // end namespace KVStore
} // end namespace Collections
