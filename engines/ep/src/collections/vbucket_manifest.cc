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

#include "collections/vbucket_manifest.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/events_generated.h"
#include "collections/kvstore.h"
#include "collections/manifest.h"
#include "collections/vbucket_serialised_manifest_entry_generated.h"
#include "ep_time.h"
#include "item.h"
#include "statwriter.h"
#include "vbucket.h"

#include <memory>

namespace Collections {
namespace VB {

Manifest::Manifest()
    : scopes({{ScopeID::Default}}), defaultCollectionExists(true) {
    addNewCollectionEntry({ScopeID::Default, CollectionID::Default}, {});
}

Manifest::Manifest(const KVStore::Manifest& data)
    : defaultCollectionExists(false), manifestUid(data.manifestUid) {
    for (const auto sid : data.scopes) {
        scopes.insert(sid);
    }

    for (const auto& e : data.collections) {
        const auto& meta = e.meta;
        addNewCollectionEntry({meta.sid, meta.cid}, meta.maxTtl, e.startSeqno);
    }
}

boost::optional<CollectionID> Manifest::applyDeletions(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<CollectionID>& changes) {
    boost::optional<CollectionID> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto id : changes) {
        dropCollection(
                wHandle, vb, manifestUid, id, OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

boost::optional<Manifest::CollectionAddition> Manifest::applyCreates(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<CollectionAddition>& changes) {
    boost::optional<CollectionAddition> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& addition : changes) {
        addCollection(wHandle,
                      vb,
                      manifestUid,
                      addition.identifiers,
                      addition.name,
                      addition.maxTtl,
                      OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

boost::optional<ScopeID> Manifest::applyScopeDrops(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<ScopeID>& changes) {
    boost::optional<ScopeID> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto id : changes) {
        dropScope(wHandle, vb, manifestUid, id, OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

boost::optional<Manifest::ScopeAddition> Manifest::applyScopeCreates(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<ScopeAddition>& changes) {
    boost::optional<ScopeAddition> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& addition : changes) {
        addScope(wHandle,
                 vb,
                 manifestUid,
                 addition.sid,
                 addition.name,
                 OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

bool Manifest::update(const WriteHandle& wHandle,
                      ::VBucket& vb,
                      const Collections::Manifest& manifest) {
    auto rv = processManifest(manifest);
    if (!rv.is_initialized()) {
        EP_LOG_WARN("VB::Manifest::update cannot update {}", vb.getId());
        return false;
    } else {
        auto finalScopeCreate = applyScopeCreates(wHandle, vb, rv->scopesToAdd);
        if (finalScopeCreate) {
            auto uid = rv->collectionsToAdd.empty() &&
                                       rv->collectionsToRemove.empty() &&
                                       rv->scopesToRemove.empty()
                               ? manifest.getUid()
                               : manifestUid;
            addScope(wHandle,
                     vb,
                     uid,
                     finalScopeCreate.get().sid,
                     finalScopeCreate.get().name,
                     OptionalSeqno{/*no-seqno*/});
        }

        auto finalDeletion =
                applyDeletions(wHandle, vb, rv->collectionsToRemove);
        if (finalDeletion) {
            auto uid =
                    rv->collectionsToAdd.empty() && rv->scopesToRemove.empty()
                            ? manifest.getUid()
                            : manifestUid;
            dropCollection(wHandle,
                           vb,
                           uid,
                           *finalDeletion,
                           OptionalSeqno{/*no-seqno*/});
        }

        auto finalAddition = applyCreates(wHandle, vb, rv->collectionsToAdd);

        if (finalAddition) {
            auto uid = rv->scopesToRemove.empty() ? manifest.getUid()
                                                  : manifestUid;
            addCollection(wHandle,
                          vb,
                          uid,
                          finalAddition.get().identifiers,
                          finalAddition.get().name,
                          finalAddition.get().maxTtl,
                          OptionalSeqno{/*no-seqno*/});
        }

        // This is done last so the scope deletion follows any collection
        // deletions
        auto finalScopeDrop = applyScopeDrops(wHandle, vb, rv->scopesToRemove);

        if (finalScopeDrop) {
            dropScope(wHandle,
                      vb,
                      manifest.getUid(),
                      *finalScopeDrop,
                      OptionalSeqno{/*no-seqno*/});
        }
    }
    return true;
}

void Manifest::addCollection(const WriteHandle& wHandle,
                             ::VBucket& vb,
                             ManifestUid manifestUid,
                             ScopeCollectionPair identifiers,
                             cb::const_char_buffer collectionName,
                             cb::ExpiryLimit maxTtl,
                             OptionalSeqno optionalSeqno) {
    // 1. Update the manifest, adding or updating an entry in the collections
    // map. Specify a non-zero start and 0 for the TTL
    auto& entry = addNewCollectionEntry(identifiers, maxTtl);

    // 1.1 record the uid of the manifest which is adding the collection
    this->manifestUid = manifestUid;

    // 2. Queue a system event, this will take a copy of the manifest ready
    //    for persistence into the vb state file.
    auto seqno = queueCollectionSystemEvent(wHandle,
                                            vb,
                                            identifiers.second,
                                            collectionName,
                                            entry,
                                            false,
                                            optionalSeqno);

    EP_LOG_INFO(
            "collections: {} adding collection:[name:{},id:{:x}] to "
            "scope:{:x}, "
            "max_ttl:{} {}, "
            "replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            cb::to_string(collectionName),
            identifiers.second,
            identifiers.first,
            maxTtl.is_initialized(),
            maxTtl ? maxTtl.get().count() : 0,
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid);

    // 3. Now patch the entry with the seqno of the system event, note the copy
    //    of the manifest taken at step 1 gets the correct seqno when the system
    //    event is flushed.
    entry.setStartSeqno(seqno);
}

ManifestEntry& Manifest::addNewCollectionEntry(ScopeCollectionPair identifiers,
                                               cb::ExpiryLimit maxTtl,
                                               int64_t startSeqno,
                                               int64_t endSeqno) {
    // This method is only for when the map does not have the collection
    auto itr = map.find(identifiers.second);
    if (itr != map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "collection already exists + "
                ", collection:" +
                        identifiers.second.to_string() +
                        ", scope:" + identifiers.first.to_string() +
                        ", startSeqno:" + std::to_string(startSeqno) +
                        ", endSeqno:" + std::to_string(endSeqno));
    }

    auto inserted = map.emplace(
            identifiers.second,
            ManifestEntry(identifiers.first, maxTtl, startSeqno, endSeqno));

    if (identifiers.second.isDefaultCollection() && inserted.second) {
        defaultCollectionExists = (*inserted.first).second.isOpen();
    }

    return (*inserted.first).second;
}

void Manifest::dropCollection(const WriteHandle& wHandle,
                              ::VBucket& vb,
                              ManifestUid manifestUid,
                              CollectionID cid,
                              OptionalSeqno optionalSeqno) {
    bool processingTombstone = false;
    // A replica that receives a collection tombstone is required to persist
    // that tombstone, so the replica can switch to active consistently
    if (optionalSeqno.is_initialized() && map.count(cid) == 0) {
        // Must store an event that replicates what the active had
        processingTombstone = true;

        // Add enough state so we can generate a system event that represents
        // the tombstone. The collection's scopeID, ttl and start-seqno are
        // unknown for a tombstone (and of no use).
        // After adding the entry, we can now proceed to queue a system event
        // as normal, the system event we generate can now be used to re-trigger
        // DCP delete collection if the replica is itself DCP streamed (or made
        // active)
        addNewCollectionEntry({ScopeID::Default, cid}, {});
    }

    auto itr = map.find(cid);
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__, "did not find collection:" + cid.to_string());
    }

    // record the uid of the manifest which removed the collection
    this->manifestUid = manifestUid;

    auto seqno = queueCollectionSystemEvent(wHandle,
                                            vb,
                                            cid,
                                            {/*no name*/},
                                            itr->second,
                                            true /*delete*/,
                                            optionalSeqno);

    EP_LOG_INFO(
            "collections: {} drop of collection:{:x} from scope:{:x}"
            ", replica:{}, backfill:{}, seqno:{}, manifest:{:x} tombstone:{}",
            vb.getId(),
            cid,
            itr->second.getScopeID(),
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid,
            processingTombstone);

    if (cid.isDefaultCollection()) {
        defaultCollectionExists = false;
    }

    itr->second.setEndSeqno(seqno);

    map.erase(itr);
}

const ManifestEntry& Manifest::getManifestEntry(CollectionID identifier) const {
    auto itr = map.find(identifier);
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "did not find collection:" + identifier.to_string());
    }

    return itr->second;
}

void Manifest::completeDeletion(::VBucket& vb, CollectionID collectionID) {
    auto itr = map.find(collectionID);

    EP_LOG_INFO("collections: {} complete delete of collection:{:x}",
                vb.getId(),
                collectionID);
    // Caller should not be calling in if the collection doesn't exist
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "could not find collection:" + collectionID.to_string());
    }

    map.erase(itr);
}

void Manifest::addScope(const WriteHandle& wHandle,
                        ::VBucket& vb,
                        ManifestUid manifestUid,
                        ScopeID sid,
                        cb::const_char_buffer scopeName,
                        OptionalSeqno optionalSeqno) {
    if (isScopeValid(sid)) {
        throwException<std::logic_error>(
                __FUNCTION__, "scope already exists, scope:" + sid.to_string());
    }

    scopes.insert(sid);

    // record the uid of the manifest which added the scope
    this->manifestUid = manifestUid;

    flatbuffers::FlatBufferBuilder builder;
    auto scope = CreateScope(
            builder,
            getManifestUid(),
            sid,
            builder.CreateString(scopeName.data(), scopeName.size()));
    builder.Finish(scope);

    auto item = SystemEventFactory::make(
            SystemEvent::Scope,
            makeScopeIdIntoString(sid),
            {builder.GetBufferPointer(), builder.GetSize()},
            optionalSeqno);

    auto seqno =
            vb.addSystemEventItem(item.release(), optionalSeqno, {}, wHandle);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!optionalSeqno.is_initialized()) {
        vb.checkpointManager->createNewCheckpoint();
    }

    EP_LOG_INFO(
            "collections: {} added scope:name:{},id:{:x} "
            "replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            cb::to_string(scopeName),
            sid,
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid);
}

void Manifest::dropScope(const WriteHandle& wHandle,
                         ::VBucket& vb,
                         ManifestUid manifestUid,
                         ScopeID sid,
                         OptionalSeqno optionalSeqno) {
    // A replica receiving a dropScope for a scope is allowed, i.e. if we are
    // creating a new replica, we will see scope tombstones from the active.
    // The replica use-case is assumed by the optionalSeqno being defined
    if (!optionalSeqno && scopes.count(sid) == 0) {
        throwException<std::logic_error>(
                __FUNCTION__, "scope doesn't exist, scope:" + sid.to_string());
    }

    // In the replica usage, the sid may not exist, but we can be told to
    // drop a scope because we see a scope tombstone. erase may not erase...
    scopes.erase(sid);

    // record the uid of the manifest which removed the scope
    this->manifestUid = manifestUid;

    flatbuffers::FlatBufferBuilder builder;
    auto scope = CreateDroppedScope(builder, getManifestUid(), sid);
    builder.Finish(scope);

    auto item = SystemEventFactory::make(
            SystemEvent::Scope,
            makeScopeIdIntoString(sid),
            {builder.GetBufferPointer(), builder.GetSize()},
            optionalSeqno);

    item->setDeleted();

    auto seqno =
            vb.addSystemEventItem(item.release(), optionalSeqno, {}, wHandle);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!optionalSeqno.is_initialized()) {
        vb.checkpointManager->createNewCheckpoint();
    }

    EP_LOG_INFO(
            "collections: {} dropped scope:id:{:x} "
            "replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            sid,
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid);
}

Manifest::ProcessResult Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    ProcessResult rv = ManifestChanges();

    for (const auto& entry : map) {
        // If the entry is open and not found in the new manifest it must be
        // deleted.
        if (entry.second.isOpen() &&
            manifest.findCollection(entry.first) == manifest.end()) {
            rv->collectionsToRemove.push_back(entry.first);
        }
    }

    for (const auto scope : scopes) {
        // Remove the scopes that don't exist in the new manifest
        if (manifest.findScope(scope) == manifest.endScopes()) {
            rv->scopesToRemove.push_back(scope);
        }
    }

    // Add scopes and collections in Manifest but not in our map
    for (auto scopeItr = manifest.beginScopes();
         scopeItr != manifest.endScopes();
         scopeItr++) {
        if (std::find(scopes.begin(), scopes.end(), scopeItr->first) ==
            scopes.end()) {
            rv->scopesToAdd.push_back({scopeItr->first, scopeItr->second.name});
        }

        for (const auto& m : scopeItr->second.collections) {
            auto mapItr = map.find(m.id);

            if (mapItr == map.end()) {
                rv->collectionsToAdd.push_back(
                        {std::make_pair(scopeItr->first, m.id),
                         manifest.findCollection(m.id)->second,
                         m.maxTtl});
            } else if (mapItr->second.isDeleting()) {
                // trying to add a collection which is deleting, not allowed.
                EP_LOG_WARN("Attempt to add a deleting collection:{}:{:x}",
                            manifest.findCollection(m.id)->second,
                            m.id);
                return {};
            }
        }
    }

    return rv;
}

bool Manifest::doesKeyContainValidCollection(const DocKey& key) const {
    if (defaultCollectionExists &&
        key.getCollectionID().isDefaultCollection()) {
        return true;
    } else {
        auto itr = map.find(key.getCollectionID());
        if (itr != map.end()) {
            return itr->second.isOpen();
        }
    }
    return false;
}

bool Manifest::isScopeValid(ScopeID scopeID) const {
    return std::find(scopes.begin(), scopes.end(), scopeID) != scopes.end();
}

Manifest::container::const_iterator Manifest::getManifestEntry(
        const DocKey& key, bool allowSystem) const {
    CollectionID lookup = key.getCollectionID();
    if (allowSystem && lookup == CollectionID::System) {
        lookup = getCollectionIDFromKey(key);
    } // else we lookup with CID which if is System => fail
    return map.find(lookup);
}

Manifest::container::const_iterator Manifest::getManifestIterator(
        CollectionID id) const {
    return map.find(id);
}

bool Manifest::isLogicallyDeleted(const DocKey& key, int64_t seqno) const {
    // Only do the searching/scanning work for keys in the deleted range.
    if (key.getCollectionID().isDefaultCollection()) {
        return !defaultCollectionExists;
    }

    CollectionID lookup = key.getCollectionID();
    if (lookup == CollectionID::System) {
        lookup = getCollectionIDFromKey(key);
    }
    auto itr = map.find(lookup);
    return isLogicallyDeleted(itr, seqno);
}

bool Manifest::isLogicallyDeleted(const container::const_iterator entry,
                                  int64_t seqno) const {
    if (entry == map.end()) {
        // Not in map - definitely deleted (or never existed)
        return true;
    }

    // seqno >= 0 (so temp items etc... are ok) AND the seqno is below the
    // collection start (start is set on creation and moves with flush)
    return seqno >= 0 && seqno <= entry->second.getStartSeqno();
}

boost::optional<CollectionID> Manifest::shouldCompleteDeletion(
        const DocKey& key,
        int64_t bySeqno,
        const container::const_iterator entry) const {
    // If this is a SystemEvent key then...
    if (key.getCollectionID() == CollectionID::System) {
        if (entry->second.isDeleting()) {
            return entry->first; // returning CID
        }
    }
    return {};
}

void Manifest::processExpiryTime(const container::const_iterator entry,
                                 Item& itm,
                                 std::chrono::seconds bucketTtl) const {
    itm.setExpTime(processExpiryTime(entry, itm.getExptime(), bucketTtl));
}

time_t Manifest::processExpiryTime(const container::const_iterator entry,
                                   time_t t,
                                   std::chrono::seconds bucketTtl) const {
    std::chrono::seconds enforcedTtl{0};

    if (bucketTtl.count()) {
        enforcedTtl = bucketTtl;
    }

    // If the collection has a TTL, it gets used
    if (entry->second.getMaxTtl()) {
        enforcedTtl = entry->second.getMaxTtl().get();
    }

    // Note: A ttl value of 0 means no max_ttl
    if (enforcedTtl.count()) {
        t = ep_limit_abstime(t, enforcedTtl);
    }
    return t;
}

std::unique_ptr<Item> Manifest::makeCollectionSystemEvent(
        ManifestUid uid,
        CollectionID cid,
        cb::const_char_buffer collectionName,
        const ManifestEntry& entry,
        bool deleted,
        OptionalSeqno seq) {
    flatbuffers::FlatBufferBuilder builder;
    if (!deleted) {
        auto collection =
                CreateCollection(builder,
                                 uid,
                                 entry.getScopeID(),
                                 cid,
                                 entry.getMaxTtl().is_initialized(),
                                 entry.getMaxTtl().is_initialized()
                                         ? (*entry.getMaxTtl()).count()
                                         : 0,
                                 builder.CreateString(collectionName.data(),
                                                      collectionName.size()));
        builder.Finish(collection);
    } else {
        auto collection =
                CreateDroppedCollection(builder, uid, entry.getScopeID(), cid);
        builder.Finish(collection);
    }

    auto item = SystemEventFactory::make(
            SystemEvent::Collection,
            makeCollectionIdIntoString(cid),
            {builder.GetBufferPointer(), builder.GetSize()},
            seq);

    if (deleted) {
        item->setDeleted();
    }
    return item;
}

int64_t Manifest::queueCollectionSystemEvent(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        CollectionID cid,
        cb::const_char_buffer collectionName,
        const ManifestEntry& entry,
        bool deleted,
        OptionalSeqno seq) const {
    auto item = makeCollectionSystemEvent(
            getManifestUid(), cid, collectionName, entry, deleted, seq);
    // Create and transfer Item ownership to the VBucket
    auto rv = vb.addSystemEventItem(item.release(), seq, wHandle);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!seq.is_initialized()) {
        vb.checkpointManager->createNewCheckpoint();
    }
    return rv;
}

void Manifest::populateWithSerialisedData(
        flatbuffers::FlatBufferBuilder& builder,
        ScopeCollectionPair identifiers,
        cb::const_char_buffer collectionName) const {
    const ManifestEntry* finalEntry = nullptr;

    std::vector<flatbuffers::Offset<SerialisedManifestEntry>> entriesVector;

    for (const auto& collectionEntry : map) {
        // Check if we find the mutated entry in the map (so we know if we're
        // mutating it)
        if (collectionEntry.first == identifiers.second) {
            // If a collection in the map matches the collection being changed
            // save the iterator so we can use it when creating the final entry
            finalEntry = &collectionEntry.second;
        } else {
            uint32_t maxTtl = 0;
            if (collectionEntry.second.getMaxTtl()) {
                maxTtl = collectionEntry.second.getMaxTtl().get().count();
            }
            auto newEntry = CreateSerialisedManifestEntry(
                    builder,
                    collectionEntry.second.getStartSeqno(),
                    collectionEntry.second.getEndSeqno(),
                    collectionEntry.second.getScopeID(),
                    collectionEntry.first,
                    collectionEntry.second.getMaxTtl().is_initialized(),
                    maxTtl);
            entriesVector.push_back(newEntry);
        }
    }

    // Note that patchSerialisedData will change one of these values when the
    // real seqno is known.
    int64_t startSeqno = StoredValue::state_collection_open;
    int64_t endSeqno = StoredValue::state_collection_open;
    uint32_t maxTtl = 0;
    bool maxTtlValid = false;
    if (finalEntry) {
        startSeqno = finalEntry->getStartSeqno();
        endSeqno = finalEntry->getEndSeqno();
        if (finalEntry->getMaxTtl()) {
            maxTtl = finalEntry->getMaxTtl().get().count();
            maxTtlValid = true;
        }
    }

    auto newEntry = CreateSerialisedManifestEntry(builder,
                                                  startSeqno,
                                                  endSeqno,
                                                  identifiers.first,
                                                  identifiers.second,
                                                  maxTtlValid,
                                                  maxTtl);

    entriesVector.push_back(newEntry);
    auto entries = builder.CreateVector(entriesVector);

    std::vector<uint32_t> scopeVector;
    for (auto sid : scopes) {
        scopeVector.push_back(uint32_t(sid));
    }

    auto scopeEntries = builder.CreateVector(scopeVector);

    if (collectionName.data()) {
        auto manifest = CreateSerialisedManifest(
                builder,
                getManifestUid(),
                entries,
                scopeEntries,
                builder.CreateString(collectionName.data(),
                                     collectionName.size()));
        builder.Finish(manifest);
    } else {
        auto manifest = CreateSerialisedManifest(builder,
                                                 getManifestUid(),
                                                 entries,
                                                 scopeEntries,
                                                 builder.CreateString(""));

        builder.Finish(manifest);
    }
}

void Manifest::populateWithSerialisedData(
        flatbuffers::FlatBufferBuilder& builder,
        cb::const_char_buffer mutatedName) const {
    std::vector<flatbuffers::Offset<SerialisedManifestEntry>> entriesVector;

    for (const auto& collectionEntry : map) {
        auto newEntry = CreateSerialisedManifestEntry(
                builder,
                collectionEntry.second.getStartSeqno(),
                collectionEntry.second.getEndSeqno(),
                collectionEntry.second.getScopeID(),
                collectionEntry.first);
        entriesVector.push_back(newEntry);
    }

    auto entries = builder.CreateVector(entriesVector);

    std::vector<uint32_t> scopeVector;
    for (auto sid : scopes) {
        scopeVector.push_back(sid);
    }

    auto scopeEntries = builder.CreateVector(scopeVector);

    if (mutatedName.data()) {
        auto manifest = CreateSerialisedManifest(
                builder,
                getManifestUid(),
                entries,
                scopeEntries,
                builder.CreateString(mutatedName.data(), mutatedName.size()));
        builder.Finish(manifest);
    } else {
        auto manifest = CreateSerialisedManifest(builder,
                                                 getManifestUid(),
                                                 entries,
                                                 scopeEntries,
                                                 builder.CreateString(""));

        builder.Finish(manifest);
    }
}

PersistedManifest Manifest::getPersistedManifest(const Item& item) {
    switch (SystemEvent(item.getFlags())) {
    case SystemEvent::Collection:
        // Collection events need the endSeqno updating
        return patchSerialisedDataForCollectionEvent(item);
    case SystemEvent::Scope: {
        return patchSerialisedDataForScopeEvent(item);
    }
    default:
        throw std::invalid_argument(
                "Manifest::getManifestData: unknown event:" +
                std::to_string(int(item.getFlags())));
    }
}

PersistedManifest Manifest::patchSerialisedDataForCollectionEvent(
        const Item& item) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(item.getData());
    PersistedManifest mutableData(ptr, ptr + item.getNBytes());
    auto manifest =
            flatbuffers::GetMutableRoot<SerialisedManifest>(mutableData.data());

    // Get the last entry from entries, that is the entry to patch
    auto mutatedEntry = manifest->mutable_entries()->GetMutableObject(
            manifest->entries()->size() - 1);

    bool failed = false;
    if (item.isDeleted()) {
        failed = !mutatedEntry->mutate_endSeqno(item.getBySeqno());
    } else {
        failed = !mutatedEntry->mutate_startSeqno(item.getBySeqno());
    }

    if (failed) {
        throw std::logic_error(
                "Manifest::patchSerialisedDataForCollectionEvent failed to "
                "mutate, new seqno: " +
                std::to_string(item.getBySeqno()) +
                " isDeleted:" + std::to_string(item.isDeleted()));
    }

    return mutableData;
}

PersistedManifest Manifest::patchSerialisedDataForScopeEvent(const Item& item) {
    if (!item.isDeleted()) {
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(item.getData());
        return {ptr, ptr + item.getNBytes()};
    }

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(item.getData());
    PersistedManifest mutableData(ptr, ptr + item.getNBytes());
    auto manifest =
            flatbuffers::GetMutableRoot<SerialisedManifest>(mutableData.data());

    // Dropping a scope sets the last scope to ScopeID::Default. Why?
    // The flatbuffers scopes Vector contains as the last element, the ID of the
    // dropped scope.  The PersistedManifest we want to store (and may roll
    // back to) must not contain the dropped scope. However flatbuffers
    // scopes Vector cannot be resized, we cannot just pop_back the last
    // element. To remove the dropped id we are overwriting it with
    // ScopeID::Default. If a VB::Manifest is ever constructed from this
    // flatbuffers data it can cope with the double entry of ScopeID::Default
    // but importantly it can never bring back the dropped scope.
    manifest->mutable_scopes()->Mutate(manifest->scopes()->size() - 1,
                                       ScopeID::Default);

    return mutableData;
}

<<<<<<< HEAD
bool Manifest::isDropInProgress() const {
    return dropInProgress;
}

CreateEventData Manifest::getCreateEventData(
        cb::const_char_buffer serialisedManifest) {
    auto manifest = flatbuffers::GetRoot<SerialisedManifest>(
            (const uint8_t*)serialisedManifest.data());
=======
template <class T>
static void verifyFlatbuffersData(cb::const_char_buffer buf,
                                  const std::string& caller) {
    flatbuffers::Verifier v(reinterpret_cast<const uint8_t*>(buf.data()),
                            buf.size());
    if (v.VerifyBuffer<T>(nullptr)) {
        return;
    }
>>>>>>> feb4fa8f9... MB-32784: 4/5 Streamline collection system events

    std::stringstream ss;
    ss << "Collections::VB::Manifest::verifyFlatbuffersData: " << caller
       << " data invalid, ptr:" << reinterpret_cast<const void*>(buf.data())
       << ", size:" << buf.size();

    throw std::runtime_error(ss.str());
}

CreateEventData Manifest::getCreateEventData(
        cb::const_char_buffer flatbufferData) {
    verifyFlatbuffersData<Collection>(flatbufferData, "getCreateEventData");
    auto collection = flatbuffers::GetRoot<Collection>(
            reinterpret_cast<const uint8_t*>(flatbufferData.data()));

    // if maxTtlValid needs considering
    cb::ExpiryLimit maxTtl;
    if (collection->ttlValid()) {
        maxTtl = std::chrono::seconds(collection->maxTtl());
    }
    return {collection->uid(),
            {collection->scopeId(),
             collection->collectionId(),
             collection->name()->str(),
             maxTtl}};
}

DropEventData Manifest::getDropEventData(cb::const_char_buffer flatbufferData) {
    verifyFlatbuffersData<DroppedCollection>(flatbufferData,
                                             "getDropEventData");
    auto droppedCollection = flatbuffers::GetRoot<DroppedCollection>(
            (const uint8_t*)flatbufferData.data());

    return {droppedCollection->uid(),
            droppedCollection->scopeId(),
            droppedCollection->collectionId()};
}

CreateScopeEventData Manifest::getCreateScopeEventData(
        cb::const_char_buffer flatbufferData) {
    verifyFlatbuffersData<Scope>(flatbufferData, "getCreateScopeEventData");
    auto scope = flatbuffers::GetRoot<Scope>(
            reinterpret_cast<const uint8_t*>(flatbufferData.data()));

    return {scope->uid(), scope->scopeId(), scope->name()->str()};
}

DropScopeEventData Manifest::getDropScopeEventData(
        cb::const_char_buffer flatbufferData) {
    verifyFlatbuffersData<DroppedScope>(flatbufferData,
                                        "getDropScopeEventData");
    auto droppedScope = flatbuffers::GetRoot<DroppedScope>(
            reinterpret_cast<const uint8_t*>(flatbufferData.data()));

    return {droppedScope->uid(), droppedScope->scopeId()};
}

std::string Manifest::getExceptionString(const std::string& thrower,
                                         const std::string& error) const {
    std::stringstream ss;
    ss << "VB::Manifest:" << thrower << ": " << error << ", this:" << *this;
    return ss.str();
}

uint64_t Manifest::getItemCount(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    // For now link through to disk count
    // @todo: ephemeral support
    return itr->second.getDiskCount();
}

uint64_t Manifest::getHighSeqno(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.getHighSeqno();
}

void Manifest::setHighSeqno(CollectionID collection, uint64_t value) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    itr->second.setHighSeqno(value);
}

uint64_t Manifest::getPersistedHighSeqno(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.getPersistedHighSeqno();
}

void Manifest::setPersistedHighSeqno(CollectionID collection,
                                     uint64_t value,
                                     bool noThrow) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        if (noThrow) {
            return;
        }
        throwException<std::logic_error>(
                __FUNCTION__,
                "did not find collection:" + collection.to_string());
    }
    itr->second.setPersistedHighSeqno(value);
}

bool Manifest::addCollectionStats(Vbid vbid,
                                  const void* cookie,
                                  const AddStatFn& add_stat) const {
    try {
        const int bsize = 512;
        char buffer[bsize];
        checked_snprintf(buffer, bsize, "vb_%d:manifest:entries", vbid.get());
        add_casted_stat(buffer, map.size(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:manifest:default_exists", vbid.get());
        add_casted_stat(buffer, defaultCollectionExists, add_stat, cookie);
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "VB::Manifest::addCollectionStats {}, failed to build stats "
                "exception:{}",
                vbid,
                e.what());
        return false;
    }
    for (const auto& entry : map) {
        if (!entry.second.addStats(
                    entry.first.to_string(), vbid, cookie, add_stat)) {
            return false;
        }
    }
    return true;
}

bool Manifest::addScopeStats(Vbid vbid,
                             const void* cookie,
                             const AddStatFn& add_stat) const {
    const int bsize = 512;
    char buffer[bsize];
    try {
        checked_snprintf(buffer, bsize, "vb_%d:manifest:scopes", vbid.get());
        add_casted_stat(buffer, scopes.size(), add_stat, cookie);
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "VB::Manifest::addScopeStats {}, failed to build stats "
                "exception:{}",
                vbid,
                e.what());
        return false;
    }

    // We'll also print the iteration index of each scope and collection.
    // This is particularly useful for scopes as the ordering of the
    // container matters when we deal with scope deletion events. It's less
    // useful for collection stats, but allows us to print the CollectionID
    // as the value and ensures that we still have unique keys which is a
    // requirement of stats.
    int i = 0;
    for (auto it = scopes.begin(); it != scopes.end(); it++, i++) {
        checked_snprintf(
                buffer, bsize, "vb_%d:manifest:scopes:%d", vbid.get(), i);
        add_casted_stat(buffer, it->to_string().c_str(), add_stat, cookie);
    }

    i = 0;
    for (auto it = map.begin(); it != map.end(); it++, i++) {
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:manifest:scope:%s:collection:%d",
                         vbid.get(),
                         it->second.getScopeID().to_string().c_str(),
                         i);
        add_casted_stat(
                buffer, it->first.to_string().c_str(), add_stat, cookie);
    }

    return true;
}

void Manifest::updateSummary(Summary& summary) const {
    for (const auto& entry : map) {
        auto s = summary.find(entry.first);
        if (s == summary.end()) {
            summary[entry.first] = entry.second.getDiskCount();
        } else {
            s->second += entry.second.getDiskCount();
        }
    }
}

boost::optional<std::vector<CollectionID>> Manifest::getCollectionsForScope(
        ScopeID identifier) const {
    if (std::find(scopes.begin(), scopes.end(), identifier) == scopes.end()) {
        return {};
    }

    std::vector<CollectionID> rv;
    for (const auto& collection : map) {
        if (collection.second.getScopeID() == identifier) {
            rv.push_back(collection.first);
        }
    }

    return rv;
}

bool Manifest::operator==(const Manifest& rhs) const {
    std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
    std::lock_guard<cb::ReaderLock> otherReadLock(rhs.rwlock.reader());

    if (rhs.map.size() != map.size()) {
        return false;
    }
    // Check all collections match
    for (const auto& e : map) {
        const auto& entry1 = e.second;
        const auto& entry2 = rhs.getManifestEntry(e.first);

        if (!(entry1 == entry2)) {
            return false;
        }
    }

    if (scopes.size() != rhs.scopes.size()) {
        return false;
    }
    // Check all scopes can be found
    for (const auto s : scopes) {
        if (std::find(rhs.scopes.begin(), rhs.scopes.end(), s) ==
            rhs.scopes.end()) {
            return false;
        }
    }
    if (rhs.manifestUid != manifestUid) {
        return false;
    }

    return true;
}

bool Manifest::operator!=(const Manifest& rhs) const {
    return !(*this == rhs);
}

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "VB::Manifest: "
       << "uid:" << manifest.manifestUid
       << ", defaultCollectionExists:" << manifest.defaultCollectionExists
       << ", scopes.size:" << manifest.scopes.size()
       << ", map.size:" << manifest.map.size() << std::endl;
    for (auto& m : manifest.map) {
        os << "cid:" << m.first.to_string() << ":" << m.second << std::endl;
    }

    for (auto s : manifest.scopes) {
        os << "scope:" << s.to_string() << std::endl;
    }

    return os;
}

std::ostream& operator<<(std::ostream& os,
                         const Manifest::ReadHandle& readHandle) {
    os << "VB::Manifest::ReadHandle: manifest:" << *readHandle.manifest;
    return os;
}

std::ostream& operator<<(std::ostream& os,
                         const Manifest::CachingReadHandle& readHandle) {
    os << "VB::Manifest::CachingReadHandle: itr:";
    if (readHandle.valid()) {
        os << (*readHandle.itr).second;
    } else {
        os << "end";
    }
    os << ", cid:" << readHandle.key.getCollectionID().to_string();
    os << ", manifest:" << *readHandle.manifest;
    return os;
}

} // end namespace VB
} // end namespace Collections
