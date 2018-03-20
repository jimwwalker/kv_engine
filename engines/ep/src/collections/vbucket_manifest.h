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

#pragma once

#include "collections/collections_dockey.h"
#include "collections/collections_types.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest_entry.h"
#include "systemevent.h"

#include <platform/non_negative_counter.h>
#include <platform/rwlock.h>
#include <platform/sized_buffer.h>

#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

class VBucket;

namespace Collections {
namespace VB {

/**
 * Collections::VB::Manifest is a container for all of the collections a VBucket
 * knows about.
 *
 * Each collection is represented by a Collections::VB::ManifestEntry and all of
 * the collections are stored in an unordered_map. The map is implemented to
 * allow look-up by collection-name without having to allocate a std::string,
 * callers only need a cb::const_char_buffer for look-ups.
 *
 * The Manifest allows for an external manager to drive the lifetime of each
 * collection - adding, begin/complete of the deletion phase.
 *
 * This class is intended to be thread-safe when accessed through the read
 * or write handles (providing RAII locking).
 *
 * Access to the class is peformed by the ReadHandle and WriteHandle classes
 * which perform RAII locking on the manifest's internal lock. A user of the
 * manifest is required to hold the correct handle for the required scope to
 * to ensure any actions they take based upon a collection's existence are
 * consistent. The important consistency issue is the checkpoint. For example
 * when setting a document code must first check the document's collection
 * exists, the document must then only enter the checkpoint after the creation
 * event for the collection and also before a delete event foe the collection.
 * Thus the set path must obtain read access to collections and keep read access
 * for the entire scope of the set path to ensure no other thread can interleave
 * collection create/delete and cause an inconsistency in the checkpoint
 * ordering.
 */
class Manifest {
public:
    /**
     * Map from a 'string_view' to an entry.
     * The key points to data stored in the value (which is actually a pointer
     * to a value to remove issues where objects move).
     * Using the string_view as the key allows faster lookups, the caller
     * need not heap allocate.
     */
    using container = ::std::unordered_map<cb::const_char_buffer,
                                           std::unique_ptr<ManifestEntry>>;

    /**
     * RAII read locking for access to the Manifest.
     */
    class ReadHandle {
    public:
        ReadHandle(const Manifest& m, cb::RWLock& lock)
            : readLock(lock), manifest(m) {
        }

        ReadHandle(ReadHandle&& rhs)
            : readLock(std::move(rhs.readLock)), manifest(rhs.manifest) {
        }

        /**
         * Does the key contain a valid collection?
         *
         * - If the key applies to the default collection, the default
         *   collection must exist.
         *
         * - If the key applies to a collection, the collection must exist and
         *   must not be in the process of deletion.
         */
        bool doesKeyContainValidCollection(::DocKey key) const {
            return manifest.doesKeyContainValidCollection(key);
        }

        /**
         * Given a key and it's seqno, the manifest can determine if that key
         * is logically deleted - that is part of a collection which is in the
         * process of being erased.
         *
         * @return true if the key belongs to a deleted collection.
         */
        bool isLogicallyDeleted(::DocKey key, int64_t seqno) const {
            return manifest.isLogicallyDeleted(key, seqno);
        }

        bool isLogicallyDeleted(::DocKey key,
                                int64_t seqno,
                                const std::string& separator) const {
            return manifest.isLogicallyDeleted(key, seqno, separator);
        }

        /**
         * @returns a copy of the current separator
         */
        std::string getSeparator() const {
            return manifest.getSeparator();
        }

        DocKey makeCollectionsDocKey(::DocKey key) const {
            return Collections::DocKey::make(key, manifest.getSeparator());
        }

        /**
         * @returns true/false if $default exists
         */
        bool doesDefaultCollectionExist() const {
            return manifest.doesDefaultCollectionExist();
        }

        /**
         * @returns true if collection is open, false if not or unknown
         */
        bool isCollectionOpen(cb::const_char_buffer collection) const {
            return manifest.isCollectionOpen(collection);
        }

        /**
         * @returns true if collection is open, false if not or unknown
         */
        bool isCollectionOpen(Identifier identifier) const {
            return manifest.isCollectionOpen(identifier);
        }

        /**
         * @return true if the collection exists in the internal container
         */
        bool exists(cb::const_char_buffer collection) const {
            return manifest.exists(collection);
        }

        /// @return the manifest UID that last updated this vb::manifest
        uid_t getManifestUid() const {
            return manifest.getManifestUid();
        }

        uint64_t getItemCount(cb::const_char_buffer collection) const {
            return manifest.getItemCount(collection);
        }

        /// @return interator to the beginning of the underlying collection map
        container::const_iterator begin() const {
            return manifest.begin();
        }

        /// @return interator to the end of the underlying collection map
        container::const_iterator end() const {
            return manifest.end();
        }

        /**
         * Dump the manifest to std::cerr
         */
        void dump() const {
            std::cerr << manifest << std::endl;
        }

    protected:
        friend std::ostream& operator<<(std::ostream& os,
                                        const Manifest::ReadHandle& readHandle);
        std::unique_lock<cb::ReaderLock> readLock;
        const Manifest& manifest;
    };

    /**
     * CachingReadHandle provides a limited set of functions to allow various
     * functional paths in KV-engine to perform collection 'legality' checks
     * with minimal key scans and map lookups.
     *
     * The pattern is that the caller creates a CachingReadHandle and during
     * creation of the object, the key is scanned for a collection and then
     * an attempt to find a manifest entry is made, the result of which is
     * stored.
     *
     * The caller next can check if the read handle represents a valid
     * collection, allowing code to return 'unknown_collection'.
     *
     * Finally a caller can pass a seqno into the isLogicallyDeleted function
     * to test if that seqno is a logically deleted key. The seqno should have
     * been found by searching for the key used during in construction.
     *
     * Privately inherited from ReadHandle so we have a readlock/manifest
     * without exposing the ReadHandle public methods that don't quite fit in
     * this class.
     */
    class CachingReadHandle : private ReadHandle {
    public:
        CachingReadHandle(const Manifest& m,
                          cb::RWLock& lock,
                          ::DocKey key,
                          const std::string& separator)
            : ReadHandle(m, lock),
              key(key.getDocNamespace() == DocNamespace::System
                          ? DocKey::make(key)
                          : DocKey::make(key, separator)),
              itr(m.getManifestEntry(this->key, separator)) {
        }

        CachingReadHandle(const Manifest& m, cb::RWLock& lock, ::DocKey key)
            : CachingReadHandle(m, lock, key, m.getSeparator()) {
        }

        /**
         * @return true if the key used in construction is associated with a
         *         valid and open collection.
         */
        bool valid() const {
            if (iteratorValid()) {
                return itr->second->isOpen();
            }
            return false;
        }

        /**
         * @return the key used in construction
         */
        ::DocKey getKey() const {
            return key;
        }

        /**
         * @return true if the seqno is logically deleted. Return of true would
         *         mean that this seqno is below the start seqno of the
         *         collection used in construction of this read handle instance.
         */
        bool isLogicallyDeleted(int64_t seqno) const {
            return manifest.isLogicallyDeleted(itr, seqno);
        }

        /// @return the manifest UID that last updated this vb::manifest
        uid_t getManifestUid() const {
            return manifest.getManifestUid();
        }

        /// increment the key's collection item count
        void incrementDiskCount() const {
            // We don't include system events when counting
            if (key.getDocNamespace() == DocNamespace::System) {
                return;
            }
            return manifest.incrementDiskCount(itr);
        }

        /// decrement the key's collection item count
        void decrementDiskCount() const {
            // We don't include system events when counting
            if (key.getDocNamespace() == DocNamespace::System) {
                return;
            }
            return manifest.decrementDiskCount(itr);
        }

        /**
         * Function intended for use by the KVBucket collection's eraser code.
         *
         * @return if the key (used in construction) indicates that we've now
         *         hit the end of a deleted collection (because the key is a
         *         system event) and the manifest entry determines we should
         *         call completeDeletion, then the return value is initialised
         *         with the collection which is to be deleted. If the key does
         *         not indicate the end, the return value is an empty buffer.
         */
        boost::optional<cb::const_char_buffer> shouldCompleteDeletion(
                int64_t bySeqno) const {
            return manifest.shouldCompleteDeletion(key, bySeqno, itr);
        }

        /**
         * Dump the manifest to std::cerr
         */
        void dump() {
            std::cerr << manifest << std::endl;
        }

    protected:
        bool iteratorValid() const {
            return itr != manifest.end();
        }

        friend std::ostream& operator<<(
                std::ostream& os,
                const Manifest::CachingReadHandle& readHandle);

        /**
         * Collections::DocKey built from the key used in creation of this
         * handle.
         */
        DocKey key;

        /**
         * An iterator for the key's collection, or end if the key has no valid
         * collection.
         */
        container::const_iterator itr;

    };

    /**
     * RAII write locking for access and updates to the Manifest.
     */
    class WriteHandle {
    public:
        WriteHandle(Manifest& m, cb::RWLock& lock)
            : writeLock(lock), manifest(m) {
        }

        WriteHandle(WriteHandle&& rhs)
            : writeLock(std::move(rhs.writeLock)), manifest(rhs.manifest) {
        }

        /**
         * Update from a Collections::Manifest
         *
         * Update compares the current collection set against the manifest and
         * triggers collection creation and collection deletion.
         *
         * Creation and deletion of a collection are pushed into the VBucket and
         * the seqno of updates is recorded in the manifest.
         *
         * @param vb The VBucket to update (queue data into).
         * @param manifest The incoming manifest to compare this object with.
         */
        void update(::VBucket& vb, const Collections::Manifest& newManifest) {
            manifest.update(vb, newManifest);
        }

        /**
         * Complete the deletion of a collection.
         *
         * Lookup the collection name and determine the deletion actions.
         * A collection could of been added again during a background delete so
         * completeDeletion may just update the state or fully drop all
         * knowledge of the collection.
         *
         * @param vb The VBucket in which the deletion is occurring.
         * @param collection The collection that has finished being deleted.
         */
        void completeDeletion(::VBucket& vb, cb::const_char_buffer collection) {
            manifest.completeDeletion(vb, collection);
        }

        /**
         * Add a collection for a replica VB, this is for receiving
         * collection updates via DCP and the collection already has a start
         * seqno assigned.
         *
         * @param vb The vbucket to add the collection to.
         * @param manifestUid the uid of the manifest which made the change
         * @param identifier Identifier of the new collection.
         * @param startSeqno The start-seqno assigned to the collection.
         */
        void replicaAdd(::VBucket& vb,
                        uid_t manifestUid,
                        Identifier identifier,
                        int64_t startSeqno) {
            manifest.addCollection(
                    vb, manifestUid, identifier, OptionalSeqno{startSeqno});
        }

        /**
         * Begin a delete collection for a replica VB, this is for receiving
         * collection updates via DCP and the collection already has an end
         * seqno assigned.
         *
         * @param vb The vbucket to begin collection deletion on.
         * @param manifestUid the uid of the manifest which made the change
         * @param identifier Identifier of the deleted collection.
         * @param endSeqno The end-seqno assigned to the end collection.
         */
        void replicaBeginDelete(::VBucket& vb,
                                uid_t manifestUid,
                                Identifier identifier,
                                int64_t endSeqno) {
            manifest.beginCollectionDelete(
                    vb, manifestUid, identifier, OptionalSeqno{endSeqno});
        }

        /**
         * Change the separator for a replica VB, this is for receiving
         * collection updates via DCP and the event already has an end seqno
         * assigned.
         *
         * @param vb The vbucket to begin collection deletion on.
         * @param manifestUid the uid of the manifest which made the change
         * @param separator The new separator.
         * @param seqno The seqno originally assigned to the active's system
         * event.
         */
        void replicaChangeSeparator(::VBucket& vb,
                                    uid_t manifestUid,
                                    cb::const_char_buffer separator,
                                    int64_t seqno) {
            manifest.changeSeparator(
                    vb, manifestUid, separator, OptionalSeqno{seqno});
        }

        /**
         * Dump the manifest to std::cerr
         */
        void dump() {
            std::cerr << manifest << std::endl;
        }

    private:
        std::unique_lock<cb::WriterLock> writeLock;
        Manifest& manifest;
    };

    friend ReadHandle;
    friend CachingReadHandle;
    friend WriteHandle;

    /**
     * Construct a VBucket::Manifest from a JSON string or an empty string.
     *
     * Empty string allows for construction where no JSON data was found i.e.
     * an upgrade occurred and this is the first construction of a manifest
     * for a VBucket which has persisted data, but no manifest data. When an
     * empty string is used, the manifest will initialise with default settings.
     * - Default Collection enabled.
     * - Separator defined as Collections::DefaultSeparator
     *
     * A non-empty string must be a valid JSON manifest that determines which
     * collections to instantiate.
     *
     * @param manifest A std::string containing a JSON manifest. An empty string
     *        indicates no manifest and is valid.
     */
    Manifest(const std::string& manifest);

    ReadHandle lock() const {
        return {*this, rwlock};
    }

    CachingReadHandle lock(::DocKey key) const {
        return {*this, rwlock, key};
    }

    CachingReadHandle lock(::DocKey key, const std::string& separator) const {
        return {*this, rwlock, key, separator};
    }

    WriteHandle wlock() {
        return {*this, rwlock};
    }

    std::pair<std::string, uint64_t> getNextStat();
    void clearMutated() {
        mutated.clear();
    }

    /**
     * Return a std::string containing a JSON representation of a
     * VBucket::Manifest. The input is an Item previously created for an event
     * with the value being a serialised binary blob which is turned into JSON.
     *
     * When the Item was created it did not have a seqno for the collection
     * entry being modified, this function will return JSON data with the
     * missing seqno 'patched' with the Item's seqno.
     *
     * @param collectionsEventItem an Item created to represent a collection
     *        event. The value of which is converted to JSON.
     * @return JSON representation of the Item's value.
     */
    static std::string serialToJson(const Item& collectionsEventItem);

    /**
     * Get the system event data from a SystemEvent, that is the information
     * that DCP would require to send a SystemEvent to a client.
     *
     * @param serialisedManifest Serialised manifest data created by
     *        ::populateWithSerialisedData
     * @returns SystemEventData which carries all of the data which needs to be
     *          marshalled into a DCP system event message. Inside the returned
     *          object maybe sized_buffer objects which point into
     *          serialisedManifest.
     */
    static SystemEventData getSystemEventData(
            cb::const_char_buffer serialisedManifest);

    /**
     * Get the system event data from a SystemEvent, that is the information
     * that DCP would require to send a SystemEvent to a client.
     *
     * This particular function returns separator changed data.
     *
     * @param serialisedManifest Serialised manifest data created by
     *        ::populateWithSerialisedData
     * @returns A buffer that contains a pointer/size to the separator.
     * @returns SystemEventSeparatorData which carries all of the data which
     *          needs to be marshalled into a DCP system event message. Inside
     *          the returned object maybe sized_buffer objects which point into
     *          serialisedManifest.
     */
    static SystemEventSeparatorData getSystemEventSeparatorData(
            cb::const_char_buffer serialisedManifest);

private:

    /**
     * Return a std::string containing a JSON representation of a
     * VBucket::Manifest. The input data should be a previously serialised
     * object, i.e. the input to this function is the output of
     * populateWithSerialisedData(cb::char_buffer out)
     *
     * @param buffer The raw data to process.
     * @returns std::string containing a JSON representation of the manifest
     */
    static std::string serialToJson(cb::const_char_buffer buffer);

    /**
     * Update from a Collections::Manifest
     *
     * Update compares the current collection set against the manifest and
     * triggers collection creation and collection deletion.
     *
     * Creation and deletion of a collection are pushed into the VBucket and
     * the seqno of updates is recorded in the manifest.
     *
     * @param vb The VBucket to update (queue data into).
     * @param manifest The incoming manifest to compare this object with.
     */
    void update(::VBucket& vb, const Collections::Manifest& manifest);

    /**
     * Add a collection to the manifest.
     *
     * @param vb The vbucket to add the collection to.
     * @param manifestUid the uid of the manifest which made the change
     * @param identifier Identifier of the new collection.
     * @param optionalSeqno Either a seqno to assign to the new collection or
     *        none (none means the checkpoint will assign a seqno).
     */
    void addCollection(::VBucket& vb,
                       uid_t manifestUid,
                       Identifier identifier,
                       OptionalSeqno optionalSeqno);

    /**
     * Begin a delete of the collection.
     *
     * @param vb The vbucket to begin collection deletion on.
     * @param manifestUid the uid of the manifest which made the change
     * @param identifier Identifier of the deleted collection.
     * @param revision manifest revision which started the deletion.
     * @param optionalSeqno Either a seqno to assign to the delete of the
     *        collection or none (none means the checkpoint assigns the seqno).
     */
    void beginCollectionDelete(::VBucket& vb,
                               uid_t manifestUid,
                               Identifier identifier,
                               OptionalSeqno optionalSeqno);

    /**
     * Change the separator.
     *
     * @param vb The vbucket to begin collection deletion on.
     * @param manifestUid the uid of the manifest which made the change
     * @param separator The new separator.
     * @param optionalSeqno Either a seqno to assign to the change event or none
     *        (none means the checkpoint assigns the seqno).
     */
    void changeSeparator(::VBucket& vb,
                         uid_t manifestUid,
                         cb::const_char_buffer separator,
                         OptionalSeqno optionalSeqno);

    /**
     * Complete the deletion of a collection.
     *
     * Lookup the collection name and determine the deletion actions.
     * A collection could of been added again during a background delete so
     * completeDeletion may just update the state or fully drop all knowledge of
     * the collection.
     *
     * @param vb The VBucket in which the deletion is occuring.
     * @param identifier Identifier of the collection that has finished being
     *        deleted.
     */
    void completeDeletion(::VBucket& vb, cb::const_char_buffer collection);

    /**
     * Does the key contain a valid collection?
     *
     * - If the key applies to the default collection, the default collection
     *   must exist.
     *
     * - If the key applies to a collection, the collection must exist and must
     *   not be in the process of deletion.
     */
    bool doesKeyContainValidCollection(const ::DocKey& key) const;


    /**
     * Given a key and it's seqno, the manifest can determine if that key
     * is logically deleted - that is part of a collection which is in the
     * process of being erased.
     *
     * @return true if the key belongs to a deleted collection.
     */
    bool isLogicallyDeleted(const ::DocKey& key, int64_t seqno) const;

    /**
     * Perform the job of isLogicallyDeleted, but with an iterator for the
     * manifest container instead of a key. This means no new key scan and
     * map lookup is performed.
     *
     *  @return true if the seqno/entry represents a logically deleted
     *          collection.
     */
    bool isLogicallyDeleted(const container::const_iterator entry,
                            int64_t seqno) const;

    void incrementDiskCount(const container::const_iterator entry) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second->incrementDiskCount();
        mutated.insert(entry->second.get());
    }

    void decrementDiskCount(const container::const_iterator entry) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second->decrementDiskCount();
        mutated.insert(entry->second.get());
    }

    /**
     * Variant of isLogicallyDeleted where the caller specifies the separator.
     *
     * @return true if the key belongs to a deleted collection.
     */
    bool isLogicallyDeleted(const ::DocKey& key,
                            int64_t seqno,
                            const std::string& separator) const;

    /**
     * Function intended for use by the collection eraser code, checking
     * keys/seqno in seqno order.
     *
     * @param key Collections::DocKey
     * @param bySeqno Seqno assigned to the key
     * @param entry the manifest entry associated with key
     * @return if the key@seqno indicates that we've now hit the real end of a
     *         deleted collection (because the key is a system event) and the
     *         manifest entry determines we should call completeDeletion, then
     *         the return value is initialised with the collection which is to
     *         be deleted. If the key does not indicate the end, the return
     *         value is an empty buffer.
     */
    boost::optional<cb::const_char_buffer> shouldCompleteDeletion(
            const DocKey& key,
            int64_t bySeqno,
            const container::const_iterator entry) const;

    /**
     * @returns the current separator
     */
    std::string getSeparator() const {
        return separator;
    }


    /**
     * @returns true/false if $default exists
     */
    bool doesDefaultCollectionExist() const {
        return defaultCollectionExists;
    }

    /**
     * @returns true if the collection isOpen - false if not (or doesn't exist)
     */
    bool isCollectionOpen(cb::const_char_buffer collection) const {
        auto itr = map.find(collection);
        if (itr != map.end()) {
            return itr->second->isOpen();
        }
        return false;
    }

    /**
     * @returns true if the collection isOpen - false if not (or doesn't exist)
     */
    bool isCollectionOpen(Identifier identifier) const {
        auto itr = map.find(identifier.getName());
        if (itr != map.end()) {
            return itr->second->getUid() == identifier.getUid() &&
                   itr->second->isOpen();
        }
        return false;
    }

    /**
     * @return true if the collection exists in the internal container
     */
    bool exists(cb::const_char_buffer collection) const {
        return map.count(collection) > 0;
    }

    /**
     * @return the number of items stored for collection
     */
    uint64_t getItemCount(cb::const_char_buffer collection) const;

    /**
     * @return const iterator for the collections map
     */
    container::const_iterator begin() const {
        return map.begin();
    }

    /**
     * @return const end iterator for the collections map
     */
    container::const_iterator end() const {
        return map.end();
    }

    /**
     * Get a manifest entry for the collection associated with the key. Can
     * return map.end() for unknown collections.
     */
    container::const_iterator getManifestEntry(
            const DocKey& key, const std::string& separator) const;

    /// @return the manifest UID that last updated this vb::manifest
    uid_t getManifestUid() const {
        return manifestUid;
    }

protected:
    /**
     * Add a collection entry to the manifest specifing the revision that it was
     * seen in and the sequence number for the point in 'time' it was created.
     *
     * @param identifier Identifier of the collection to add.
     * @return a non const reference to the new/updated ManifestEntry so the
     *         caller can set the correct seqno.
     */
    ManifestEntry& addCollectionEntry(Identifier identifier);

    /**
     * Add a collection entry to the manifest specifing the revision that it was
     * seen in and the sequence number span covering it.
     *
     * @param identifier Identifier of the collection to add.
     * @param startSeqno The seqno where the collection begins
     * @param endSeqno The seqno where it ends (can be the special open marker)
     * @return a non const reference to the new ManifestEntry so the caller can
     *         set the correct seqno.
     */
    ManifestEntry& addNewCollectionEntry(Identifier identifier,
                                         int64_t startSeqno,
                                         int64_t endSeqno);

    /**
     * Begin the deletion process by marking the collection entry with the seqno
     * that represents its end.
     *
     * After "begin" delete a collection can be added again or fully deleted
     * by the completeDeletion method.
     *
     * @param identifier Identifier of the collection to delete.
     * @return a reference to the updated ManifestEntry
     */
    ManifestEntry& beginDeleteCollectionEntry(Identifier identifier);

    /**
     * Process a Collections::Manifest to determine if collections need adding
     * or removing.
     *
     * This function returns two sets of collections. Those which are being
     * added and those which are being deleted.
     *
     * @param manifest The Manifest to compare with.
     * @returns A pair of vectors containing the required changes, first
     *          contains collections that need adding whilst second contains
     *          those which should be deleted.
     */
    using processResult =
            std::pair<std::vector<Collections::Manifest::Identifier>,
                      std::vector<Collections::Manifest::Identifier>>;
    processResult processManifest(const Collections::Manifest& manifest) const;

    /**
     * Create a SystemEvent Item, the Item's value will contain data for later
     * consumption by serialToJson
     *
     * @param se SystemEvent to create.
     * @param identifier The Identifier of the collection which is changing.
     * @param deleted If the Item should be marked deleted.
     * @param seqno An optional sequence number. If a seqno is specified, the
     *        returned item will have its bySeqno set to seqno.
     *
     * @returns unique_ptr to a new Item that represents the requested
     *          SystemEvent.
     */
    std::unique_ptr<Item> createSystemEvent(SystemEvent se,
                                            Identifier identifier,
                                            bool deleted,
                                            OptionalSeqno seqno) const;

    /**
     * Create a SystemEvent Item for a Separator Changed. The Item's
     * value will contain data for later consumption by serialToJson.
     *
     * @param highSeqno the current high-seqno which gets mixed into the items
     *        key
     * @param seqno optional-seqno, defined when event is driven by DCP
     * @returns unique_ptr to a new Item that represents the requested
     *          SystemEvent.
     */
    std::unique_ptr<Item> createSeparatorChangedEvent(
            int64_t highSeqno,
            OptionalSeqno seqno) const;

    /**
     * Create an Item that carries a system event and queue it to the vb
     * checkpoint.
     *
     * @param vb The vbucket onto which the Item is queued.
     * @param se The SystemEvent to create and queue.
     * @param identifier The Identifier of the collection being added/deleted.
     * @param deleted If the Item created should be marked as deleted.
     * @param seqno An optional seqno which if set will be assigned to the
     *        system event.
     *
     * @returns The sequence number of the queued Item.
     */
    int64_t queueSystemEvent(::VBucket& vb,
                             SystemEvent se,
                             Identifier identifier,
                             bool deleted,
                             OptionalSeqno seqno) const;

    /**
     * Create an Item that carries a CollectionsSeparatorChanged system event
     * and queue it to the vb checkpoint.
     *
     * @param vb The vbucket onto which the Item is queued.
     * @param seqno An optional seqno which if set will be assigned to the
     *        system event
     */
    int64_t queueSeparatorChanged(::VBucket& vb,
                                  const std::string& oldSeparator,
                                  OptionalSeqno seqno) const;

    /**
     * Obtain how many bytes of storage are needed for a serialised copy
     * of this object including the size of the modified collection.
     *
     * @param collection The name of the collection being changed. It's size is
     *        included in the returned value.
     * @returns how many bytes will be needed to serialise the manifest and
     *          the collection being changed.
     */
    size_t getSerialisedDataSize(cb::const_char_buffer collection) const;

    /**
     * Obtain how many bytes of storage are needed for a serialised copy
     * of this object.
     *
     * @returns how many bytes will be needed to serialise the manifest.
     */
    size_t getSerialisedDataSize() const;

    /**
     * Populate a buffer with the serialised state of the manifest and one
     * additional entry that is the collection being changed, i.e. the addition
     * or deletion.
     *
     * @param out A buffer for the data to be written into.
     * @param revision The Manifest revision we are processing
     * @param identifier The Identifier of the collection being added/deleted
     */
    void populateWithSerialisedData(cb::char_buffer out,
                                    Identifier identifier) const;

    /**
     * Populate a buffer with the serialised state of this object.
     *
     * @param out A buffer for the data to be written into.
     */
    void populateWithSerialisedData(cb::char_buffer out) const;

    /**
     * @returns the string for the given key from the cJSON object.
     */
    const char* getJsonEntry(cJSON* cJson, const char* key);

    /**
     * @returns true if the separator cannot be changed.
     */
    bool cannotChangeSeparator() const;

    /**
     * Update greatestEndSeqno if the seqno is larger
     * @param seqno an endSeqno for a deleted collection
     */
    void trackEndSeqno(int64_t seqno);

    /**
     * Return a string for use in throwException, returns:
     *   "VB::Manifest::<thrower>:<error>, this:<ostream *this>"
     *
     * @param thrower a string for who is throwing, typically __FUNCTION__
     * @param error a string containing the error and useful data
     * @returns string as per description above
     */
    std::string getExceptionString(const std::string& thrower,
                                   const std::string& error) const;

    /**
     * throw exception with the following error string:
     *   "VB::Manifest::<thrower>:<error>, this:<ostream *this>"
     *
     * @param thrower a string for who is throwing, typically __FUNCTION__
     * @param error a string containing the error and useful data
     * @throws exception
     */
    template <class exception>
    [[noreturn]] void throwException(const std::string& thrower,
                                     const std::string& error) const {
        throw exception(getExceptionString(thrower, error));
    }

    /**
     * The current set of collections
     */
    container map;

    /**
     * Does the current set contain the default collection?
     */
    bool defaultCollectionExists;

    /**
     * The collection separator
     */
    std::string separator;

    /**
     * A key below this seqno might be logically deleted and should be checked
     * against the manifest. This member will be set to
     * StoredValue::state_collection_open when no collections are being deleted
     * and the greatestEndSeqno has no use (all collections exclusively open)
     */
    int64_t greatestEndSeqno;

    /**
     * The manifest tracks how many collections are being deleted so we know
     * when greatestEndSeqno can be set to StoredValue::state_collection_open
     */
    cb::NonNegativeCounter<size_t, cb::ThrowExceptionUnderflowPolicy>
            nDeletingCollections;

    /// The manifest UID which updated this vb::manifest
    uid_t manifestUid;

    /// this is VB granularity and the flusher is updating this member
    /// hence only 1 thread is affecting this member so no lock is required
    mutable std::unordered_set<ManifestEntry*> mutated;
    std::unordered_set<ManifestEntry*>::iterator itr;
    bool started = false;

    /**
     * shared lock to allow concurrent readers and safe updates
     */
    mutable cb::RWLock rwlock;

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);
};

/// Note that the VB::Manifest << operator does not obtain the rwlock
/// it is used internally in the object for exception string generation so must
/// not double lock.
std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

/// This is the locked version for printing the manifest
std::ostream& operator<<(std::ostream& os,
                         const Manifest::ReadHandle& readHandle);

} // end namespace VB
} // end namespace Collections
