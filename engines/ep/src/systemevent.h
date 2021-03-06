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

#include "atomic.h"
#include "ep_types.h"

#include <string>

class Item;
class KVStore;
class SystemEventMessage;

/// underlying size of uint32_t as this is to be stored in the Item flags field.
enum class SystemEvent : uint32_t {
    /**
     * The CreateCollection system event is generated when a VBucket receives
     * knowledge of a new collection. The event's purpose is to carry data
     * to the flusher so we can persist a new collections JSON manifest that
     * includes the new collection and also to persist a special marker document
     * allowing DCP backfills to re-transmit collection creation at the correct
     * point in "seqno-time". This event will also be used to generate
     * DCP messages to inform consumers of the new collection (for in-memory
     * streaming).
     */
    CreateCollection,

    /**
     * The BeginDeleteCollection system event is generated when a VBucket
     * receives a manifest that removes a collection. The event's purpose is to
     * carry data to the flusher so we can persist a new collections JSON
     * manifest that indicates the collection is now in the process of being
     * removed. This is indicated by changing the end-seqno of a collection's
     * entry. BeginDeleteCollection also informs DCP clients that the collection
     * is now deleted (no more data can be written to it).
     */
    BeginDeleteCollection,

    /**
     * The DeleteCollectionHard system event is generated when a VBucket has
     * completed the deletion of all items of a collection. The hard delete
     * carries data to the flusher so we can persist a JSON manifest that now
     * fully removes the collection and also deleted the special marker document
     * created by CreateCollection.
     */
    DeleteCollectionHard,

    /**
     * The DeleteCollectionSoft system event is generated when a VBucket has
     * completed the deletion of all items of a collection *but*  a
     * collection of the same name was added back during the deletion. The soft
     * delete carries data to the flusher so we can persist a JSON manifest that
     * only updates the end-seqno of the deleted collection entry. The soft
     * delete also deleted the special marker document created by
     * CreateCollection.
     */
    DeleteCollectionSoft,

    /**
     * The CollectionsSeparatorChanged system event is generated when a VBucket
     * changes the separator used for identifying collections in keys. This
     * must result in a vbucket manifest update and a SystemEvent document is
     * stored. All separator changes write to the same SystemEvent document.
     */
    CollectionsSeparatorChanged
};

static inline std::string to_string(const SystemEvent se) {
    switch (se) {
    case SystemEvent::CreateCollection:
        return "CreateCollection";
    case SystemEvent::BeginDeleteCollection:
        return "BeginDeleteCollection";
    case SystemEvent::DeleteCollectionHard:
        return "DeleteCollectionHard";
    case SystemEvent::DeleteCollectionSoft:
        return "DeleteCollectionSoft";
    case SystemEvent::CollectionsSeparatorChanged:
        return "CollectionSeparatorChanged";
    }
    throw std::invalid_argument("to_string(SystemEvent) unknown " +
                                std::to_string(int(se)));
}

class SystemEventFactory {
public:
    /**
     * Make an Item representing the SystemEvent
     * @param se The SystemEvent being created. The returned Item will have this
     *           value stored in the flags field.
     * @param keyExtra Every SystemEvent has defined key, keyExtra is appended
     *        to the defined key
     * @param itemSize The returned Item can be requested to allocate a value
     *        of itemSize. Some SystemEvents will update the value with data to
     *        be persisted/replicated.
     * @param seqno An OptionalSeqno - if defined the returned Item will have
     *        the seqno value set as its bySeqno.
     */
    static std::unique_ptr<Item> make(SystemEvent se,
                                      const std::string& collectionsSeparator,
                                      const std::string& keyExtra,
                                      size_t itemSize,
                                      OptionalSeqno seqno);

    /**
     * Map the internal ep-engine SystemEvent IDs to mcbp values.
     * Internally there are more events than the outside world will know about.
     */
    static mcbp::systemevent::id mapToMcbp(SystemEvent se);

private:
    static std::string makeKey(SystemEvent se,
                               const std::string& collectionsSeparator,
                               const std::string& keyExtra);
};

enum class ProcessStatus { Skip, Continue };

/**
 * SystemEventFlush holds all SystemEvent data for a single invocation of a
 * vbucket's flush
 * If the flush encountered no SystemEvents then this class does nothing
 * If the flush has SystemEvents then this class will ensure the correct
 * actions occur.
 */
class SystemEventFlush {
public:
    /**
     * Get the Item which is updating the collections manifest (if any)
     *
     * @return nullptr if no manifest exists or the Item to be used in writing
     *         a manifest.
     */
    const Item* getCollectionsManifestItem() const;

    /**
     * The flusher passes each item into this function and process determines
     * what needs to happen (possibly updating the Item).
     *
     * This function /may/ take a reference to the ref-counted Item if the Item
     * is required for a collections manifest update.
     *
     * Warning: Even though the input is a const queued_item, the Item* is not
     * const. This function may call setOperation on the shared item
     *
     * @param item an item from the flushers items to flush.
     * @returns Skip if the flusher should not continue with the item or
     *          Continue if the flusher can continue the rest of the flushing
     *          function against the item.
     */
    ProcessStatus process(const queued_item& item);

    /**
     * Determine the flushing action of the Item, knows about normal set/del
     * and how to flush SystemEvent Items
     *
     * @param item An Item to determine if it should result in an upsert.
     * @returns true if the Item is an upsert (add or update) of the Item
     */
    static bool isUpsert(const Item& item);

private:
    /**
     * Save the item as the item which contains the manifest which will be
     * used in the flush's update of the vbucket's metadata documents.
     * The function will only set the them if it has a seqno higher than any
     * previously saved item.
     */
    void saveCollectionsManifestItem(const queued_item& item);

    /**
     * Shared pointer to an Item which holds collections manifest data that
     * maybe needed by the flush::commit
     */
    queued_item collectionManifestItem;
};

class SystemEventReplicate {
public:
    static ProcessStatus process(const Item& item);
};
