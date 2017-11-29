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

#include "collections/collections_types.h"
#include "stored-value.h"
#include "systemevent.h"

#include <platform/make_unique.h>
#include <platform/non_negative_counter.h>
#include <platform/sized_buffer.h>

#include <memory>

namespace Collections {
namespace VB {

/**
 * The Collections::VB::ManifestEntry stores the data a collection
 * needs from a vbucket's perspective.
 * - The Collections::Manifest revision
 * - The seqno lifespace of the collection
 *
 * Additionally this object is designed for use by Collections::VB::Manifest,
 * this is why the object stores a pointer to a std::string collection name,
 * rather than including the entire object std::string.
 * Thus a std::map can map from cb::const_char_buffer, ManifestEntry, where
 * the buffer refers to the key we own via the pointer.
 */
class ManifestEntry {
public:
    ManifestEntry(Identifier identifier, int64_t _startSeqno, int64_t _endSeqno)
        : collectionName(std::make_unique<std::string>(
                  identifier.getName().data(), identifier.getName().size())),
          uid(identifier.getUid()),
          startSeqno(-1),
          endSeqno(-1),
          count(0) {
        // Setters validate the start/end range is valid
        setStartSeqno(_startSeqno);
        setEndSeqno(_endSeqno);
    }

    ManifestEntry(const ManifestEntry& rhs)
        : collectionName(
                  std::make_unique<std::string>(rhs.collectionName->c_str())),
          uid(rhs.uid),
          startSeqno(rhs.startSeqno),
          endSeqno(rhs.endSeqno) {
    }

    ManifestEntry(ManifestEntry&& rhs)
        : collectionName(std::move(rhs.collectionName)),
          uid(rhs.uid),
          startSeqno(rhs.startSeqno),
          endSeqno(rhs.endSeqno) {
    }

    ManifestEntry& operator=(ManifestEntry&& rhs) {
        std::swap(collectionName, rhs.collectionName);
        uid = rhs.uid;
        startSeqno = rhs.startSeqno;
        endSeqno = rhs.endSeqno;
        return *this;
    }

    ManifestEntry& operator=(const ManifestEntry& rhs) {
        collectionName.reset(new std::string(rhs.collectionName->c_str()));
        uid = rhs.uid;
        startSeqno = rhs.startSeqno;
        endSeqno = rhs.endSeqno;
        return *this;
    }

    const std::string& getCollectionName() const {
        return *collectionName;
    }

    /**
     * @return const_char_buffer initialised with address/size of the internal
     *         collection-name string data.
     */
    cb::const_char_buffer getCharBuffer() const {
        return cb::const_char_buffer(collectionName->data(),
                                     collectionName->size());
    }

    Identifier getIdentifier() const {
        return {getCharBuffer(), getUid()};
    }

    int64_t getStartSeqno() const {
        return startSeqno;
    }

    void setStartSeqno(int64_t seqno) {
        // Enforcing that start/end are not the same, they should always be
        // separated because they represent start/end mutations.
        if (seqno < 0 || seqno <= startSeqno || seqno == endSeqno) {
            throwException<std::invalid_argument>(
                    __FUNCTION__,
                    "cannot set startSeqno to " + std::to_string(seqno));
        }
        startSeqno = seqno;
    }

    int64_t getEndSeqno() const {
        return endSeqno;
    }

    void setEndSeqno(int64_t seqno) {
        // Enforcing that start/end are not the same, they should always be
        // separated because they represent start/end mutations.
        if (seqno != StoredValue::state_collection_open &&
            (seqno <= endSeqno || seqno == startSeqno)) {
            throwException<std::invalid_argument>(
                    __FUNCTION__,
                    "cannot set "
                    "endSeqno to " +
                            std::to_string(seqno));
        }
        endSeqno = seqno;
    }

    void resetEndSeqno() {
        endSeqno = StoredValue::state_collection_open;
    }

    uid_t getUid() const {
        return uid;
    }

    void setUid(uid_t uid) {
        this->uid = uid;
    }

    /**
     * A collection is open when the start is greater than the end.
     * An open collection is one that is readable and writable by the data
     * path.
     */
    bool isOpen() const {
        return startSeqno > endSeqno;
    }

    /**
     * A collection is being deleted when the endSeqno is not the special
     * state_collection_open value.
     */
    bool isDeleting() const {
        return endSeqno != StoredValue::state_collection_open;
    }

    /**
     * A collection can be open whilst at the same time a previous incarnation
     * of it is being deleted.
     * isOpenAndDeleted returns true for exactly this case
     */
    bool isOpenAndDeleting() const {
        return isOpen() && isDeleting();
    }

    /**
     * A collection can be open whilst at the same time a previous incarnation
     * of it is being deleted.
     * Exclusively Open is true only when there is no deletion in progress.
     */
    bool isExclusiveOpen() const {
        return isOpen() && !isDeleting();
    }

    /**
     * A collection can be open whilst at the same time a previous incarnation
     * of it is being deleted.
     * Exclusively Deleting means that there was no re-addition of the
     * collection, it is only deleting.
     */
    bool isExclusiveDeleting() const {
        return !isOpen() && isDeleting();
    }

    /**
     * To determine if the eraser process should trigger completeDeletion we
     * need to compare the seqno of SystemEvents read from disk with the
     * end-seqno/start-seqno of the collection.
     * For example if the eraser finds a fruit collection event system key
     * (delete or create) and it is a match for start or end, then that means
     * the eraser has finished processing the seqno span in which logically
     * deleted keys lived and it's time to call completeDeletion
     *
     * @return  endSeqno == bySeqno || startSeqno == bySeqno
     */
    bool shouldCompleteDeletion(int64_t bySeqno) const {
        return endSeqno == bySeqno || startSeqno == bySeqno;
    }

    /**
     * Inform the collection that all items of the collection up to endSeqno
     * have been deleted.
     *
     * @return the correct SystemEvent for vbucket manifest management. If the
     *         collection has been reopened, a soft delete, else hard.
     */
    SystemEvent completeDeletion();

    void incrementItemCount() const {
        count++;
    }

    void decrementItemCount() const {
        count--;
    }

    uint64_t getItemCount() const {
        return count;
    }

private:
    /**
     * Return a string for use in throwException, returns:
     *   "VB::ManifestEntry::<thrower>:<error>, this:<ostream *this>"
     *
     * @param thrower a string for who is throwing, typically __FUNCTION__
     * @param error a string containing the error and useful data
     * @returns string as per description above
     */
    std::string getExceptionString(const std::string& thrower,
                                   const std::string& error) const;

    /**
     * throw exception with the following error string:
     *   "VB::ManifestEntry::<thrower>:<error>, this:<ostream *this>"
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
     * An entry has a name that is heap allocated... this is due to the
     * way ManifestEntry objects are stored in the VB::Manifest
     */
    std::unique_ptr<std::string> collectionName;

    /**
     * The uid of the collection
     */
    uid_t uid;

    /**
     * Collection life-time is recorded as the seqno the collection was added
     * to the seqno of the point we started to delete it.
     *
     * If a collection is not being deleted then endSeqno has a value of
     * StoredValue::state_collection_open to indicate this.
     */
    int64_t startSeqno;
    int64_t endSeqno;

    /// mutable as we can update this with Read (const) or Write access
    mutable cb::NonNegativeCounter<uint64_t, cb::ThrowExceptionUnderflowPolicy>
            count;
};

std::ostream& operator<<(std::ostream& os, const ManifestEntry& manifestEntry);

} // end namespace VB
} // end namespace Collections