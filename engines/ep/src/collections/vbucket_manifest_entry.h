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

#include <platform/non_negative_counter.h>
#include <platform/sized_buffer.h>

#include <memory>

namespace Collections {
namespace VB {

/**
 * The Collections::VB::ManifestEntry stores the data a collection
 * needs from a vbucket's perspective.
 * - The CollectionID
 * - The seqno lifespace of the collection
 */
class ManifestEntry {
public:
    ManifestEntry(int64_t _startSeqno, int64_t _endSeqno) {
        // Setters validate the start/end range is valid
        setStartSeqno(_startSeqno);
        setEndSeqno(_endSeqno);
    }

    ManifestEntry(int64_t _startSeqno, int64_t _endSeqno, int32_t flushCount) : ManifestEntry(_startSeqno, _endSeqno) : flushCount(flushCount) {
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
     * Inform the collection that all items of the collection up to endSeqno
     * have been deleted.
     *
     * @return the correct SystemEvent for vbucket manifest management. If the
     *         collection has been reopened, a soft delete, else hard.
     */
    SystemEvent completeDeletion();

    /// increment how many items are stored on disk for this collection
    void incrementDiskCount() const {
        diskCount++;
    }

    /// decrement how many items are stored on disk for this collection
    void decrementDiskCount() const {
        diskCount--;
    }

    /// @return how many items are stored on disk for this collection
    uint64_t getDiskCount() const {
        return diskCount;
    }

    bool isLogicallyDeleted(int64_t seqno) const {
        return seqno < startSeqno || seqno <= endSeqno;
    }

    void setFlushCount(int32_t flushCount) const {
        flushing = flushCount;
    }

    int32_t getFlushCount() const {
        return flushing;
    }

static constexpr int32_t NoFlush = 1;
    void completedFlushing() const {
        flushing = NoFlush;
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
     * Collection life-time is recorded as the seqno the collection was added
     * to the seqno of the point we started to delete it.
     *
     * If a collection is not being deleted then endSeqno has a value of
     * StoredValue::state_collection_open to indicate this.
     */
    int64_t startSeqno{-1};
    int64_t endSeqno{-1};

    /**
     * A flushing collection can be flushed again, which is indicated from
     * the server by just incrementing the flush-count. The current flushed
     * count must be maintained (and persisted) so that we can start new flushes
     * and resume unfinished ones.
     */
    int32_t flushing{-1};

    /**
     * The count of items in this collection
     * mutable - the VB:Manifest read/write lock protects this object and
     *           we can do stats updates as long as the read lock is held.
     *           The write lock is really for the Manifest map being changed.
     */
    mutable cb::NonNegativeCounter<uint64_t, cb::ThrowExceptionUnderflowPolicy>
            diskCount{0};
};

std::ostream& operator<<(std::ostream& os, const ManifestEntry& manifestEntry);

} // end namespace VB
} // end namespace Collections