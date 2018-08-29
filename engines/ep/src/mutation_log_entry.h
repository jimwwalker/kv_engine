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

#include "config.h"

#include "storeddockey.h"
#include "utility.h"

#include <type_traits>

enum class MutationLogType : uint8_t {
    New = 0,
    /* removed: ML_DEL = 1 */
    /* removed: ML_DEL_ALL = 2 */
    Commit1 = 3,
    Commit2 = 4,
    NumberOfTypes
};

std::string to_string(MutationLogType t);

class MutationLogEntryV2;
class MutationLogEntryV3;

/**
 * An entry in the MutationLog.
 * This is the V1 layout which pre-dates the addition of document namespaces and
 * is only defined to permit upgrading to V2.
 */
class MutationLogEntryV1 {
public:
    static const uint8_t MagicMarker = 0x45;

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid
     *        MutationLogEntryV1
     * @param buflen the length of said buf
     */
    static const MutationLogEntryV1* newEntry(
            std::vector<uint8_t>::const_iterator itr, size_t buflen) {
        if (buflen < len(0)) {
            throw std::invalid_argument(
                    "MutationLogEntryV1::newEntry: buflen "
                    "(which is " +
                    std::to_string(buflen) +
                    ") is less than minimum required (which is " +
                    std::to_string(len(0)) + ")");
        }

        const auto* me = reinterpret_cast<const MutationLogEntryV1*>(&(*itr));

        if (me->magic != MagicMarker) {
            throw std::invalid_argument(
                    "MutationLogEntryV1::newEntry: "
                    "magic (which is " +
                    std::to_string(me->magic) + ") is not equal to " +
                    std::to_string(MagicMarker));
        }
        if (me->len() > buflen) {
            throw std::invalid_argument(
                    "MutationLogEntryV1::newEntry: "
                    "entry length (which is " +
                    std::to_string(me->len()) +
                    ") is greater than available buflen (which is " +
                    std::to_string(buflen) + ")");
        }
        return me;
    }

    // Statically buffered.  There is no delete.
    void operator delete(void*) = delete;

    /**
     * The size of a MutationLogEntryV1, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // 13 == the exact empty record size as will be packed into
        // the layout
        return 13 + klen;
    }

    /**
     * The number of bytes of the serialized form of this
     * MutationLogEntryV1.
     */
    size_t len() const {
        return len(keylen);
    }

    /**
     * This entry's key.
     */
    const std::string key() const {
        return std::string(_key, keylen);
    }

    uint8_t getKeylen() const {
        return keylen;
    }

    /**
     * This entry's rowid.
     */
    uint64_t rowid() const {
        return ntohll(_rowid);
    }

    /**
     * This entry's vbucket.
     */
    uint16_t vbucket() const {
        return ntohs(_vbucket);
    }

    /**
     * The type of this log entry.
     */
    MutationLogType type() const {
        return _type;
    }

protected:
    friend MutationLogEntryV2;

    MutationLogEntryV1(uint64_t r,
                       MutationLogType t,
                       uint16_t vb,
                       const std::string& k)
        : _rowid(htonll(r)),
          _vbucket(htons(vb)),
          magic(MagicMarker),
          _type(t),
          keylen(static_cast<uint8_t>(k.length())) {
        if (k.length() > std::numeric_limits<uint8_t>::max()) {
            throw std::invalid_argument(
                    "MutationLogEntryV1(): key length "
                    "(which is " +
                    std::to_string(k.length()) + ") is greater than " +
                    std::to_string(std::numeric_limits<uint8_t>::max()));
        }
        memcpy(_key, k.data(), k.length());
    }

    friend std::ostream& operator<<(std::ostream& out,
                                    const MutationLogEntryV1& e);

    const uint64_t _rowid;
    const uint16_t _vbucket;
    const uint8_t magic;
    const MutationLogType _type;
    const uint8_t keylen;
    char _key[1];

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntryV1);
};

/**
 * An entry in the MutationLog.
 * This is the V2 layout which stores document namespaces and removes the rowid
 * (sequence number) as it was unused.
 *
 *  V2 was persisted by spock/vulcan/alice
 *
 */
class MutationLogEntryV2 {
public:
    static const uint8_t MagicMarker = 0x46;

    /**
     * Construct a V2 from V1, this places the key into the default collection
     * No constructor delegation, copy the values raw (so no byte swaps occur)
     * key is initialised into the default collection
     */
    MutationLogEntryV2(const MutationLogEntryV1& mleV1)
        : _vbucket(mleV1._vbucket),
          magic(MagicMarker),
          _type(mleV1._type),
          _key({reinterpret_cast<const uint8_t*>(mleV1._key), mleV1.keylen},
               CollectionID::DefaultCollection) {
        (void)pad;
    }

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid
     *        MutationLogEntryV2
     * @param buflen the length of said buf
     */
    static const MutationLogEntryV2* newEntry(
            std::vector<uint8_t>::const_iterator itr, size_t buflen) {
        if (buflen < len(0)) {
            throw std::invalid_argument(
                    "MutationLogEntryV2::newEntry: buflen "
                    "(which is " +
                    std::to_string(buflen) +
                    ") is less than minimum required (which is " +
                    std::to_string(len(0)) + ")");
        }

        const auto* me = reinterpret_cast<const MutationLogEntryV2*>(&(*itr));

        if (me->magic != MagicMarker) {
            throw std::invalid_argument(
                    "MutationLogEntryV2::newEntry: "
                    "magic (which is " +
                    std::to_string(me->magic) + ") is not equal to " +
                    std::to_string(MagicMarker));
        }
        if (me->len() > buflen) {
            throw std::invalid_argument(
                    "MutationLogEntryV2::newEntry: "
                    "entry length (which is " +
                    std::to_string(me->len()) +
                    ") is greater than available buflen (which is " +
                    std::to_string(buflen) + ")");
        }
        return me;
    }

    // Statically buffered.  There is no delete.
    void operator delete(void*) = delete;

    /**
     * The size of a MutationLogEntryV2, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // the exact empty record size as will be packed into the layout
        return sizeof(MutationLogEntryV2) + (klen - 1);
    }

    /**
     * The number of bytes of the serialized form of this
     * MutationLogEntryV2.
     */
    size_t len() const {
        return len(_key.size());
    }

    /**
     * This entry's key.
     */
    const SerialisedDocKey& key() const {
        return _key;
    }

    /**
     * This entry's vbucket.
     */
    uint16_t vbucket() const {
        return ntohs(_vbucket);
    }

    /**
     * The type of this log entry.
     */
    MutationLogType type() const {
        return _type;
    }

private:
    friend MutationLogEntryV3;

    friend std::ostream& operator<<(std::ostream& out,
                                    const MutationLogEntryV2& e);

    const uint16_t _vbucket;
    const uint8_t magic;
    const MutationLogType _type;
    const uint8_t pad[2] = {}; // padding to ensure _key is the final member
    const SerialisedDocKey _key;

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntryV2);

    static_assert(sizeof(MutationLogType) == sizeof(uint8_t),
                  "_type must be a uint8_t");
};

/**
 * An entry in the MutationLog.
 * This is the V3 layout which stores leb encoded collectionID
 *
 * Stored by mad-hatter
 *
 */
class MutationLogEntryV3 {
public:
    static const uint8_t MagicMarker = 0x47;

    /**
     * Construct a V3 from V2.
     * V2 stored a 1 byte namespace which was the value of 0. we could just
     * treat that as a leb128 encoded DefaultCollection, but for cleanliness
     * skip that first byte and re-encode as the DefaultCollection
     */
    MutationLogEntryV3(const MutationLogEntryV2& mleV2)
        : _vbucket(mleV2._vbucket),
          magic(MagicMarker),
          _type(mleV2._type),
          _key({mleV2._key.data() + 1, mleV2._key.size() - 1},
               CollectionID::DefaultCollection) {
        (void)pad;
    }

    /**
     * Initialize a new entry inside the given buffer.
     *
     * @param r the rowid
     * @param t the type of log entry
     * @param vb the vbucket
     * @param k the key
     */
    static MutationLogEntryV3* newEntry(uint8_t* buf,
                                        MutationLogType t,
                                        uint16_t vb,
                                        const DocKey& k) {
        return new (buf) MutationLogEntryV3(t, vb, k);
    }

    static MutationLogEntryV3* newEntry(uint8_t* buf,
                                        MutationLogType t,
                                        uint16_t vb) {
        if (MutationLogType::Commit1 != t && MutationLogType::Commit2 != t) {
            throw std::invalid_argument(
                    "MutationLogEntryV3::newEntry: invalid type");
        }
        return new (buf) MutationLogEntryV3(t, vb);
    }

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid
     *        MutationLogEntryV3
     * @param buflen the length of said buf
     */
    static const MutationLogEntryV3* newEntry(
            std::vector<uint8_t>::const_iterator itr, size_t buflen) {
        if (buflen < len(0)) {
            throw std::invalid_argument(
                    "MutationLogEntryV3::newEntry: buflen "
                    "(which is " +
                    std::to_string(buflen) +
                    ") is less than minimum required (which is " +
                    std::to_string(len(0)) + ")");
        }

        const auto* me = reinterpret_cast<const MutationLogEntryV3*>(&(*itr));

        if (me->magic != MagicMarker) {
            throw std::invalid_argument(
                    "MutationLogEntryV3::newEntry: "
                    "magic (which is " +
                    std::to_string(me->magic) + ") is not equal to " +
                    std::to_string(MagicMarker));
        }
        if (me->len() > buflen) {
            throw std::invalid_argument(
                    "MutationLogEntryV3::newEntry: "
                    "entry length (which is " +
                    std::to_string(me->len()) +
                    ") is greater than available buflen (which is " +
                    std::to_string(buflen) + ")");
        }
        return me;
    }

    // Statically buffered.  There is no delete.
    void operator delete(void*) = delete;

    /**
     * The size of a MutationLogEntryV3, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // the exact empty record size as will be packed into the layout
        return sizeof(MutationLogEntryV3) + (klen - 1);
    }

    /**
     * The number of bytes of the serialized form of this
     * MutationLogEntryV3.
     */
    size_t len() const {
        return len(_key.size());
    }

    /**
     * This entry's key.
     */
    const SerialisedDocKey& key() const {
        return _key;
    }

    /**
     * This entry's vbucket.
     */
    uint16_t vbucket() const {
        return ntohs(_vbucket);
    }

    /**
     * The type of this log entry.
     */
    MutationLogType type() const {
        return _type;
    }

private:
    friend std::ostream& operator<<(std::ostream& out,
                                    const MutationLogEntryV3& e);

    MutationLogEntryV3(MutationLogType t, uint16_t vb, const DocKey& k)
        : _vbucket(htons(vb)), magic(MagicMarker), _type(t), _key(k) {
        // Assert that _key is the final member
        static_assert(
                offsetof(MutationLogEntryV3, _key) ==
                        (sizeof(MutationLogEntryV3) - sizeof(SerialisedDocKey)),
                "_key must be the final member of MutationLogEntryV2");
    }

    MutationLogEntryV3(MutationLogType t, uint16_t vb)
        : MutationLogEntryV3(
                  t, vb, {nullptr, 0, DocKeyEncodesCollectionId::No}) {
    }

    const uint16_t _vbucket;
    const uint8_t magic;
    const MutationLogType _type;
    const uint8_t pad[2] = {}; // padding to ensure _key is the final member
    const SerialisedDocKey _key;

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntryV3);

    static_assert(sizeof(MutationLogType) == sizeof(uint8_t),
                  "_type must be a uint8_t");
};

using MutationLogEntry = MutationLogEntryV3;

std::ostream& operator<<(std::ostream& out, const MutationLogEntryV1& mle);
std::ostream& operator<<(std::ostream& out, const MutationLogEntryV2& mle);
std::ostream& operator<<(std::ostream& out, const MutationLogEntryV3& mle);
