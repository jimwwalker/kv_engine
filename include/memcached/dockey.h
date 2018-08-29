/*
 *     Copyright 2016 Couchbase, Inc
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

#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>

#include <mcbp/protocol/unsigned_leb128.h>
#include <platform/sized_buffer.h>
#include <platform/socket.h>

class CollectionIDNetworkOrder;

/**
 * DocNamespace "Document Namespace"
 * Meta-data that applies to every document stored in an engine.
 *
 * A document "key" with the flag DefaultCollection is not the same document
 * as "key" with the Collections flag and so on...
 *
 * DefaultCollection: describes "legacy" documents stored in a bucket by
 * clients that do not understand collections.
 *
 * Collections: describes documents that have a collection name as part of
 * the key. E.g. "planet::earth" and "planet::mars" are documents belonging
 * to a "planet" collection.
 *
 * System: describes documents that are created by the system for our own
 * uses. This is only planned for use with the collection feature where
 * special keys are interleaved in the users data stream to represent create
 * and delete events. In future more generic "system documents" maybe
 * created by the core but until such plans are more clear, ep-engine will
 * deny the core from performing operations in the System DocNamespace.
 * DocNamespace values are persisted ot the database and thus are fully
 * described now ready for future use.
 */
using CollectionIDType = uint32_t;
class CollectionID {
public:
    /// To allow KV to move legacy data into a collection, reserve 0
    static constexpr CollectionIDType DefaultCollection = 0;

    /// To allow KV to weave system things into the users namespace, reserve 1
    static constexpr CollectionIDType System = 1;

    CollectionID() : value(DefaultCollection) {
    }

    CollectionID(CollectionIDType value) : value(value) {
    }

    operator uint32_t() const {
        return value;
    }

    bool isDefaultCollection() const {
        return value == DefaultCollection;
    }

    /// Get network byte order of the value
    CollectionIDNetworkOrder to_network() const;

    std::string to_string() const {
        std::stringstream sstream;
        sstream << "0x" << std::hex << value;
        return sstream.str();
    }

private:
    CollectionIDType value;
};

/**
 * NetworkByte order version of CollectionID - limited interface for small areas
 * of code which deal with a network byte order CID
 */
class CollectionIDNetworkOrder {
public:
    CollectionIDNetworkOrder(CollectionID v) : value(htonl(uint32_t(v))) {
    }

    CollectionID to_host() const {
        return CollectionID(ntohl(value));
    }

private:
    CollectionIDType value;
};

static_assert(sizeof(CollectionIDType) == 4,
              "CollectionIDNetworkOrder assumes 4-byte id");

inline CollectionIDNetworkOrder CollectionID::to_network() const {
    return {*this};
}

namespace std {
template <>
struct hash<CollectionID> {
    std::size_t operator()(const CollectionID& k) const {
        return std::hash<uint32_t>()(k);
    }
};

} // namespace std

/// To allow manageable patches during updates to Collections, allow both names
using DocNamespace = CollectionID;

/**
 * A DocKey views a key (non-owning). It can view a key with or without
 * defined collection-ID. Keys with a collection-ID, encode the collection-ID
 * as a unsigned_leb128 prefix in the key bytes. This enum defines if the
 * DocKey is viewing a unsigned_leb128 prefixed key (Yes) or not (No)
 */
enum class DocKeyEncodesCollectionId : uint8_t { Yes, No };

// Constant value of the DefaultCollection(value of 0) leb128 encoded
const uint8_t DefaultCollectionLeb128Encoded = 0;

template <class T>
struct DocKeyInterface {
    size_t size() const {
        return static_cast<const T*>(this)->size();
    }

    const uint8_t* data() const {
        return static_cast<const T*>(this)->data();
    }

    CollectionID getCollectionID() const {
        return static_cast<const T*>(this)->getCollectionID();
    }

    DocNamespace getDocNamespace() const {
        return static_cast<const T*>(this)->getDocNamespace();
    }

    DocKeyEncodesCollectionId getEncoding() const {
        return static_cast<const T*>(this)->getEncoding();
    }

    uint32_t hash() const {
        uint32_t h = 5381;

        if (getEncoding() == DocKeyEncodesCollectionId::No) {
            h = ((h << 5) + h) ^ uint32_t(DefaultCollectionLeb128Encoded);
        }
        // else hash the entire data which includes an encoded CollectionID

        for (auto c : cb::const_byte_buffer(data(), size())) {
            h = ((h << 5) + h) ^ uint32_t(c);
        }

        return h;
    }
};

/**
 * DocKey is a non-owning structure used to describe a document keys over
 * the engine-API. All API commands working with "keys" must specify the
 * data, length and if the data contain an encoded CollectionID
 */
struct DocKey : DocKeyInterface<DocKey> {
    /**
     * Standard constructor - creates a view onto key/nkey
     */
    DocKey(const uint8_t* key, size_t nkey, DocKeyEncodesCollectionId encoding)
        : buffer(key, nkey), encoding(encoding) {
    }

    /**
     * C-string constructor - only for use with null terminated strings and
     * creates a view onto key/strlen(key).
     */
    DocKey(const char* key, DocKeyEncodesCollectionId encoding)
        : DocKey(reinterpret_cast<const uint8_t*>(key),
                 std::strlen(key),
                 encoding) {
    }

    /**
     * const_char_buffer constructor, views the data()/size() of the key
     */
    DocKey(const cb::const_char_buffer& key, DocKeyEncodesCollectionId encoding)
        : DocKey(reinterpret_cast<const uint8_t*>(key.data()),
                 key.size(),
                 encoding) {
    }

    /**
     * Disallow rvalue strings as we would view something which would soon be
     * out of scope.
     */
    DocKey(const std::string&& key,
           DocKeyEncodesCollectionId encoding) = delete;

    const uint8_t* data() const {
        return buffer.data();
    }

    size_t size() const {
        return buffer.size();
    }

    // @todo remove DocNamespace methods
    DocNamespace getDocNamespace() const {
        return getCollectionID();
    }

    CollectionID getCollectionID() const {
        if (encoding == DocKeyEncodesCollectionId::Yes) {
            return cb::mcbp::decode_unsigned_leb128<CollectionIDType>(buffer)
                    .first;
        }
        return CollectionID::DefaultCollection;
    }

    DocKeyEncodesCollectionId getEncoding() const {
        return encoding;
    }

    /**
     * @return the ID and the key as separate entities (the key does not contain
     * the ID). Thus a key which encodes the DefaultCollection can be
     * hashed/compared to the same value as the same logical key which doesn't
     * encode the collection-ID.
     */
    std::pair<CollectionID, cb::const_byte_buffer> getIdAndKey() const {
        if (encoding == DocKeyEncodesCollectionId::Yes) {
            return cb::mcbp::decode_unsigned_leb128<CollectionIDType>(buffer);
        }
        return {CollectionID::DefaultCollection, {data(), size()}};
    }

    /**
     * @return a DocKey that views this DocKey but without any collection-ID
     * prefix. If this was already viewing a key without any encoded
     * collection-ID, then this is returned.
     */
    DocKey makeDocKeyWithoutCollectionID() const {
        if (getEncoding() == DocKeyEncodesCollectionId::Yes) {
            auto decoded =
                    cb::mcbp::skip_unsigned_leb128<CollectionIDType>(buffer);
            return {decoded.data(),
                    decoded.size(),
                    DocKeyEncodesCollectionId::No};
        }
        return *this;
    }

private:
    cb::const_byte_buffer buffer;
    DocKeyEncodesCollectionId encoding{DocKeyEncodesCollectionId::No};
};
