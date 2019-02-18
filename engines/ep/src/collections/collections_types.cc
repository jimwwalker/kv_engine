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

#include "collections/collections_types.h"
#include "collections/vbucket_serialised_manifest_entry_generated.h"
#include "systemevent.h"

#include <mcbp/protocol/unsigned_leb128.h>

#include <cctype>
#include <cstring>
#include <iostream>
#include <sstream>

namespace Collections {

ManifestUid makeUid(const char* uid, size_t len) {
    if (std::strlen(uid) == 0 || std::strlen(uid) > len) {
        throw std::invalid_argument(
                "Collections::makeUid uid must be > 0 and <=" +
                std::to_string(len) +
                " characters: "
                "strlen(uid):" +
                std::to_string(std::strlen(uid)));
    }

    // verify that the input characters satisfy isxdigit
    for (size_t ii = 0; ii < std::strlen(uid); ii++) {
        if (uid[ii] == 0) {
            break;
        } else if (!std::isxdigit(uid[ii])) {
            throw std::invalid_argument("Collections::makeUid: uid:" +
                                        std::string(uid) + ", index:" +
                                        std::to_string(ii) + " fails isxdigit");
        }
    }

    return std::strtoul(uid, nullptr, 16);
}

// Just return the manifest-ID as it is encoded in the JSON manifest
// base-16 with no 0x prefix.
std::string getUnknownCollectionErrorContext(uint64_t manifestUid) {
    std::stringstream ss;
    ss << std::hex << manifestUid;
    return ss.str();
}

std::string makeCollectionIdIntoString(CollectionID collection) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(collection);
    return std::string(reinterpret_cast<const char*>(leb128.data()),
                       leb128.size());
}

std::string makeScopeIdIntoString(ScopeID sid) {
    cb::mcbp::unsigned_leb128<ScopeIDType> leb128(sid);
    return std::string(reinterpret_cast<const char*>(leb128.data()),
                       leb128.size());
}

CollectionID getCollectionIDFromKey(const DocKey& key, const char* separator) {
    if (!key.getCollectionID().isSystem()) {
        throw std::invalid_argument("getCollectionIDFromKey: non-system key");
    }
    return cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
                   SystemEventFactory::getKeyExtra(key, separator))
            .first;
}

ScopeID getScopeIDFromKey(const DocKey& key, const char* separator) {
    if (!key.getCollectionID().isSystem()) {
        throw std::invalid_argument("getCollectionIDFromKey: non-system key");
    }
    return cb::mcbp::decode_unsigned_leb128<ScopeIDType>(
                   SystemEventFactory::getKeyExtra(key, separator))
            .first;
}

} // end namespace Collections
