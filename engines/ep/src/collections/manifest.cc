/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "collections/manifest.h"
#include "collections/collections_types.h"

#include <JSON_checker.h>
#include <nlohmann/json.hpp>
#include <gsl/gsl>

#include <cstring>
#include <iostream>
#include <sstream>

namespace Collections {

Manifest::Manifest(cb::const_char_buffer json, size_t maxNumberOfCollections)
    : defaultCollectionExists(false), uid(0) {
    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(json.data()),
                       json.size())) {
        throw std::invalid_argument("Manifest::Manifest input not valid json:" +
                                    cb::to_string(json));
    }
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(json);
    } catch (const nlohmann::json::exception& e) {
        throw std::invalid_argument(
                "Manifest::Manifest cJSON cannot parse json:" +
                cb::to_string(json) + ", e:" + e.what());
    }

    // Read the Manifest UID e.g. "uid" : "5fa1"
    auto jsonUid = getJsonObject(parsed, CollectionUidKey, CollectionUidType);
    uid = makeUid(jsonUid.get<std::string>());

    auto collections = getJsonObject(parsed, CollectionsKey, CollectionsType);

    if (collections.size() > maxNumberOfCollections) {
        throw std::invalid_argument(
                "Manifest::Manifest too many collections count:" +
                std::to_string(collections.size()));
    }

    for (const auto& collection : collections) {
        throwIfWrongType(std::string(CollectionsKey),
                         collection,
                         nlohmann::json::value_t::object);

        auto name = getJsonObject(
                collection, CollectionNameKey, CollectionNameType);
        auto uid =
                getJsonObject(collection, CollectionUidKey, CollectionUidType);

        CollectionID uidValue = makeCollectionID(uid.get<std::string>());
        auto nameValue = name.get<std::string>();
        if (validCollection(nameValue) && validUid(uidValue)) {
            if (this->collections.count(uidValue) > 0) {
                throw std::invalid_argument(
                        "Manifest::Manifest duplicate cid:" +
                        uidValue.to_string() + ", name:" + nameValue);
            }

            enableDefaultCollection(uidValue);
            this->collections.emplace(uidValue, nameValue);
        } else {
            throw std::invalid_argument(
                    "Manifest::Manifest invalid collection entry:" + nameValue +
                    " cid:" + uidValue.to_string());
        }
    }
}

nlohmann::json Manifest::getJsonObject(const nlohmann::json& object,
                                       const std::string& key,
                                       nlohmann::json::value_t expectedType) {
    try {
        auto rv = object.at(key);
        throwIfWrongType(key, rv, expectedType);
        return rv;
    } catch (const nlohmann::json::exception& e) {
        throw std::invalid_argument("Manifest: cannot find key:" + key +
                                    ", e:" + e.what());
    }
}

void Manifest::throwIfWrongType(const std::string& errorKey,
                                const nlohmann::json& object,
                                nlohmann::json::value_t expectedType) {
    if (object.type() != expectedType) {
        throw std::invalid_argument("Manifest: wrong type for key:" + errorKey +
                                    ", " + object.dump());
    }
}

void Manifest::enableDefaultCollection(CollectionID identifier) {
    if (identifier == CollectionID::DefaultCollection) {
        defaultCollectionExists = true;
    }
}

bool Manifest::validCollection(const std::string& collection) {
    // Current validation is to just check the prefix to ensure
    // 1. $default is the only $ prefixed collection.
    // 2. _ is not allowed as the first character.
    if (collection[0] == '$' &&
        (collection.compare(DefaultCollectionIdentifier.data()) != 0)) {
        return false;
    }
    return collection[0] != '_';
}

bool Manifest::validUid(CollectionID identifier) {
    // We reserve system
    return identifier != CollectionID::System;
}

std::string Manifest::toJson() const {
    std::stringstream json;
    json << R"({"uid":")" << std::hex << uid << R"(","collections":[)";
    int ii = 0;
    for (const auto& collection : collections) {
        json << R"({"name":")" << collection.second << R"(","uid":")"
             << std::hex << collection.first << R"("})";
        if (ii != collections.size() - 1) {
            json << ",";
        }
        ii++;
    }
    json << "]}";
    return json.str();
}

void Manifest::dump() const {
    std::cerr << *this << std::endl;
}

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "Collections::Manifest"
       << ", defaultCollectionExists:" << manifest.defaultCollectionExists
       << ", collections.size:" << manifest.collections.size() << std::endl;
    for (const auto& entry : manifest.collections) {
        os << "collection:{" << std::hex << entry.first << "," << entry.second
           << "}\n";
    }
    return os;
}
}
