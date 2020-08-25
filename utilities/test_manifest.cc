/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "test_manifest.h"

#include <memcached/dockey.h>

#include <nlohmann/json.hpp>
#include <iomanip>
#include <iostream>
#include <sstream>

CollectionsManifest::CollectionsManifest() {
    add(ScopeEntry::defaultS);
    add(CollectionEntry::defaultC);
}

CollectionsManifest::CollectionsManifest(NoDefault) {
    add(ScopeEntry::defaultS);
}

CollectionsManifest::CollectionsManifest(const CollectionEntry::Entry& entry)
    : CollectionsManifest() {
    add(entry);
}

CollectionsManifest& CollectionsManifest::add(const ScopeEntry::Entry& entry) {
    updateUid();

    nlohmann::json jsonEntry;
    std::stringstream ss;
    ss << std::hex << entry.uid;

    jsonEntry["name"] = entry.name;
    jsonEntry["uid"] = ss.str();
    jsonEntry["collections"] = std::vector<nlohmann::json>();

    json["scopes"].push_back(jsonEntry);

    return *this;
}

CollectionsManifest& CollectionsManifest::add(
        const CollectionEntry::Entry& collectionEntry,
        cb::ExpiryLimit maxTtl,
        const ScopeEntry::Entry& scopeEntry) {
    updateUid();

    nlohmann::json jsonEntry;
    std::stringstream ss;
    ss << std::hex << collectionEntry.uid;

    jsonEntry["name"] = collectionEntry.name;
    jsonEntry["uid"] = ss.str();

    if (maxTtl) {
        jsonEntry["maxTTL"] = maxTtl.value().count();
    }

    // Add the new collection to the set of collections belonging to the
    // given scope
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == scopeEntry.name) {
            (*itr)["collections"].push_back(jsonEntry);
            break;
        }
    }

    return *this;
}

CollectionsManifest& CollectionsManifest::add(
        const CollectionEntry::Entry& collectionEntry,
        const ScopeEntry::Entry& scopeEntry) {
    return add(collectionEntry, {/*no ttl*/}, scopeEntry);
}

CollectionsManifest& CollectionsManifest::remove(
        const ScopeEntry::Entry& entry) {
    updateUid();
    std::stringstream sid;
    sid << std::hex << entry.uid;
    bool removed = false;
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == entry.name && (*itr)["uid"] == sid.str()) {
            json["scopes"].erase(itr);
            removed = true;
            break;
        }
    }

    if (!removed) {
        throw std::invalid_argument(
                "CollectionsManifest::remove(scope) did not remove anything");
    }

    return *this;
}

CollectionsManifest& CollectionsManifest::remove(
        const CollectionEntry::Entry& collectionEntry,
        const ScopeEntry::Entry& scopeEntry) {
    updateUid();

    std::stringstream cid;
    cid << std::hex << collectionEntry.uid;

    bool removed = false;
    // Iterate on all scopes, find the one matching the passed scopeEntry
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == scopeEntry.name) {
            // Iterate on all collections within the scope, find the one
            // matching the passed collectionEntry
            for (auto citr = (*itr)["collections"].begin();
                 citr != (*itr)["collections"].end();
                 citr++) {
                if ((*citr)["name"] == collectionEntry.name &&
                    (*citr)["uid"] == cid.str()) {
                    (*itr)["collections"].erase(citr);
                    removed = true;
                    break;
                }
            }
            break;
        }
    }

    if (!removed) {
        throw std::invalid_argument(
                "CollectionsManifest::remove(collection) did not remove "
                "anything");
    }

    return *this;
}

bool CollectionsManifest::exists(const CollectionEntry::Entry& collectionEntry,
                                 const ScopeEntry::Entry& scopeEntry) const {
    std::stringstream cid;
    cid << std::hex << collectionEntry.uid;
    std::stringstream sid;
    sid << std::hex << scopeEntry.uid;

    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == scopeEntry.name && (*itr)["uid"] == sid.str()) {
            for (auto citr = (*itr)["collections"].begin();
                 citr != (*itr)["collections"].end();
                 citr++) {
                if ((*citr)["name"] == collectionEntry.name &&
                    (*citr)["uid"] == cid.str()) {
                    return true;
                }
            }
            break;
        }
    }
    return false;
}

bool CollectionsManifest::exists(const ScopeEntry::Entry& scopeEntry) const {
    std::stringstream sid;
    sid << std::hex << scopeEntry.uid;
    for (auto itr = json["scopes"].begin(); itr != json["scopes"].end();
         itr++) {
        if ((*itr)["name"] == scopeEntry.name && (*itr)["uid"] == sid.str()) {
            return true;
        }
    }
    return false;
}

void CollectionsManifest::updateUid() {
    uid++;

    std::stringstream ss;
    ss << std::hex << uid;
    json["uid"] = ss.str();
}

void CollectionsManifest::updateUid(uint64_t uid) {
    this->uid = uid;

    std::stringstream ss;
    ss << std::hex << uid;
    json["uid"] = ss.str();
}

std::string CollectionsManifest::toJson() const {
    return json.dump();
}

void CollectionsManifest::setUid(const std::string& uid) {
    this->uid = strtoull(uid.c_str(), nullptr, 16);
    updateUid();
}


