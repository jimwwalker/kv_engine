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

#include "collections/manifest.h"

#include <boost/optional/optional.hpp>
#include <memcached/dockey.h>
#include <platform/sized_buffer.h>

#include <string>
#include <unordered_map>

namespace Collections {

/**
 * Collections::Filter stores the JSON filter which DCP_OPEN_PRODUCER can
 * specify.
 *
 * Two flavours of JSON input are valid:
 * Stream from zero (you don't know the UID)
 * {"collections" : ["name1", "name2", ...]}
 *
 * Stream from non-zero seqno (you need the correct uid)
 * {"collections" : [{"name":"name1", "uid":"xxx"},
 *                   {"name":"name2", "uid":"yyy"}, ...]}
 *
 * Note Collections::Filter input is optional in that the client can omit a
 * filter in which case the filter is a pass-through.
 *
 * This object is used to create Collections::VB::Filter objects when VB streams
 * are requested. The Collections::VB::Filter object is used to make the final
 * decision if data should be streamed or dropped.
 */
class Filter {
public:

    /// a name with an optional UID
    using container =
            std::vector<std::pair<std::string, boost::optional<uid_t>>>;
    /**
     * Construct a Collections::Filter using an optional JSON document
     * and the bucket's current Manifest.
     *
     * The optional JSON document allows a client to filter a chosen set of
     * collections or just the default collection.
     *
     * if jsonFilter is defined and empty - then we create a passthrough.
     * if jsonFilter is defined and !empty - then we filter as requested.
     * if jsonFilter is not defined (maybe a legacy client who doesn't
     *   understand collections) then only documents with
     *   DocNamespace::DefaultCollection are allowed.
     *
     * @throws invalid_argument if the JSON is invalid or contains unknown
     *         collections.
     */
    Filter(boost::optional<const std::string&> jsonFilter,
           const Manifest& manifest);

    /**
     * Get the list of collections the filter knows about. Can be empty
     * @returns std::vector of std::string, maybe empty for a passthrough filter
     */
    const container& getFilter() const {
        return filter;
    }

    /**
     * @returns if the filter configured so that it allows everything through?
     */
    bool isPassthrough() const {
        return passthrough;
    }

    /**
     * @returns if the filter contains the default collection
     */
    bool allowDefaultCollection() const {
        return defaultAllowed;
    }

    /**
     * @returns if the filter should allow system events
     */
    bool allowSystemEvents() const {
        return systemEventsAllowed;
    }

    /**
     * @return true if the Filter uses name/uid pairs
     */
    bool isUidFilter() const {
        return uidFound;
    }

    /**
     * @return true if the Filter uses name only identification
     */
    bool isNameFilter() const {
        return nameFound;
    }

    /**
     * Dump this to std::cerr
     */
    void dump() const;

private:
    /**
     * Private helper to examine the given collection name against the manifest
     * and add to internal container or throw an exception
     */
    void addCollection(const char* collection, const Manifest& manifest);

    /**
     * Private helper to examine the given collection object against the
     * manifest and add to internal container or throw an exception
     */
    void addCollection(cJSON* object, const Manifest& manifest);

    container filter;
    bool defaultAllowed;
    bool passthrough;
    bool systemEventsAllowed;

    /// Set to true if the json uses a name/uid pair for a collection
    bool uidFound = false;
    /// Set to true if the json uses a name only for a collection
    bool nameFound = false;

    friend std::ostream& operator<<(std::ostream& os, const Filter& filter);
};

std::ostream& operator<<(std::ostream& os, const Filter& filter);

} // end namespace Collections
