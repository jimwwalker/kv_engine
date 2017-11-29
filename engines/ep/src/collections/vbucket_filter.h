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

#include <memcached/dockey.h>
#include <memcached/engine_common.h>
#include <platform/sized_buffer.h>

#include <memory>
#include <string>
#include <unordered_map>

class SystemEventMessage;
class Item;

namespace Collections {

class Filter;

namespace VB {

class Manifest;

/**
 * The VB filter is used to decide if keys on a DCP stream should be sent
 * or dropped.
 *
 * A filter is built from the Collections::Filter that was established when
 * the producer was opened. During the time the producer was opened and a
 * stream is requested, filtered collections may have been deleted, so the
 * ::VB::Filter becomes the intersection of the producer's filter and the
 * open collections within the manifest.
 *
 * Note: There is no locking on a VB::Filter as at the moment it is constructed
 * and then is not mutable.
 */

class Filter {
public:
    class Empty : public std::exception {};

    /**
     * Construct a Collections::VB::Filter using the producer's
     * Collections::Filter and the vbucket's collections manifest.
     *
     * If the producer's filter is configured to filter collections then the
     * resulting object will filter the intersection filter:manifest
     * collections. The constructor will log when it finds it must drop a
     * collection
     *
     * If the producer's filter is effectively a passthrough
     * (allowAllCollections returns true) then so will the resulting VB filter.
     *
     * @param filter The producer's filter that the client configured.
     * @param manifest The vbucket's collection manifest.
     */
    Filter(const ::Collections::Filter& filter,
           const ::Collections::VB::Manifest& manifest);

    /**
     * @param item an Item to check against the filter
     * @returns if the filter allows key based on the filter contents
     */
    bool allow(const Item& item) const;
    bool checkAndUpdate(const Item& item);

    bool empty() const;
    void remove(const Item& item);
    bool remove(cb::const_char_buffer coll);

    /**
     * Add statistics for this filter, currently just depicts the object's state
     */
    void addStats(ADD_STAT add_stat,
                  const void* c,
                  const std::string& prefix,
                  uint16_t vb) const;

    /**
     * Dump this to std::cerr
     */
    void dump() const;

protected:
    /**
     * Does the filter allow the system event? I.e. a "meat,dairy" filter
     * shouldn't allow delete events for the "fruit" collection.
     *
     * @param item a SystemEventMessage to check
     * @param return true if the filter says this event should be allowed
     */
    bool allowSystemEvent(const Item& item) const;

    using Container = ::std::unordered_map<cb::const_char_buffer,
                                           std::unique_ptr<std::string>>;
    Container filter;
    bool defaultAllowed;
    bool passthrough;
    bool systemEventsAllowed;
    std::string separator;

    friend std::ostream& operator<<(std::ostream& os, const Filter& filter);
};

std::ostream& operator<<(std::ostream& os, const Filter& filter);

} // end namespace VB
} // end namespace Collections
