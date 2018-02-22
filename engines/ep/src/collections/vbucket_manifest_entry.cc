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

#include "collections/vbucket_manifest_entry.h"
#include "statwriter.h"

#include <platform/checked_snprintf.h>

SystemEvent Collections::VB::ManifestEntry::completeDeletion() {
    if (isExclusiveDeleting()) {
        // All items from any generation of the collection gone, so delete the
        // collection metadata
        return SystemEvent::DeleteCollectionHard;
    } else if (isOpenAndDeleting()) {
        resetEndSeqno(); // reset the end to the special seqno and return soft
        return SystemEvent::DeleteCollectionSoft;
    }
    throwException<std::logic_error>(__FUNCTION__, "invalid state");
}

std::string Collections::VB::ManifestEntry::getExceptionString(
        const std::string& thrower, const std::string& error) const {
    std::stringstream ss;
    ss << "VB::ManifestEntry::" << thrower << ": " << error
       << ", this:" << *this;
    return ss.str();
}

void Collections::VB::ManifestEntry::addStats(uint16_t vbid,
                                              const void* cookie,
                                              ADD_STAT add_stat) const {
    try {
        const int bsize = 512;
        char buffer[bsize];
        const auto name = getCollectionName();
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:uid",
                         vbid,
                         name.c_str());
        add_casted_stat(buffer, getUid(), add_stat, cookie);
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:start_seqno",
                         vbid,
                         name.c_str());
        add_casted_stat(buffer, getStartSeqno(), add_stat, cookie);
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:end_seqno",
                         vbid,
                         name.c_str());
        add_casted_stat(buffer, getEndSeqno(), add_stat, cookie);
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:items",
                         vbid,
                         name.c_str());
        add_casted_stat(buffer, getDiskCount(), add_stat, cookie);
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "VB::ManifestEntry::addStats (vb:%" PRIu16
            ") failed to build stats: %s",
            vbid,
            error.what());
    }
}

std::ostream& Collections::VB::operator<<(
        std::ostream& os,
        const Collections::VB::ManifestEntry& manifestEntry) {
    os << "ManifestEntry: collection:" << manifestEntry.getCollectionName()
       << ", uid:" << manifestEntry.getUid()
       << ", startSeqno:" << manifestEntry.getStartSeqno()
       << ", endSeqno:" << manifestEntry.getEndSeqno()
       << ", diskCount:" << manifestEntry.getDiskCount();

    return os;
}