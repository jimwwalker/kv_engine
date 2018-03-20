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

#include "collections/manager.h"
#include "collections/filter.h"
#include "collections/manifest.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "string_utils.h"
#include "vbucket.h"

Collections::Manager::Manager() {
}

cb::engine_error Collections::Manager::update(KVBucket& bucket,
                                              const std::string& json) {
    std::unique_lock<std::mutex> ul(lock, std::try_to_lock);
    if (!ul.owns_lock()) {
        // Make concurrent updates fail, in realiy there should only be one
        // admin connection making changes.
        return cb::engine_error(cb::engine_errc::temporary_failure,
                                "Collections::Manager::update already locked");
    }

    std::unique_ptr<Manifest> newManifest;
    // Construct a newManifest (will throw if JSON was illegal)
    try {
        newManifest =
                std::make_unique<Manifest>(json,
                                           bucket.getEPEngine()
                                                   .getConfiguration()
                                                   .getCollectionsMaxSize());
    } catch (std::exception& e) {
        LOG(EXTENSION_LOG_NOTICE,
            "Collections::Manager::update can't construct manifest e.what:%s",
            e.what());
        return cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Collections::Manager::update manifest json invalid:" + json);
    }

    current = std::move(newManifest);

    for (int i = 0; i < bucket.getVBuckets().getSize(); i++) {
        auto vb = bucket.getVBuckets().getBucket(i);

        if (vb && vb->getState() == vbucket_state_active) {
            vb->updateFromManifest(*current);
        }
    }

    return cb::engine_error(cb::engine_errc::success,
                            "Collections::Manager::update");
}

cb::EngineErrorStringPair Collections::Manager::getManifest() const {
    std::unique_lock<std::mutex> ul(lock);
    if (current) {
        return {cb::engine_errc::success, current->toJson()};
    } else {
        return {cb::engine_errc::no_collections_manifest, {}};
    }
}

void Collections::Manager::update(VBucket& vb) const {
    // Lock manager updates
    std::lock_guard<std::mutex> ul(lock);
    if (current) {
        vb.updateFromManifest(*current);
    }
}

Collections::Filter Collections::Manager::makeFilter(
        uint32_t openFlags, cb::const_byte_buffer jsonExtra) const {
    // Lock manager updates
    std::lock_guard<std::mutex> lg(lock);
    boost::optional<const std::string&> jsonFilter;
    std::string json;
    if (openFlags & DCP_OPEN_COLLECTIONS) {
        // assign to std::string as cJSON needs guaranteed zero termination
        json.assign(reinterpret_cast<const char*>(jsonExtra.data()),
                    jsonExtra.size());
        jsonFilter = json;
    }
    return Collections::Filter(jsonFilter, current.get());
}

// This method is really to aid development and allow the dumping of the VB
// collection data to the logs.
void Collections::Manager::logAll(KVBucket& bucket) const {
    std::stringstream ss;
    ss << *this;
    LOG(EXTENSION_LOG_NOTICE, "%s", ss.str().c_str());
    for (int i = 0; i < bucket.getVBuckets().getSize(); i++) {
        auto vb = bucket.getVBuckets().getBucket(i);
        if (vb) {
            std::stringstream vbss;
            vbss << vb->lockCollections();
            LOG(EXTENSION_LOG_NOTICE,
                "vb:%d: %s %s",
                i,
                VBucket::toString(vb->getState()),
                vbss.str().c_str());
        }
    }
}

void Collections::Manager::addStats(const void* cookie,
                                    ADD_STAT add_stat) const {
    std::lock_guard<std::mutex> lg(lock);
    if (current) {
        current->addStats(cookie, add_stat);
    } else {
        add_casted_stat("manifest", "none", add_stat, cookie);
    }
}

class CollectionCountVBucketVisitor : public VBucketVisitor {
public:
    void visitBucket(VBucketPtr& vb) override {
        if (vb->getState() == vbucket_state_active) {
            vb->lockCollections().updateSummary(summary);
        }
    }
    std::unordered_map<std::string, uint64_t> summary;
};

class CollectionDetailedVBucketVisitor : public VBucketVisitor {
public:
    CollectionDetailedVBucketVisitor(const void* c, ADD_STAT a)
        : cookie(c), add_stat(a) {
    }

    void visitBucket(VBucketPtr& vb) override {
        vb->lockCollections().addStats(vb->getId(), cookie, add_stat);
    }

private:
    const void* cookie;
    ADD_STAT add_stat;
};

// collections-details
//   - return top level stats (manager/manifest)
//   - iterate vbuckets returning detailed VB stats
// collections-details n
//   - return detailed VB stats for n only
// collections
//   - return top level stats (manager/manifest)
//   - return per collection item counts from all active VBs
ENGINE_ERROR_CODE Collections::Manager::doStats(KVBucket& bucket,
                                                const void* cookie,
                                                ADD_STAT add_stat,
                                                const std::string& statKey) {
    if (cb_isPrefix(statKey, "collections-details")) {
        // VB maybe encoded in statKey
        auto pos = statKey.find_first_of(" ");
        if (pos != std::string::npos) {
            try {
                uint16_t vbid = std::stoi(statKey.substr(pos));
                VBucketPtr vb = bucket.getVBucket(vbid);
                if (vb) {
                    vb->lockCollections().addStats(vbid, cookie, add_stat);
                }
            } catch (const std::exception& e) {
                return ENGINE_EINVAL;
            }
        } else {
            bucket.getCollectionsManager().addStats(cookie, add_stat);
            CollectionDetailedVBucketVisitor visitor(cookie, add_stat);
            bucket.visit(visitor);
        }
    } else {
        // Do the high level stats (includes global count)
        bucket.getCollectionsManager().addStats(cookie, add_stat);
        CollectionCountVBucketVisitor visitor;
        bucket.visit(visitor);
        for (const auto& entry : visitor.summary) {
            try {
                const int bsize = 512;
                char buffer[bsize];
                checked_snprintf(buffer,
                                 bsize,
                                 "collection:%s:items",
                                 entry.first.c_str());
                add_casted_stat(buffer, entry.second, add_stat, cookie);
            } catch (const std::exception& e) {
                LOG(EXTENSION_LOG_WARNING,
                    "Collections::Manager::doStats failed to build stats: %s",
                    e.what());
            }
        }
    }

    return ENGINE_SUCCESS;
}

void Collections::Manager::dump() const {
    std::cerr << *this;
}

std::ostream& Collections::operator<<(std::ostream& os,
                                      const Collections::Manager& manager) {
    std::lock_guard<std::mutex> lg(manager.lock);
    if (manager.current) {
        os << "Collections::Manager current:" << *manager.current << "\n";
    } else {
        os << "Collections::Manager current:nullptr\n";
    }
    return os;
}
