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

#include "collections/scan_context.h"
#include "collections/vbucket_manifest.h"

#pragma once

namespace Collections {
namespace VB {

/**
 * The EraserContext holds a reference to the manifest which is used by the
 * erasing process of a collection, keys will be tested for isLogicallyDeleted
 * with this object's manifest.
 *
 * Additionally the class tracks how many collections were fully erased.
 *
 */
class EraserContext : public ScanContext {
public:
    EraserContext(const std::vector<Collections::KVStore::DroppedCollection>&
                          droppedCollections);

    void processEndOfCollection(const DocKey& key, SystemEvent se);

    bool needToUpdateCollectionsMetadata() const;

    bool empty() const {
        return dropped.empty();
    }

private:
    friend std::ostream& operator<<(std::ostream&, const EraserContext&);

    void remove(CollectionID cid);

    bool removed = false;
};

std::ostream& operator<<(std::ostream&, const EraserContext&);

} // namespace VB
} // namespace Collections
