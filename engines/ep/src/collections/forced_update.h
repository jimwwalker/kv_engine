/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc.
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

#include "globaltask.h"
#include "kvshard.h"

#pragma once

class EPBucket;

namespace Collections {

class Manifest;

/**
 * A task for doing forced updates of vbuckets from a background task.
 * When a forced update occurs, the update needs extra information from
 * KVStore, hence why this is not done on the front-end.
 *
 * The task is intended to be ran with one task per shard and that task updates
 * the vbuckets of each shard.
 */
class ForcedUpdateTask : public GlobalTask {
public:
    // Data which will be shared between a group of tasks so they can track
    // that all shards have done the update.
    struct CompletionData {
        CompletionData(size_t total) : totalShards(total) {
        }
        std::atomic<size_t> completedShards{0};
        const size_t totalShards{0};
    };

    ForcedUpdateTask(EPBucket& bucket,
                     ::KVShard::id_type shard,
                     const Manifest& manifest,
                     std::shared_ptr<CompletionData> completionData,
                     const void* cookie);

    bool run() override;

    std::string getDescription() override;

    std::chrono::microseconds maxExpectedDuration() override {
        return std::chrono::seconds(1);
    }

private:
    EPBucket& bucket;
    const Manifest& newManifest;
    std::shared_ptr<CompletionData> completionData;
    const void* cookie{nullptr};
    ::KVShard::id_type shard;
};

} // end namespace Collections