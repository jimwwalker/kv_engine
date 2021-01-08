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

#include "collections/forced_update.h"
#include "bucket_logger.h"
#include "ep_bucket.h"
#include "ep_engine.h"

namespace Collections {

ForcedUpdateTask::ForcedUpdateTask(
        EPBucket& bucket,
        ::KVShard::id_type shard,

        const Collections::Manifest& manifest,
        std::shared_ptr<CompletionData> completionData,
        const void* cookie)
    : ::GlobalTask(
              &bucket.getEPEngine(), TaskId::ForcedCollectionsUpdate, 0, true),
      bucket(bucket),
      newManifest(manifest),
      completionData(completionData),
      cookie(cookie),
      shard(shard) {
}

std::string ForcedUpdateTask::getDescription() {
    return "ForcedUpdateTask for shard:" + std::to_string(shard);
}

bool ForcedUpdateTask::run() {
    EP_LOG_INFO("ForcedUpdateTask::run {} {} {}",
                shard,
                completionData->completedShards,
                completionData->totalShards);

    auto vbuckets = bucket.getVBuckets().getShard(shard)->getVBuckets();
    for (auto vbid : vbuckets) {
        auto vb = bucket.getVBucket(vbid);
        if (vb && vb->getState() == vbucket_state_active) {
            // A forced update shouldn't have any error to handle
            auto status = vb->updateFromManifest(newManifest);
            if (status != Collections::VB::ManifestUpdateStatus::Success) {
                EP_LOG_WARN("ForcedUpdateTask updateFromManifest status:{}",
                            to_string(status));
            }
        }
    }

    if (++completionData->completedShards == completionData->totalShards) {
        EP_LOG_INFO("ForcedUpdateTask::run notifyIOComplete:{}", shard);
        // finished
        engine->notifyIOComplete(cookie, ENGINE_SUCCESS);
    }
    return false;
}

} // end namespace Collections