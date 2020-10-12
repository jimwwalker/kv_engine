/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "collections/persist_manifest_task.h"
#include "bucket_logger.h"
#include "collections/collections_types.h"
#include "collections/manifest.h"
#include "collections/manifest_generated.h"
#include "ep_bucket.h"
#include "ep_engine.h"

#include <nlohmann/json.hpp>
#include <platform/crc32c.h>
#include <platform/dirutils.h>

#include <fstream>

namespace Collections {

PersistManifestTask::PersistManifestTask(
        EPBucket& bucket,
        std::unique_ptr<Collections::Manifest> manifest,
        const void* cookie)
    : ::GlobalTask(&bucket.getEPEngine(),
                   TaskId::PersistCollectionsManifest,
                   0,
                   true),
      manifest(std::move(manifest)),
      cookie(cookie) {
}

std::string PersistManifestTask::getDescription() {
    return "PersistManifestTask for " + engine->getName();
}

bool PersistManifestTask::run() {
    auto fname = std::string(ManifestFileName);
    std::string tmpFile = engine->getConfiguration().getDbname() +
                          cb::io::DirectorySeparator + cb::io::mktemp(fname);
    std::string finalFile = engine->getConfiguration().getDbname() +
                            cb::io::DirectorySeparator + fname;

    auto fbData = manifest->toFlatbuffer();

    // Now wrap with a CRC
    flatbuffers::FlatBufferBuilder builder;
    auto fbManifest = builder.CreateVector(fbData.data(), fbData.size());
    auto toWrite = Collections::Persist::CreateManifestWithCrc(
            builder, crc32c(fbData.data(), fbData.size(), 0), fbManifest);
    builder.Finish(toWrite);

    std::ofstream writer(tmpFile, std::ofstream::trunc | std::ofstream::binary);
    writer.write(reinterpret_cast<const char*>(builder.GetBufferPointer()),
                 builder.GetSize());
    writer.close();

    ENGINE_ERROR_CODE status = ENGINE_SUCCESS;
    if (!writer.good()) {
        status = ENGINE_ERROR_CODE(
                cb::engine_errc::cannot_apply_collections_manifest);
        // log the bad, the fail and the eof.
        EP_LOG_WARN(
                "PersistManifestTask::run writer error bad:{} fail:{} eof:{}",
                writer.bad(),
                writer.fail(),
                writer.eof());
        // failure, when this task goes away the manifest will be destroyed
    } else {
        // Now 'move' tmp over the output file.
        if (rename(tmpFile.c_str(), finalFile.c_str()) != 0) {
            EP_LOG_WARN(
                    "PersistManifestTask::run failed to rename {} to {}, "
                    "errno:{}",
                    tmpFile,
                    finalFile,
                    errno);
        } else {
            // Success, release the manifest back to set_collections
            manifest.release();
        }
    }

    if (remove(tmpFile.c_str()) == 0) {
        EP_LOG_WARN("PersistManifestTask::run failed to remove {} errno:",
                    tmpFile,
                    errno);
    }

    engine->notifyIOComplete(cookie, status);
    return false;
}

std::optional<std::unique_ptr<Manifest>> PersistManifestTask::tryAndLoad(
        const std::string& dbname) {
    std::string fname =
            dbname + cb::io::DirectorySeparator + std::string(ManifestFileName);
    if (!cb::io::isFile(fname)) {
        return nullptr;
    }

    std::unique_ptr<Manifest> rv;
    try {
        auto manifestRaw = cb::io::loadFile(fname);

        // First do a verification with FlatBuffers - this does a basic check
        // that the data appears to be of the correct schema, but does not
        // detect values that changed in-place.
        flatbuffers::Verifier v(
                reinterpret_cast<const uint8_t*>(manifestRaw.data()),
                manifestRaw.size());
        if (!v.VerifyBuffer<Collections::Persist::ManifestWithCrc>(nullptr)) {
            EP_LOG_CRITICAL(
                    "PersistManifestTask::tryAndLoad failed VerifyBuffer");
            return std::nullopt;
        }

        auto fbData =
                flatbuffers::GetRoot<Collections::Persist::ManifestWithCrc>(
                        manifestRaw.data());

        // Now re-do the CRC which will pick-up unexpected changes
        uint32_t storedCrc = fbData->crc();
        uint32_t crc = crc32c(
                fbData->manifest()->data(), fbData->manifest()->size(), 0);
        if (crc != storedCrc) {
            EP_LOG_CRITICAL(
                    "PersistManifestTask::tryAndLoad failed crc mismatch "
                    "storedCrc:{}, crc:{} ",
                    storedCrc,
                    crc);
            return std::nullopt;
        }

        std::string_view view(
                reinterpret_cast<const char*>(fbData->manifest()->data()),
                fbData->manifest()->size());
        return std::make_unique<Manifest>(view, Manifest::FlatBuffers{});
    } catch (const std::exception& e) {
        EP_LOG_CRITICAL("PersistManifestTask::tryAndLoad failed {}", e.what());
    }
    return std::nullopt;
}

} // namespace Collections
