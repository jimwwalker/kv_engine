/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "snapshots/snapshots.h"

#include "bucket_logger.h"
#include "kvstore/kvstore_iface.h"

#include <logger/logger.h>
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <platform/uuid.h>

constexpr auto snapdir = "snapshots";
constexpr auto manifest = "manifest.json";

std::uintmax_t remove_all(const std::filesystem::path& path,
                          std::error_code& ec,
                          int line) {
    auto rv = std::filesystem::remove_all(path, ec);

    if (ec) {
        EP_LOG_WARN("snapshots: failed remove_all: path:{} error:{} at line:{}",
                    path.string(),
                    ec.message(),
                    line);
    }
    return rv;
}

std::variant<cb::engine_errc, nlohmann::json> Snapshots::prepare(
        CookieIface& cookie,
        KVStoreIface& kvs,
        std::string_view path,
        Vbid vbid) {
    const auto base = std::filesystem::path{path} / snapdir; // snapshots
    const auto base_vb = base / std::to_string(vbid.get()); // snapshots/1
    const auto manifest_file = base_vb / manifest; // snapshots/1/manifest.json

    if (exists(manifest_file)) {
        try {
            // @todo: consider moving this to a pre-prepare phase.
            // This should validate the found  manifest matches the uuid sub-dir
            // If the pre-prepare found a mismatch of manifest and uuid sub-dir
            // probably burn the snapshot and hit prepare (and log it all).
            auto manifest =
                    nlohmann::json::parse(cb::io::loadFile(manifest_file));

            return manifest;
        } catch (std::exception& exception) {
            // an error occurred..
            LOG_WARNING_CTX("Failed to read existing manifest",
                            {"conn_id", cookie.getConnectionId()},
                            {"error", exception.what()});
            return cb::engine_errc::failed;
        }
    }

    std::string uuid = ::to_string(cb::uuid::random());
    auto snapshotDirectory = base / uuid;
    std::optional<std::filesystem::path> linkFile;

    try {
        create_directories(snapshotDirectory);
        create_symlink("." / snapshotDirectory.filename(), base_vb);
        linkFile = base_vb;
        auto [rv, files] = kvs.prepareSnapshot(snapshotDirectory, vbid);

        if (rv != cb::engine_errc::success) {
            std::error_code ec;
            remove_all(snapshotDirectory, ec, __LINE__);
            remove_all(linkFile.value(), ec, __LINE__);
            return rv;
        }

        nlohmann::json array = nlohmann::json::array();
        int ii = 0;
        for (const auto& info : files) {
            array.emplace_back(
                    nlohmann::json{{"id", ++ii},
                                   {"path", info.path.string()},
                                   {"size", std::to_string(info.size)},
                                   {"deks", info.deks}});
        }
        nlohmann::json manifest = {{"uuid", uuid}, {"files", std::move(array)}};
        FILE* fp = fopen(manifest_file.string().c_str(), "w");
        if (!fp) {
            EP_LOG_WARN_CTX("Failed to save vbucket snapshot manifest",
                            {"conn_id", cookie.getConnectionId()},
                            {"file", manifest_file.string().c_str()},
                            {"error", cb_strerror()});
            return cb::engine_errc::failed;
        }
        fprintf(fp, "%s\n", manifest.dump().c_str());
        fclose(fp);
        return manifest;

    } catch (const std::exception& exception) {
        LOG_WARNING_CTX("Failed to prepare snapshot",
                        {"conn_id", cookie.getConnectionId()},
                        {"error", exception.what()});

        std::error_code ec;
        remove_all(snapshotDirectory, ec, __LINE__);
        if (linkFile.has_value()) {
            remove_all(linkFile.value(), ec, __LINE__);
        }
    }

    return cb::engine_errc::failed;
}

cb::engine_errc Snapshots::release(CookieIface& cookie,
                                   std::string_view path,
                                   std::string_view uuid) {
    const auto snapshot = std::filesystem::path{path} / snapdir / uuid;

    if (!exists(snapshot)) {
        return cb::engine_errc::no_such_key;
    }

    try {
        // find the link pointing to the snapshot:
        std::error_code ec;
        for (const auto& p :
             std::filesystem::directory_iterator(snapshot.parent_path(), ec)) {
            if (is_symlink(p.path(), ec)) {
                auto target = read_symlink(p.path(), ec);
                if (target.filename().string() == uuid) {
                    remove_all(p.path(), ec);
                    break;
                }
            }
        }
        // finally remove the actual snapshot
        remove_all(snapshot);
    } catch (const std::exception& exception) {
        EP_LOG_WARN_CTX("Failed to remove snapshot",
                        {"conn_id", cookie.getConnectionId()},
                        {"uuid", uuid},
                        {"exception", exception.what()});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}
