/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "download_snapshot_task.h"

#include "platform/dirutils.h"
#include "platform/timeutils.h"
#include "protocol/connection/client_connection.h"
#include "protocol/connection/client_mcbp_commands.h"
#include "utilities/engine_errc_2_mcbp.h"

#include <bucket_logger.h>
#include <ep_engine.h>
#include <memcached/cookie_iface.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>
#include <snapshot_download/snapshot_downloader.h>

class DownloadSnapshotTaskImpl : public DownloadSnapshotTask {
public:
    DownloadSnapshotTaskImpl(CookieIface& cookie,
                             EventuallyPersistentEngine& ep,
                             nlohmann::json manifest)
        : DownloadSnapshotTask(ep),
          cookie(cookie),
          snapshot_manifest(manifest["snapshot_manifest"]),
          source(manifest["source"]),
          directory(std::filesystem::path(ep.getConfiguration().getDbname()) /
                    "snapshots" / snapshot_manifest["uuid"]){};
    std::string getDescription() const override {
        return "DownloadSnapshotTask";
    }
    std::chrono::microseconds maxExpectedDuration() const override {
        // @todo this could be deducted from the total size
        return std::chrono::seconds(30);
    }
    std::pair<cb::engine_errc, std::string> getResult() const override {
        return result.copy();
    }

protected:
    bool run() override;

    CookieIface& cookie;
    const nlohmann::json snapshot_manifest;
    const nlohmann::json source;
    std::filesystem::path directory;
    folly::Synchronized<std::pair<cb::engine_errc, std::string>, std::mutex>
            result;
};

bool DownloadSnapshotTaskImpl::run() {
    try {
        auto connection = std::make_unique<MemcachedConnection>(
                source["host"],
                source["port"].get<in_port_t>(),
                AF_UNSPEC,
                source.contains("tls"));
        if (source.contains("tls")) {
            connection->setTlsConfigFiles(
                    source["tls"]["cert"].get<std::string>(),
                    source["tls"]["key"].get<std::string>(),
                    source["tls"]["ca_store"].get<std::string>());
            std::string passphrase = source["tls"].value("passphrase", "");
            if (!passphrase.empty()) {
                connection->setPemPassphrase(cb::base64::decode(passphrase));
            }
        }
        connection->connect();

        if (source.contains("sasl")) {
            connection->authenticate(source["sasl"].value("username", ""),
                                     source["sasl"].value("password", ""),
                                     source["sasl"].value("mechanism", ""));
        }
        connection->selectBucket(source["bucket"].get<std::string>());

        snapshot::download(std::move(connection),
                           directory,
                           snapshot_manifest,
                           [this](auto level, auto msg, auto json) {
                               auto& logger = getGlobalBucketLogger();
                               logger->logWithContext(level, msg, json);
                           });

        result = {cb::engine_errc::success, ""};
    } catch (const std::exception& e) {
        result = {cb::engine_errc::failed,
                  fmt::format("Received exception: {}", e.what())};
        EP_LOG_ERR_CTX("DownloadSnapshotTaskImpl::run()",
                       {"conn_id", cookie.getConnectionId()},
                       {"error", e.what()});
    }
    cookie.notifyIoComplete(cb::engine_errc::success);
    return false;
}

std::shared_ptr<DownloadSnapshotTask> DownloadSnapshotTask::create(
        CookieIface& cookie,
        EventuallyPersistentEngine& ep,
        std::string_view manifest) {
    return std::make_shared<DownloadSnapshotTaskImpl>(
            cookie, ep, nlohmann::json::parse(manifest));
}

DownloadSnapshotTask::DownloadSnapshotTask(EventuallyPersistentEngine& ep)
    : EpTask(ep, TaskId::DownloadSnapshotTask, 0, true) {
}
