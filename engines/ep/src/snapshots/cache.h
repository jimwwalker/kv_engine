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

#pragma once

#include <folly/Synchronized.h>
#include <nlohmann/json.hpp>

namespace Snapshots {

struct Snapshot {
    Snapshot(std::chrono::steady_clock::time_point lastTouched,
             nlohmann::json manifest)
        : created(std::chrono::steady_clock::now()),
          lastTouched(lastTouched),
          manifest(std::move(manifest)) {
    }
    std::chrono::steady_clock::time_point created;
    std::chrono::steady_clock::time_point lastTouched;
    bool released{false}; // released and in map means gone
    nlohmann::json manifest;

    friend std::ostream& operator<<(std::ostream&, const Snapshots::Snapshot&);
};

/**
 * Snapshots provides a cache of all known vbucket snapshots.
 *
 * A vbucket snapshot is created on demand by the PrepareSnapshot command, these
 * snapshots then live in the bucket data directory (but cached here).
 *
 * A snapshot is permitted to exist provided that certain operations occur
 * against it (GetFileFragment).
 *
 * A snapshot that has no operations occur against it will be evicted after some
 * duration, resulting in real disk artefacts being deleted.
 *
 * A snapshot should in general be removed "co-operatively" by the client
 * issuing ReleaseSnapshot.
 *
 * Note that the snapshot cache is populated by warmup, ensuring any scans
 * created before a restart are monitored and usable if the client reconnects.
 *
 * The cache is indexed by vbucket-id, but in general snapshots are accessed by
 * a uuid (so we have some point-in-time ID). The cache for now keeps one data
 * sturcture and doesn't have an "optimal" uuid lookup, we will scan by uuid
 * when locating the snapshot.
 */
class Cache {
public:
    cb::engine_errc getOrPrepare(
            CookieIface& cookie,
            Vbid vbid,
            std::string_view path,
            KVStoreIface& kvs,
            const std::function<void(const nlohmann::json&)>& callback);

    cb::engine_errc releaseSnapshot(CookieIface& cookie, std::string_view uuid);

    /**
     * Add a snapshot found during warmup
     */
    void addSnapshot(Vbid vbid, nlohmann::json manifest);

    /**
     * touch the last used time to extend the snapshots lifespan.
     */
    void touch(std::string_view uuid);

    /**
     * Purge snapshots which have not been touched
     *
     * @return count of purged snapshots
     */
    int purge(std::string_view path, std::chrono::seconds maxAge);

    void dump(std::ostream& os = std::cerr) const;

    static std::variant<cb::engine_errc, nlohmann::json> prepare(
            std::string_view path, KVStoreIface& kvs, Vbid vbid);

protected:
    void touch(Snapshot& snapshot);

    using MapValue = std::shared_ptr<Snapshot>;
    using Map = std::unordered_map<Vbid, MapValue>;

    MapValue find(Vbid vbid);
    MapValue find(
            std::string_view uuid,
            std::function<MapValue(Map::iterator, Map&)> apply =
                    [](Map::iterator i, Map& m) { return i->second; });

    folly::Synchronized<Map, std::mutex> map;

    friend std::ostream& operator<<(std::ostream&, const Cache&);
};

std::ostream& operator<<(std::ostream& os, const Cache&);
std::ostream& operator<<(std::ostream& os, const Snapshot&);
} // end namespace Snapshots
