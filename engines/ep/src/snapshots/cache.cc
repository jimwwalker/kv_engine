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

#include "snapshots/cache.h"
#include "snapshots/snapshots.h"

#include <platform/timeutils.h>

#include <chrono>

namespace Snapshots {

cb::engine_errc Cache::getOrPrepare(
        CookieIface& cookie,
        Vbid vbid,
        std::string_view path,
        KVStoreIface& kvs,
        const std::function<void(const nlohmann::json&)>& callback) {
    auto now = std::chrono::steady_clock::now();
    auto locked = map.lock();
    auto itr = locked->find(vbid);

    // Snapshot exists - return
    if (itr != locked->end()) {
        itr->second->lastTouched = now;
        callback(itr->second->manifest);
        return cb::engine_errc::success;
    }

    auto rv = Snapshots::prepare(cookie, kvs, path, vbid);

    if (std::holds_alternative<cb::engine_errc>(rv)) {
        return std::get<cb::engine_errc>(rv);
    }

    // JSON back to connection/request
    auto& manifest = std::get<nlohmann::json>(rv);
    callback(manifest);

    auto [emplaceItr, emplaced] = locked->emplace(
            vbid, std::make_shared<Snapshot>(now, std::move(manifest)));

    Expects(emplaced);

    return cb::engine_errc::success;
}

cb::engine_errc Cache::releaseSnapshot(CookieIface& cookie,
                                       std::string_view uuid) {
    auto entry = find(uuid, [](Map::iterator i, Map& m) {
        std::shared_ptr<Snapshot> rv;
        rv.swap(i->second);
        m.erase(i);
        return rv;
    });

    if (!entry) {
        EP_LOG_WARN("Snapshots::releaseSnapshot no such snapshot with uuid:{}",
                    uuid);
        return cb::engine_errc::no_such_key;
    }

    return cb::engine_errc::success;
}

void Cache::addSnapshot(Vbid vbid, nlohmann::json manifest) {
    auto now = std::chrono::steady_clock::now();
    auto locked = map.lock();
    auto [emplaceItr, emplaced] =
            locked->emplace(vbid, std::make_shared<Snapshot>(now, manifest));

    Expects(emplaced);
}

void Cache::touch(std::string_view uuid) {
    // for now iterate and find
    auto entry = find(uuid);

    // todo: this is for GetFileFrag which should fail if find is null
    Expects(entry);

    touch(*entry);
}

void Cache::touch(Snapshot& snapshot) {
    // update time
    snapshot.lastTouched = std::chrono::steady_clock::now();
}

int Cache::purge(std::string_view path, std::chrono::seconds maxAge) {
    auto now = std::chrono::steady_clock::now();
    std::vector<MapValue> toRelease = map.withLock([now, maxAge](auto& map){
        std::vector<MapValue> toRelease;
        for (const auto& [vbid, snapshot] : map) {
            if (snapshot->lastTouched < now - maxAge) {
                toRelease.push_back(snapshot);
            }
        }
        return toRelease;
    });

    for (const auto snap : toRelease) {
       Snapshots::release("bg-task", path, snap->manifest);
    }
    return toRelease.size();
}

Snapshots::Cache::MapValue Cache::find(Vbid vbid) {
    return map.withLock([vbid](auto& map) {
        auto itr = map.find(vbid);
        if (itr != map.end()) {
            return itr->second;
        }
        return std::shared_ptr<Snapshot>{};
    });
}

Snapshots::Cache::MapValue Cache::find(
        std::string_view uuid,
        std::function<MapValue(Map::iterator, Map&)> apply) {
    return map.withLock([uuid, apply](auto& map) {
        std::shared_ptr<Snapshot> rv;
        for (auto itr = map.begin(); itr != map.end(); ++itr) {
            if (itr->second->manifest["uuid"] == uuid) {
                return apply(itr, map);
            }
        }
        return rv;
    });
}

void Cache::dump(std::ostream& os) const {
    os << *this << std::endl;
}

std::ostream& operator<<(std::ostream& os, const Cache& cache) {
    cache.map.withLock([&os](auto& map) {
        for (const auto& [vbid, snapshot] : map) {
            os << vbid << "->snapshot:" << *snapshot << std::endl;
        }
    });
    return os;
}

std::ostream& operator<<(std::ostream& os, const Snapshot& s) {
    return os << s.created.time_since_epoch().count() << " "
              << s.lastTouched.time_since_epoch().count() << " " << s.manifest;
}

} // end namespace Snapshots
