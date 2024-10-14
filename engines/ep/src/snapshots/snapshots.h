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

#include <memcached/engine_error.h>
#include <memcached/vbucket.h>

class CookieIface;
class KVStoreIface;

namespace Snapshots {

/**
 * Prepare a snapshot.
 *
 * This function will prepare a snapshot and return a JSON manifest describing
 * the snapshot.
 *
 * The given path is where all file activity will take place.
 *
 * snapshots are referenced by vbid and a uuid, only one snapshot per vb
 * can exist, and the uuid gives a "point-in-time" identifier.
 *
 * E.g. if the arguments are /path/ and Vbid(1) on success we will see.
 *
 *    $> ls /path/snapshots/
 *    1 -> ./uuid
 *    uuid
 *
 *    $> ls /path/snapshots/1 (or uuid)
 *    1.couch.1
 *    manifest.json
 *
 * That is a directory named with the snapshot uuid and a symlink named after
 * the vbucket pointing to the uuid directory.
 *
 * This function will return a JSON manifest of a prepared snapshot that
 * describes all files of the snapshot (names, sizes). This is stored in the
 * snapshot directory as manifest.json.
 *
 * @param cookie cookie executing PrepareSnapshot
 * @param kvs KVStore relevant to vbid
 * @param path base path for snapshots
 * @param vbid vbucket to prepare a snapshot from
 */
std::variant<cb::engine_errc, nlohmann::json> prepare(CookieIface& cookie,
                                                      KVStoreIface& kvs,
                                                      std::string_view path,
                                                      Vbid vbid);

cb::engine_errc release(std::string_view connectionId,
                        std::string_view path,
                        std::string_view uuid);

} // end namespace Snapshots