/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "kvstore/kvstore_iface.h"

/**
 * MagmaScanResult expands the KVStore scan_error_t to include one extra status
 * (scan_next) used internally by the magma scan functions.
 */
struct MagmaScanResult {
    enum class Status {
        Success, // Scan loop successfully processed the last item, continue
        Again, // Scan loop couldn't process the last item, try again
        Failed // Scan loop failed process the last item, hard failure, stop
    } code;

    MagmaScanResult(Status s) : code(s) {
    }

    static MagmaScanResult Success() {
        return MagmaScanResult(Status::Success);
    }
    static MagmaScanResult Again() {
        return MagmaScanResult(Status::Again);
    }
    static MagmaScanResult Failed() {
        return MagmaScanResult(Status::Failed);
    }

    explicit operator scan_error_t() const {
        switch (code) {
        case Status::Success:
            return scan_error_t::scan_success;
        case Status::Again:
            return scan_error_t::scan_again;
        case Status::Failed:
            return scan_error_t::scan_failed;
        default:
            throw std::runtime_error(
                    "MagmaScanResult: cannot convert to scan_error_t");
        }
    }
};