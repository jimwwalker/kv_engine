/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <boost/uuid/uuid.hpp>

using RangeScanId = boost::uuids::uuid;

enum class RangeScanKeyOnly : char { No, Yes };

// Create always begin in Pending and then:
// 1) Pending->Create
// 2) Pending->WaitForPersistence->Create
enum class RangeScanCreateState : char { Pending, WaitForPersistence, Create };

struct RangeScanCreateData {
    RangeScanId uuid;
    RangeScanCreateState state{RangeScanCreateState::Pending};
};

struct RangeScanSnapshotRequirements {
    // The vbucket on the frontend request must match this uuid
    // The snapshot we must also match this uuid
    uint64_t vbUuid{0};
    // This seqno must of been persisted to snapshot
    uint64_t seqno{0};
    /**
     * This is the timeout to use when the seqno is not yet persisted.
     * This is optional to allow for a timeout of 0 in unit tests (so no real
     * waiting), but other APIs can use 0 as "no-timeout" and leave this
     * variable as std::nullopt
     */
    std::optional<std::chrono::milliseconds> timeout;
    // true: The seqno must still exist in snapshot
    bool seqnoMustBeInSnapshot{false};
};

struct RangeScanSamplingConfiguration {
    size_t seed{0};
    size_t samples{0};
};