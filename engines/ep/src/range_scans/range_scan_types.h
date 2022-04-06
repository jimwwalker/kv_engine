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

#include <memcached/range_scan_id.h>

// Create always begin in Pending and then:
// 1) Pending->Create
// 2) Pending->WaitForPersistence->Create
enum class RangeScanCreateState : char { Pending, WaitForPersistence, Create };

// Data stored in engine-specific during a RangeScan create request
struct RangeScanCreateData {
    cb::rangescan::Id uuid;
    RangeScanCreateState state{RangeScanCreateState::Pending};
};
