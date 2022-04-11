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

#include "engine_wrapper.h"
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <memcached/range_scan_optional_configuration.h>

void create_range_scan_executor(Cookie& cookie) {
    std::pair<cb::engine_errc, cb::rangescan::Id> status;
    status.first = cookie.swapAiostat(cb::engine_errc::success);

    if (status.first == cb::engine_errc::success) {
        const auto& req = cookie.getRequest();

        // scan it all for now - default collection only

        cb::rangescan::KeyView start{"\0", 1};
        cb::rangescan::KeyView end{"\xFF"};

        status = createRangeScan(cookie,
                                 req.getVBucket(),
                                 CollectionID::Default,
                                 start,
                                 end,
                                 cb::rangescan::KeyOnly::Yes,
                                 {},
                                 {});
    }

    if (status.first != cb::engine_errc::success) {
        handle_executor_status(cookie, status.first);
    } else {
        // Success - we have an id to return
        cookie.getConnection().sendResponse(
                cookie,
                cb::mcbp::Status::Success,
                {},
                {},
                {reinterpret_cast<const char*>(status.second.data),
                 status.second.size()},
                PROTOCOL_BINARY_RAW_BYTES,
                nullptr);
    }
}
