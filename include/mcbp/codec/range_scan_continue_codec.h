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

#include <memcached/protocol_binary.h>

#include <optional>
#include <string>

struct DocKey;
struct item_info;

namespace cb::mcbp::response {

class RangeScanContinueKeyPayload {
public:
    RangeScanContinueKeyPayload(std::string_view payload);

    std::string_view next();

    static void encode(std::vector<uint8_t>& v, const DocKey& key);

private:
    std::string_view payload;
};

class RangeScanContinueValuePayload {
public:
    RangeScanContinueValuePayload(std::string_view payload);

    struct Record {
        std::string_view key;
        std::string_view value;
        cb::mcbp::response::RangeScanContinueMetaResponse meta;
    };

    Record next();

    static void encode(std::vector<uint8_t>& v, const item_info& item);

private:
    void advance(size_t n);
    std::string_view nextView();
    std::string_view payload;
};

} // namespace cb::mcbp::response