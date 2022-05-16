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

/**
 * Class which can encode and decode the payload of a RangeScanContinue.
 * This class knows how to split the payload into keys for a KeyOnly scan.
 */
class RangeScanContinueKeyPayload {
public:
    RangeScanContinueKeyPayload(std::string_view payload);

    /// @return the next key from the payload. Returns empty for no more keys
    std::string_view next();

    /**
     * Encode into a vector a single DocKey (leb128 prefix + key)
     * @param v The vector into which the key is encoded
     * @param key The key to encode into v
     */
    static void encode(std::vector<uint8_t>& v, const DocKey& key);

private:
    std::string_view payload;
};

/**
 * Class which can encode and decode the payload of a RangeScanContinue.
 * This class knows how to split the payload into key, meta and value for a
 * 'document' scan.
 */
class RangeScanContinueValuePayload {
public:
    RangeScanContinueValuePayload(std::string_view payload);

    /**
     * Record defines the 3 elements that represent a single 'document' from
     * a RangeScan
     */
    struct Record {
        std::string_view key;
        std::string_view value;
        cb::mcbp::response::RangeScanContinueMetaResponse meta;
    };

    /// @return the next Record from a payload. empty key/value indicates end
    Record next();

    /**
     * Encode into a vector key/meta/value
     * @param v The vector into which the key/meta/value is encoded
     * @param item The data to encode into v
     */
    static void encode(std::vector<uint8_t>& v, const item_info& item);

private:
    void advance(size_t n);
    std::string_view nextView();
    std::string_view payload;
};

} // namespace cb::mcbp::response