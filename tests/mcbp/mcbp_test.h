/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

/*
 * Memcached binary protocol validator tests.
 */

#include "config.h"

#include "mock_connection.h"

#include <daemon/connection.h>
#include <daemon/mcbp_validators.h>
#include <daemon/stats.h>
#include <gtest/gtest.h>

namespace mcbp {
namespace test {

class ValidatorTest : public ::testing::Test {
public:
    ValidatorTest(bool collectionsEnabled);
    void SetUp() override;

protected:
    /**
     * Validate that the provided packet is correctly encoded
     *
     * @param opcode The opcode for the packet
     * @param request The packet to validate
     */
    protocol_binary_response_status validate(protocol_binary_command opcode,
                                             void* request);
    std::string validate_error_context(protocol_binary_command opcode,
                                       void* request);

    McbpValidatorChains validatorChains;

    MockConnection connection;

    // backing store which may be used for the request
    protocol_binary_request_no_extras &request;
    uint8_t blob[4096]{};

    bool collectionsEnabled{false};
};

} // namespace test
} // namespace mcbp
