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
#include "config.h"

#include "mcbp_test.h"
#include "utilities/protocol2text.h"
#include "protocol/connection/client_mcbp_commands.h"

#include <memcached/protocol_binary.h>
#include <algorithm>
#include <vector>

/*
 * Sub-document API validator tests
 */

namespace mcbp {
namespace test {

// Single-path subdocument API commands
class SubdocSingleTest : public ValidatorTest,
                         public ::testing::WithParamInterface<bool> {
public:
    SubdocSingleTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 3;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(/*keylen*/ 10 +
                                                                  /*extlen*/ 3 +
                                                                  /*pathlen*/ 1);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.extras.pathlen = htons(1);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }

    protocol_binary_request_subdocument &request =
        *reinterpret_cast<protocol_binary_request_subdocument*>(blob);
};

TEST_P(SubdocSingleTest, Get_Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_GET));
}

TEST_P(SubdocSingleTest, Get_InvalidBody) {
    // Need a non-zero body.
    request.message.header.request.bodylen = htonl(0);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_GET));

    // Make sure we detect if it won't fit in the packet (extlen + key + path
    // is bigger than in the full packet
    request.message.header.request.extlen = 7;
    request.message.header.request.bodylen = htonl(10 + 5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_GET));
}

TEST_P(SubdocSingleTest, Get_InvalidPath) {
    // Need a non-zero path.
    request.message.header.request.bodylen =
        htonl(/*keylen*/ 10 + /*extlen*/ 3);
    request.message.extras.pathlen = htons(0);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_GET));
}

TEST_P(SubdocSingleTest, DictAdd_InvalidValue) {
    // Need a non-zero value.
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD));
}

TEST_P(SubdocSingleTest, DictAdd_InvalidExtras) {
    // Extlen can be 3, 4, 7 or 8
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD));

    request.message.header.request.extlen = 7;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD));

    request.message.header.request.bodylen = htonl(10 + 7 + 1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS));
}

class SubdocMultiLookupTest : public ValidatorTest,
                              public ::testing::WithParamInterface<bool> {
public:
    SubdocMultiLookupTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();

        // Setup basic, correct header.
        request.setKey("multi_lookup");
        request.addLookup(
            {PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, SUBDOC_FLAG_NONE, "[0]"});
    }

protected:
    int validate(const std::vector<uint8_t> request) {
        void* packet =
            const_cast<void*>(static_cast<const void*>(request.data()));
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP,
                                       packet);
    }

    int validate(const BinprotSubdocMultiLookupCommand& cmd) {
        std::vector<uint8_t> packet;
        cmd.encode(packet);
        return validate(packet);
    }

    BinprotSubdocMultiLookupCommand request;
};

TEST_P(SubdocMultiLookupTest, Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
}

TEST_P(SubdocMultiLookupTest, InvalidMagic) {
    std::vector<uint8_t> payload;
    request.encode(payload);
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_P(SubdocMultiLookupTest, InvalidDatatype) {
    std::vector<uint8_t> payload;
    request.encode(payload);
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
    header->request.datatype = (PROTOCOL_BINARY_DATATYPE_SNAPPY |
                                PROTOCOL_BINARY_DATATYPE_JSON);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_SNAPPY;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_P(SubdocMultiLookupTest, InvalidKey) {
    request.setKey("");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiLookupTest, InvalidExtras) {
    std::vector<uint8_t> payload;
    request.encode(payload);

    // add backing space for the extras
    payload.resize(payload.size() + 4);

    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.extlen = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));

    // extlen of 4 permitted for mutations only.
    header->request.extlen = 4;
    header->request.bodylen = htonl(ntohl(header->request.bodylen) + 4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_P(SubdocMultiLookupTest, NumPaths) {
    // Need at least one path.
    request.clearLookups();
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO, validate(request));

    // Should handle total of 16 paths.
    request.clearLookups();
    // Add maximum number of paths.
    BinprotSubdocMultiLookupCommand::LookupSpecifier spec{
        PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, SUBDOC_FLAG_NONE, "[0]"};
    for (unsigned int i = 0; i < 16; i++) {
        request.addLookup(spec);
    }
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));

    // Add one more - should now fail.
    request.addLookup(spec);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO, validate(request));
}

TEST_P(SubdocMultiLookupTest, ValidLocationOpcodes) {
    // Check that GET is supported.
    request.clearLookups();
    request.addLookup(
        {PROTOCOL_BINARY_CMD_SUBDOC_GET, SUBDOC_FLAG_NONE, "[0]"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
}

TEST_P(SubdocMultiLookupTest, InvalidLocationOpcodes) {
    // Check that all opcodes apart from the two lookup ones are not supported.

    for (uint8_t ii = 0; ii < std::numeric_limits<uint8_t>::max(); ii++) {
        auto cmd = protocol_binary_command(ii);
        // Skip over lookup opcodes
        if ((cmd == PROTOCOL_BINARY_CMD_GET) ||
            (cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET) ||
            (cmd == PROTOCOL_BINARY_CMD_SUBDOC_EXISTS) ||
            (cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT)) {
            continue;
        }
        request.at(0) = {cmd, SUBDOC_FLAG_NONE, "[0]"};
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
                  validate(request))
                    << "Failed for cmd:" << memcached_opcode_2_text(ii);
    }
}

TEST_P(SubdocMultiLookupTest, InvalidLocationPaths) {
    // Path must not be zero length.
    request.at(0).path.clear();
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Maximum length should be accepted...
    request.at(0).path.assign(1024, 'x');
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));

    // But any longer should be rejected.
    request.at(0).path.push_back('x');
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiLookupTest, InvalidLocationFlags) {
    // Both GET and EXISTS do not accept any flags.
    for (auto& opcode :
        {PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, PROTOCOL_BINARY_CMD_SUBDOC_GET}) {
        request.at(0).opcode = opcode;
        request.at(0).flags = SUBDOC_FLAG_MKDIR_P;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
        request.at(0).flags = SUBDOC_FLAG_NONE;

        request.addDocFlag(mcbp::subdoc::doc_flag::Mkdoc);
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
        request.clearDocFlags();

        request.addDocFlag(mcbp::subdoc::doc_flag::Add);
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

        request.addDocFlag(mcbp::subdoc::doc_flag::Mkdoc);
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
        request.clearDocFlags();
    }
}

/*** MULTI_MUTATION **********************************************************/

class SubdocMultiMutationTest : public ValidatorTest,
                                public ::testing::WithParamInterface<bool> {
public:
    SubdocMultiMutationTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();

        // Setup basic, correct header.
        request.setKey("multi_mutation");
        request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                             protocol_binary_subdoc_flag(0),
                             "key",
                             "value"});
    }

protected:
    int validate(const BinprotSubdocMultiMutationCommand& cmd) {
        std::vector<uint8_t> packet;
        cmd.encode(packet);
        return validate(packet);
    }

    int validate(std::vector<uint8_t>& packet) {
        return ValidatorTest::validate(
            PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION, packet.data());
    }

    /*
     * Tests the request with both the pathFlag and docFlag specified. It does
     * not test when both are used together
     */
    void testFlags(protocol_binary_subdoc_flag pathFlag,
                   mcbp::subdoc::doc_flag docFlag,
                   protocol_binary_response_status expected,
                   uint8_t spec) {
        request.at(spec).flags = pathFlag;
        EXPECT_EQ(expected, validate(request));
        request.at(spec).flags = SUBDOC_FLAG_NONE;

        request.addDocFlag(docFlag);
        EXPECT_EQ(expected, validate(request));
        request.clearDocFlags();
    }

    /*
     * Tests the request with both the pathFlag and docFlag specified. It does
     * not test when both are used together
     */
    void testFlagCombo(protocol_binary_subdoc_flag pathFlag,
                       mcbp::subdoc::doc_flag docFlag,
                       protocol_binary_response_status expected,
                       uint8_t spec) {
        request.at(spec).flags = pathFlag;
        EXPECT_EQ(expected, validate(request));
        request.addDocFlag(docFlag);
        EXPECT_EQ(expected, validate(request));
        request.at(spec).flags = SUBDOC_FLAG_NONE;
        EXPECT_EQ(expected, validate(request));
    }

    BinprotSubdocMultiMutationCommand request;
};

TEST_P(SubdocMultiMutationTest, Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidMagic) {
    std::vector<uint8_t> payload;
    request.encode(payload);
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_P(SubdocMultiMutationTest, InvalidDatatype) {
    std::vector<uint8_t> payload;
    request.encode(payload);
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
    header->request.datatype = (PROTOCOL_BINARY_DATATYPE_SNAPPY |
                                PROTOCOL_BINARY_DATATYPE_JSON);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_SNAPPY;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_P(SubdocMultiMutationTest, InvalidKey) {
    request.setKey("");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidExtras) {
    std::vector<uint8_t> payload;
    request.encode(payload);
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.extlen = 2;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_P(SubdocMultiMutationTest, Expiry) {
    // extlen of 4 permitted for mutations.
    request.setExpiry(10);
    std::vector<uint8_t> payload;
    request.encode(payload);

    // Check that we encoded correctly.
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    ASSERT_EQ(4, header->request.extlen);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(payload));
}

TEST_P(SubdocMultiMutationTest, ExplicitZeroExpiry) {
    // extlen of 4 permitted for mutations.
    request.setExpiry(0);
    std::vector<uint8_t> payload;
    request.encode(payload);

    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    ASSERT_EQ(4, header->request.extlen);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(payload));
}

TEST_P(SubdocMultiMutationTest, NumPaths) {
    // Need at least one path.
    request.clearMutations();
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO, validate(request));

    // Should handle total of 16 paths.
    request.clearMutations();
    // Add maximum number of paths.
    BinprotSubdocMultiMutationCommand::MutationSpecifier spec{
        PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
        protocol_binary_subdoc_flag(0),
        "",
        "0"};
    for (unsigned int i = 0; i < 16; i++) {
        request.addMutation(spec);
    }
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));

    // Add one more - should now fail.
    request.addMutation(spec);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
              validate(request));
}

TEST_P(SubdocMultiMutationTest, ValidDictAdd) {
    // Only allowed empty flags or
    // SUBDOC_FLAG_MKDIR_P/mcbp::subdoc::doc_flag::Mkdoc
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  mcbp::subdoc::doc_flag::Mkdoc,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  1);
}

TEST_P(SubdocMultiMutationTest, InvalidDictAdd) {
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                         protocol_binary_subdoc_flag(0xff),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have path.
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have value.
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, ValidDictUpsert) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P(0x01)/MKDOC(0x02)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  mcbp::subdoc::doc_flag::Mkdoc,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  1);
}

TEST_P(SubdocMultiMutationTest, InvalidDictUpsert) {
    // Only allowed empty flags SUBDOC_FLAG_{MKDIR_P (0x1), MKDOC (0x2)}
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                         protocol_binary_subdoc_flag(0xff),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have path.
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have value.
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
}

TEST_P(SubdocMultiMutationTest, ValidDelete) {
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         ""});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidDelete) {
    // Shouldn't have value.
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Shouldn't have flags.
    testFlags(SUBDOC_FLAG_MKDIR_P,
              mcbp::subdoc::doc_flag::Mkdoc,
              PROTOCOL_BINARY_RESPONSE_EINVAL,
              1);

    // Must have path.
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, ValidReplace) {
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "new_value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidReplace) {
    // Must have path.
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                         protocol_binary_subdoc_flag(0),
                         "",
                         "new_value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have value
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Shouldn't have flags.
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                     SUBDOC_FLAG_NONE,
                     "path",
                     "new_value"};
    testFlags(SUBDOC_FLAG_MKDIR_P,
              mcbp::subdoc::doc_flag::Mkdoc,
              PROTOCOL_BINARY_RESPONSE_EINVAL,
              1);
}

TEST_P(SubdocMultiMutationTest, ValidArrayPushLast) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));

    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  mcbp::subdoc::doc_flag::Mkdoc,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  1);
    // Allowed empty path.
    request.at(1).path.clear();
    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  mcbp::subdoc::doc_flag::Mkdoc,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  1);
}

TEST_P(SubdocMultiMutationTest, InvalidArrayPushLast) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                         protocol_binary_subdoc_flag(0xff),
                         "",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have value
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, ValidArrayPushFirst) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));

    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  mcbp::subdoc::doc_flag::Mkdoc,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  1);
    // Allowed empty path.
    request.at(1).path.clear();
    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  mcbp::subdoc::doc_flag::Mkdoc,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  1);
}

TEST_P(SubdocMultiMutationTest, InvalidArrayPushFirst) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                         protocol_binary_subdoc_flag(0xff),
                         "",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have value
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, ValidArrayInsert) {
    // Only allowed empty flags.
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidArrayInsert) {
    // Only allowed empty flags.
    for (size_t i = 0; i < 2; i++) {
        request.addMutation({});
    }

    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                     SUBDOC_FLAG_NONE,
                     "path",
                     "value"};
    testFlags(SUBDOC_FLAG_MKDIR_P,
              mcbp::subdoc::doc_flag::Mkdoc,
              PROTOCOL_BINARY_RESPONSE_EINVAL,
              1);

    // Must have path
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                     protocol_binary_subdoc_flag(0),
                     "",
                     "value"};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have value
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, ValidArrayAddUnique) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));

    testFlags(SUBDOC_FLAG_MKDIR_P,
              mcbp::subdoc::doc_flag::Mkdoc,
              PROTOCOL_BINARY_RESPONSE_SUCCESS,
              1);

    // Allowed empty path.
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                     protocol_binary_subdoc_flag(0),
                     "",
                     "value"};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidArrayAddUnique) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                         protocol_binary_subdoc_flag(0xff),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have value
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, ValidArrayCounter) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));

    testFlags(SUBDOC_FLAG_MKDIR_P,
              mcbp::subdoc::doc_flag::Mkdoc,
              PROTOCOL_BINARY_RESPONSE_SUCCESS,
              1);

    // Empty path invalid
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                     protocol_binary_subdoc_flag(0),
                     "",
                     "value"};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidArrayCounter) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                         protocol_binary_subdoc_flag(0xff),
                         "path",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));

    // Must have value
    request.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidLocationOpcodes) {
    // Check that all opcodes apart from the mutation ones are not supported.
    for (uint8_t ii = 0; ii < std::numeric_limits<uint8_t>::max(); ii++) {
        auto cmd = protocol_binary_command(ii);
        // Skip over mutation opcodes.
        switch (cmd) {
        case PROTOCOL_BINARY_CMD_SET:
        case PROTOCOL_BINARY_CMD_DELETE:
        case PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD:
        case PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT:
        case PROTOCOL_BINARY_CMD_SUBDOC_DELETE:
        case PROTOCOL_BINARY_CMD_SUBDOC_REPLACE:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE:
        case PROTOCOL_BINARY_CMD_SUBDOC_COUNTER:
            continue;
        default:
            break;
        }

        request.at(0) = {cmd, protocol_binary_subdoc_flag(0), "[0]"};
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
                  validate(request))
                    << "Failed for cmd:" << memcached_opcode_2_text(ii);
    }
}

TEST_P(SubdocMultiMutationTest, InvalidCas) {
    // Check that a non 0 CAS is rejected
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    request.setCas(12234);
    request.addDocFlag(mcbp::subdoc::doc_flag::Add);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, WholeDocDeleteInvalidValue) {
    request.clearMutations();
    // Shouldn't have value.
    request.addMutation({PROTOCOL_BINARY_CMD_DELETE,
                         protocol_binary_subdoc_flag(0),
                         "",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, WholeDocDeleteInvalidPath) {
    request.clearMutations();
    // Must not have path.
    request.addMutation({PROTOCOL_BINARY_CMD_DELETE,
                         protocol_binary_subdoc_flag(0),
                         "_sync",
                         ""});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, WholeDocDeleteInvalidXattrFlag) {
    request.clearMutations();
    // Can't use CMD_DELETE to delete Xattr
    request.addMutation(
        {PROTOCOL_BINARY_CMD_DELETE, SUBDOC_FLAG_XATTR_PATH, "", ""});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request));
}

TEST_P(SubdocMultiMutationTest, ValidWholeDocDeleteFlags) {
    request.clearMutations();
    request.addMutation({PROTOCOL_BINARY_CMD_DELETE,
                         protocol_binary_subdoc_flag(0),
                         "",
                         ""});
    request.addDocFlag(mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request));
}

TEST_P(SubdocMultiMutationTest, InvalidWholeDocDeleteMulti) {
    // Doing a delete and another subdoc/wholedoc command on the body in
    // the same multi mutation is invalid
    // Note that the setup of this test adds an initial mutation
    request.addMutation({PROTOCOL_BINARY_CMD_DELETE,
                         protocol_binary_subdoc_flag(0),
                         "",
                         ""});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO, validate(request));

    // Now try the delete first
    request.clearMutations();
    request.addMutation({PROTOCOL_BINARY_CMD_DELETE,
                         protocol_binary_subdoc_flag(0),
                         "",
                         ""});
    request.addMutation({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                         protocol_binary_subdoc_flag(0),
                         "key",
                         "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO, validate(request));
}

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SubdocSingleTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SubdocMultiLookupTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SubdocMultiMutationTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

} // namespace test
} // namespace mcbp
