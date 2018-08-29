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

#include <event2/event.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <gsl/gsl>
#include <memory>

/**
 * Test all of the command validators we've got to ensure that they
 * catch broken packets. There is still a high number of commands we
 * don't have any command validators for...
 */
namespace mcbp {
namespace test {

ValidatorTest::ValidatorTest(bool collectionsEnabled)
    : request(*reinterpret_cast<protocol_binary_request_no_extras*>(blob)),
      collectionsEnabled(collectionsEnabled) {
}

void ValidatorTest::SetUp() {
    settings.setXattrEnabled(true);
    connection.setCollectionsSupported(collectionsEnabled);
    McbpValidatorChains::initializeMcbpValidatorChains(validatorChains);
    memset(request.bytes, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
}

/**
 * Mock the cookie class and override the getPacket method so that we
 * may use the buffer directly instead of having to insert it into the read/
 * write buffers of the underlying connection
 */
class MockCookie : public Cookie {
public:
    MockCookie(Connection& connection, cb::const_byte_buffer buffer)
        : Cookie(connection) {
        setPacket(PacketContent::Full, buffer);
    }
};

protocol_binary_response_status
ValidatorTest::validate(protocol_binary_command opcode, void* packet) {
    // Mockup a McbpConnection and Cookie for the validator chain
    connection.enableDatatype(cb::mcbp::Feature::XATTR);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    MockCookie cookie(connection, buffer);
    return validatorChains.invoke(opcode, cookie);
}

std::string ValidatorTest::validate_error_context(
        protocol_binary_command opcode, void* packet) {
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    MockCookie cookie(connection, buffer);
    validatorChains.invoke(opcode, cookie);
    return cookie.getErrorContext();
}

enum class GetOpcodes : uint8_t {
    Get = PROTOCOL_BINARY_CMD_GET,
    GetQ = PROTOCOL_BINARY_CMD_GETQ,
    GetK = PROTOCOL_BINARY_CMD_GETK,
    GetKQ = PROTOCOL_BINARY_CMD_GETKQ,
    GetMeta = PROTOCOL_BINARY_CMD_GET_META,
    GetQMeta = PROTOCOL_BINARY_CMD_GETQ_META
};

std::string to_string(const GetOpcodes& opcode) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(opcode));
#else
    switch (opcode) {
    case GetOpcodes::Get:
        return "Get";
    case GetOpcodes::GetQ:
        return "GetQ";
    case GetOpcodes::GetK:
        return "GetK";
    case GetOpcodes::GetKQ:
        return "GetKQ";
    case GetOpcodes::GetMeta:
        return "GetMeta";
    case GetOpcodes::GetQMeta:
        return "GetQMeta";
    }
    throw std::invalid_argument("to_string(): unknown opcode");
#endif
}

std::ostream& operator<<(std::ostream& os, const GetOpcodes& o) {
    os << to_string(o);
    return os;
}

// Test the validators for GET, GETQ, GETK, GETKQ, GET_META and GETQ_META
class GetValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<std::tuple<GetOpcodes, bool>> {
public:
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 0;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

    GetValidatorTest()
        : ValidatorTest(std::get<1>(GetParam())),
          bodylen(request.message.header.request.bodylen) {
        // empty
    }

protected:

    protocol_binary_response_status validateExtendedExtlen(uint8_t version) {
        bodylen = htonl(ntohl(bodylen) + 1);
        request.message.header.request.extlen = 1;
        blob[sizeof(protocol_binary_request_get)] = version;
        return validate();
    }

    protocol_binary_response_status validate() {
        auto opcode = (protocol_binary_command)std::get<0>(GetParam());
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }

    uint32_t& bodylen;
};

TEST_P(GetValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, ExtendedExtlenV1) {
    switch (std::get<0>(GetParam())) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validateExtendedExtlen(1));
        break;
    }
}

TEST_P(GetValidatorTest, ExtendedExtlenV2) {
    switch (std::get<0>(GetParam())) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validateExtendedExtlen(2));
        break;
    }
}

TEST_P(GetValidatorTest, InvalidExtendedExtlenVersion) {
    switch (std::get<0>(GetParam())) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validateExtendedExtlen(3));
        break;
    }
}

TEST_P(GetValidatorTest, InvalidExtlen) {
    bodylen = htonl(ntohl(bodylen) + 21);
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen =
            std::get<1>(GetParam()) ? htons(1) : 0;
    bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, InvalidKey) {
    if (!std::get<1>(GetParam())) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, illegal.
    std::fill(blob + sizeof(request.bytes),
              blob + sizeof(request.bytes) + 10,
              0x81ull);
    request.message.header.request.keylen = htons(10);
    bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}


// @todo add test case for the extra legal modes for the
// get meta case

INSTANTIATE_TEST_CASE_P(
        GetOpcodes,
        GetValidatorTest,
        ::testing::Combine(::testing::Values(GetOpcodes::Get,
                                             GetOpcodes::GetQ,
                                             GetOpcodes::GetK,
                                             GetOpcodes::GetKQ,
                                             GetOpcodes::GetMeta,
                                             GetOpcodes::GetQMeta),
                           ::testing::Bool()), );

// Test ADD & ADDQ
class AddValidatorTest : public ValidatorTest,
                         public ::testing::WithParamInterface<bool> {
public:
    AddValidatorTest() : ValidatorTest(GetParam()) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(AddValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_P(AddValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_P(AddValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_P(AddValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_P(AddValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_P(AddValidatorTest, InvalidKey) {
    if (!GetParam()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, illegal.
    auto fill = blob + request.message.header.request.extlen;
    std::fill(fill + sizeof(request.bytes),
              fill + sizeof(request.bytes) + 10,
              0x80ull);
    request.message.header.request.keylen = htons(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_P(AddValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

// Test SET, SETQ, REPLACE, REPLACEQ
class SetReplaceValidatorTest : public ValidatorTest,
                                public ::testing::WithParamInterface<bool> {
public:
    SetReplaceValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(SetReplaceValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_P(SetReplaceValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_P(SetReplaceValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_P(SetReplaceValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_P(SetReplaceValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_P(SetReplaceValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_P(SetReplaceValidatorTest, InvalidKey) {
    if (!GetParam()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, illegal.
    std::fill(blob + sizeof(request.bytes),
              blob + sizeof(request.bytes) + 10,
              0x81ull);
    request.message.header.request.keylen = htons(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

// Test Append[q] and Prepend[q]
class AppendPrependValidatorTest : public ValidatorTest,
                                   public ::testing::WithParamInterface<bool> {
public:
    AppendPrependValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(AppendPrependValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_P(AppendPrependValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_P(AppendPrependValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_P(AppendPrependValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_P(AppendPrependValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_P(AppendPrependValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

// Test DELETE & DELETEQ
class DeleteValidatorTest : public ValidatorTest,
                            public ::testing::WithParamInterface<bool> {
public:
    DeleteValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();

        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(DeleteValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_P(DeleteValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_P(DeleteValidatorTest, WithValue) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_P(DeleteValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_P(DeleteValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_P(DeleteValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_P(DeleteValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

// Test INCREMENT[q] and DECREMENT[q]
class IncrementDecrementValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<bool> {
public:
    IncrementDecrementValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 20;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(30);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(IncrementDecrementValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_P(IncrementDecrementValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_P(IncrementDecrementValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_P(IncrementDecrementValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_P(IncrementDecrementValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_P(IncrementDecrementValidatorTest, WithValue) {
    request.message.header.request.bodylen = htonl(40);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_P(IncrementDecrementValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

// Test QUIT & QUITQ
class QuitValidatorTest : public ValidatorTest,
                          public ::testing::WithParamInterface<bool> {
public:
    QuitValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(QuitValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_P(QuitValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_P(QuitValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_P(QuitValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = ntohl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_P(QuitValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_P(QuitValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_P(QuitValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

// Test FLUSH & FLUSHQ
class FlushValidatorTest : public ValidatorTest,
                           public ::testing::WithParamInterface<bool> {
public:
    FlushValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(FlushValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_P(FlushValidatorTest, CorrectMessageWithTime) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_P(FlushValidatorTest, CorrectMessageWithUnsupportedTime) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    *reinterpret_cast<uint32_t*>(request.bytes + sizeof(request.bytes)) = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_P(FlushValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_P(FlushValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_P(FlushValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = ntohl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_P(FlushValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_P(FlushValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_P(FlushValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

// test Noop
class NoopValidatorTest : public ValidatorTest,
                          public ::testing::WithParamInterface<bool> {
public:
    NoopValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_NOOP,
                                       static_cast<void*>(&request));
    }
};

TEST_P(NoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(NoopValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(NoopValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(NoopValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = ntohs(32);
    request.message.header.request.bodylen = htonl(32);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(NoopValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(NoopValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(NoopValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test version
class VersionValidatorTest : public ValidatorTest,
                             public ::testing::WithParamInterface<bool> {
public:
    VersionValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_VERSION,
                                       static_cast<void*>(&request));
    }
};

TEST_P(VersionValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(VersionValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VersionValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VersionValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = ntohs(32);
    request.message.header.request.bodylen = htonl(32);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VersionValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VersionValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VersionValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test stat
class StatValidatorTest : public ValidatorTest,
                          public ::testing::WithParamInterface<bool> {
public:
    StatValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_STAT,
                                       static_cast<void*>(&request));
    }
};

TEST_P(StatValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(StatValidatorTest, WithKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(StatValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(StatValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(StatValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(StatValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(StatValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test verbosity
class VerbosityValidatorTest : public ValidatorTest,
                               public ::testing::WithParamInterface<bool> {
public:
    VerbosityValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_VERBOSITY,
                                       static_cast<void*>(&request));
    }
};

TEST_P(VerbosityValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(VerbosityValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VerbosityValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VerbosityValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VerbosityValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VerbosityValidatorTest, InvalidKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(VerbosityValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test HELLO
class HelloValidatorTest : public ValidatorTest,
                           public ::testing::WithParamInterface<bool> {
public:
    HelloValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_HELLO,
                                       static_cast<void*>(&request));
    }
};

TEST_P(HelloValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(HelloValidatorTest, MultipleFeatures) {
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
    request.message.header.request.bodylen = htonl(6);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(HelloValidatorTest, WithKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(HelloValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(HelloValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(HelloValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(HelloValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(HelloValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test SASL_LIST_MECHS
class SaslListMechValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
    SaslListMechValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SASL_LIST_MECHS,
                                       static_cast<void*>(&request));
    }
};

TEST_P(SaslListMechValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test SASL_AUTH
class SaslAuthValidatorTest : public ValidatorTest,
                              public ::testing::WithParamInterface<bool> {
public:
    SaslAuthValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(SaslAuthValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_P(SaslAuthValidatorTest, WithChallenge) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_P(SaslAuthValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_P(SaslAuthValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_P(SaslAuthValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_P(SaslAuthValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_P(SaslAuthValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

class GetErrmapValidatorTest : public ValidatorTest,
                               public ::testing::WithParamInterface<bool> {
public:
    GetErrmapValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ERROR_MAP,
                                       static_cast<void*>(&request));
    }
};

TEST_P(GetErrmapValidatorTest, CorrectMessage) {
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetErrmapValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetErrmapValidatorTest, MissingBody) {
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test IOCTL_GET
class IoctlGetValidatorTest : public ValidatorTest,
                              public ::testing::WithParamInterface<bool> {
public:
    IoctlGetValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    // #defined in memcached.h..
    const int IOCTL_KEY_LENGTH = 128;

    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_IOCTL_GET,
                                       static_cast<void*>(&request));
    }
};

TEST_P(IoctlGetValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    request.message.header.request.keylen = htons(IOCTL_KEY_LENGTH + 1);
    request.message.header.request.bodylen = htonl(IOCTL_KEY_LENGTH + 1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test IOCTL_SET
class IoctlSetValidatorTest : public ValidatorTest,
                              public ::testing::WithParamInterface<bool> {
public:
    IoctlSetValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    // #defined in memcached.h..
    const int IOCTL_KEY_LENGTH = 128;
    const int IOCTL_VAL_LENGTH = 128;

    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_IOCTL_SET,
                                       static_cast<void*>(&request));
    }
};

TEST_P(IoctlSetValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    request.message.header.request.keylen = htons(IOCTL_KEY_LENGTH + 1);
    request.message.header.request.bodylen = htonl(IOCTL_KEY_LENGTH + 1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(IOCTL_VAL_LENGTH + 11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(IoctlSetValidatorTest, ValidBody) {
    request.message.header.request.bodylen = htonl(IOCTL_VAL_LENGTH + 10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

// test AUDIT_PUT
class AuditPutValidatorTest : public ValidatorTest,
                              public ::testing::WithParamInterface<bool> {
public:
    AuditPutValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_AUDIT_PUT,
                                       static_cast<void*>(&request));
    }
};

TEST_P(AuditPutValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(AuditPutValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditPutValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditPutValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(15);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditPutValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditPutValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditPutValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test audit_config_reload
class AuditConfigReloadValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<bool> {
public:
    AuditConfigReloadValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                                       static_cast<void*>(&request));
    }
};

TEST_P(AuditConfigReloadValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test shutdown
class ShutdownValidatorTest : public ValidatorTest,
                              public ::testing::WithParamInterface<bool> {
public:
    ShutdownValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.cas = 1;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SHUTDOWN,
                                       static_cast<void*>(&request));
    }
};

TEST_P(ShutdownValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(ShutdownValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ShutdownValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ShutdownValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ShutdownValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ShutdownValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ShutdownValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpOpenValidatorTest : public ValidatorTest,
                             public ::testing::WithParamInterface<bool> {
public:
    DcpOpenValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(2);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_OPEN,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_dcp_open &request = *reinterpret_cast<protocol_binary_request_dcp_open*>(blob);
};

TEST_P(DcpOpenValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 9;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpOpenValidatorTest, ValueButNoCollections) {
    // Only valid when collections disabled
    if (GetParam()) {
        return;
    }
    request.message.header.request.bodylen = htonl(10 + 20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpOpenValidatorTest, CorrectMessageValueCollections) {
    // Only valid when collections enabled
    if (!GetParam()) {
        return;
    }
    request.message.header.request.bodylen = htonl(10 + 20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

class DcpAddStreamValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
    DcpAddStreamValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_ADD_STREAM,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpAddStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpCloseStreamValidatorTest : public ValidatorTest,
                                    public ::testing::WithParamInterface<bool> {
public:
    DcpCloseStreamValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpCloseStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpGetFailoverLogValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<bool> {
public:
    DcpGetFailoverLogValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpGetFailoverLogValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpStreamReqValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
    DcpStreamReqValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 48;
        request.message.header.request.bodylen = htonl(48);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpStreamReqValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(54);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
// Can the stream req also conain data?
// TEST_P(DcpStreamReqValidatorTest, InvalidBody) {
//     request.message.header.request.bodylen = htonl(12);
//     EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
// }

class DcpStreamEndValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
    DcpStreamEndValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_STREAM_END,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpStreamEndValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpSnapshotMarkerValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<bool> {
public:
    DcpSnapshotMarkerValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 20;
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpSnapshotMarkerValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 32;
    request.message.header.request.bodylen = htonl(52);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

/**
 * Test class for DcpMutation validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a mutation)
 */
class DcpMutationValidatorTest : public ValidatorTest,
                                 public ::testing::WithParamInterface<bool> {
public:
public:
    DcpMutationValidatorTest()
        : ValidatorTest(GetParam()),
          request(0 /*opaque*/,
                  0 /*vbucket*/,
                  0 /*cas*/,
                  GetParam() ? 2 : 1 /*keylen*/,
                  0 /*valueLen*/,
                  PROTOCOL_BINARY_RAW_BYTES,
                  0 /*bySeqno*/,
                  0 /*revSeqno*/,
                  0 /*flags*/,
                  0 /*expiration*/,
                  0 /*lockTime*/,
                  0 /*nmeta*/,
                  0 /*nru*/) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
    }

protected:
    protocol_binary_response_status validate() {
        std::copy(request.bytes, request.bytes + sizeof(request.bytes), blob);
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_MUTATION,
                                       static_cast<void*>(blob));
    }

    protocol_binary_request_dcp_mutation request;
};


TEST_P(DcpMutationValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(22);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidExtlenCollections) {
    request.message.header.request.extlen =
            protocol_binary_request_dcp_mutation::getExtrasLength() + 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    request.message.header.request.bodylen = htonl(31);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// A key which has no leb128 stop-byte
TEST_P(DcpMutationValidatorTest, InvalidKey1) {
    if (GetParam()) {
        std::fill(blob + sizeof(request.bytes),
                  blob + sizeof(request.bytes) + 10,
                  0x81ull);
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen =
                htonl(request.message.header.request.extlen + 10);
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    }
}

// A key which has a stop-byte, but no data after that
TEST_P(DcpMutationValidatorTest, InvalidKey2) {
    if (GetParam()) {
        std::fill(blob + sizeof(request.bytes),
                  blob + sizeof(request.bytes) + 9,
                  0x81ull);
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen =
                htonl(request.message.header.request.extlen + 10);
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    }
}

/**
 * Test class for DcpDeletion validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a deletion)
 */
class DcpDeletionValidatorTest : public ValidatorTest,
                                 public ::testing::WithParamInterface<bool> {
public:
public:
    DcpDeletionValidatorTest()
        : ValidatorTest(GetParam()),
          request(GetParam() ? makeV2() : makeV1()),
          header(request->getHeader()) {
        header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_DELETION;
        if (GetParam()) {
            header.request.keylen = htons(5); // min-collection key
            header.request.bodylen = htonl(header.request.extlen + 5);
        }
    }

    void SetUp() override {
        ValidatorTest::SetUp();
    }

protected:
    protocol_binary_response_status validate() {
        std::copy(request->getBytes(),
                  request->getBytes() + request->getSizeofBytes(),
                  blob);
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_DELETION,
                                       static_cast<void*>(blob));
    }

    class Request {
    public:
        virtual ~Request() = default;
        virtual protocol_binary_request_header& getHeader() = 0;

        virtual uint8_t* getBytes() = 0;

        virtual size_t getSizeofBytes() = 0;
    };

    class RequestV1 : public Request {
    public:
        RequestV1()
            : request(0 /*opaque*/,
                      0 /*vbucket*/,
                      0 /*cas*/,
                      2 /*keylen*/,
                      0 /*valueLen*/,
                      PROTOCOL_BINARY_RAW_BYTES,
                      0 /*bySeqno*/,
                      0 /*revSeqno*/,
                      0 /*nmeta*/) {
        }
        protocol_binary_request_header& getHeader() override {
            return request.message.header;
        }

        uint8_t* getBytes() override {
            return request.bytes;
        }

        size_t getSizeofBytes() override {
            return sizeof(request.bytes);
        }

    private:
        protocol_binary_request_dcp_deletion request;
    };

    class RequestV2 : public Request {
    public:
        RequestV2()
            : request(0 /*opaque*/,
                      0 /*vbucket*/,
                      0 /*cas*/,
                      2 /*keylen*/,
                      0 /*valueLen*/,
                      PROTOCOL_BINARY_RAW_BYTES,
                      0 /*bySeqno*/,
                      0 /*revSeqno*/,
                      0, /*deleteTime*/
                      0 /*collectionLen*/) {
        }
        protocol_binary_request_header& getHeader() override {
            return request.message.header;
        }

        uint8_t* getBytes() override {
            return request.bytes;
        }

        size_t getSizeofBytes() override {
            return sizeof(request.bytes);
        }

    private:
        protocol_binary_request_dcp_deletion_v2 request;
    };

    std::unique_ptr<Request> makeV1() {
        return std::make_unique<RequestV1>();
    }

    std::unique_ptr<Request> makeV2() {
        return std::make_unique<RequestV2>();
    }

    std::unique_ptr<Request> request;
    protocol_binary_request_header& header;
};


TEST_P(DcpDeletionValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidMagic) {
    header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpDeletionValidatorTest, ValidDatatype) {
    using cb::mcbp::Datatype;
    const std::array<uint8_t, 3> datatypes = {
            {uint8_t(Datatype::Raw),
             uint8_t(Datatype::Xattr),
             uint8_t(Datatype::Xattr) | uint8_t(Datatype::Snappy)}};

    for (auto valid : datatypes) {
        header.request.datatype = valid;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate())
                    << "Testing valid datatype:" << int(valid);
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidDatatype) {
    using cb::mcbp::Datatype;
    const std::array<uint8_t, 3> datatypes = {
            {uint8_t(Datatype::JSON),
             uint8_t(Datatype::Snappy),
             uint8_t(Datatype::Snappy) | uint8_t(Datatype::JSON)}};

    for (auto invalid : datatypes) {
        header.request.datatype = invalid;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate())
                    << "Testing invalid datatype:" << int(invalid);
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidExtlen) {
    header.request.extlen = 5;
    header.request.bodylen = htonl(7);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidExtlenCollections) {
    // Flip extlen, so when not collections, set the length collections uses
    header.request.extlen =
            GetParam() ? protocol_binary_request_dcp_deletion::extlen
                       : protocol_binary_request_dcp_deletion_v2::extlen;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidKeylen) {
    header.request.keylen = GetParam() ? htons(1) : 0;
    header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpDeletionValidatorTest, WithValue) {
    header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

/**
 * Test class for DcpExpiration validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of an expiration)
 */
class DcpExpirationValidatorTest : public ValidatorTest,
                                   public ::testing::WithParamInterface<bool> {
public:
public:
    DcpExpirationValidatorTest()
        : ValidatorTest(GetParam()),
          request(0 /*opaque*/,
                  0 /*vbucket*/,
                  0 /*cas*/,
                  GetParam() ? 5 : 1 /*keylen*/,
                  0 /*valueLen*/,
                  PROTOCOL_BINARY_RAW_BYTES,
                  0 /*bySeqno*/,
                  0 /*revSeqno*/,
                  0 /*nmeta*/) {
        request.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_EXPIRATION;
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        connection.setCollectionsSupported(GetParam());
    }

protected:
    protocol_binary_response_status validate() {
        std::copy(request.bytes, request.bytes + sizeof(request.bytes), blob);
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_EXPIRATION,
                                       static_cast<void*>(blob));
    }

    protocol_binary_request_dcp_expiration request;
};

TEST_P(DcpExpirationValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(7);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpSetVbucketStateValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<bool> {
public:
    DcpSetVbucketStateValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 1;
        request.message.header.request.bodylen = htonl(1);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.body.state = 1;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(
            PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE,
            static_cast<void*>(&request));
    }

    protocol_binary_request_dcp_set_vbucket_state &request =
       *reinterpret_cast<protocol_binary_request_dcp_set_vbucket_state*>(blob);
};

TEST_P(DcpSetVbucketStateValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, LegalValues) {
    for (int ii = 1; ii < 5; ++ii) {
        request.message.body.state = ii;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
    }
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, IllegalValues) {
    request.message.body.state = 5;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    request.message.body.state = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpNoopValidatorTest : public ValidatorTest,
                             public ::testing::WithParamInterface<bool> {
public:
    DcpNoopValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_NOOP,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpNoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpBufferAckValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
    DcpBufferAckValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(
            PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
            static_cast<void*>(&request));
    }
};

TEST_P(DcpBufferAckValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpControlValidatorTest : public ValidatorTest,
                                public ::testing::WithParamInterface<bool> {
public:
    DcpControlValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(4);
        request.message.header.request.bodylen = htonl(8);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpControlValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpControlValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpControlValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(13);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpControlValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpControlValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpControlValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test observe seqno
class ObserveSeqnoValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
    ObserveSeqnoValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.bodylen = ntohl(8);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_OBSERVE_SEQNO,
                                       static_cast<void*>(&request));
    }
};

TEST_P(ObserveSeqnoValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 8;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test set drift counter state
class SetDriftCounterStateValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<bool> {
public:
    SetDriftCounterStateValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 9;
        request.message.header.request.bodylen = ntohl(9);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(
            PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE,
            static_cast<void*>(&request));
    }
};

TEST_P(SetDriftCounterStateValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(19);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test get adjusted time
class GetAdjustedTimeValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<bool> {
public:
    GetAdjustedTimeValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME,
                                       static_cast<void*>(&request));
    }
};

TEST_P(GetAdjustedTimeValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

enum class RefreshOpcodes : uint8_t {
    Isasl = uint8_t(PROTOCOL_BINARY_CMD_ISASL_REFRESH),
    Ssl = uint8_t(PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH),
    Rbac = uint8_t(PROTOCOL_BINARY_CMD_RBAC_REFRESH)
};

std::string to_string(const RefreshOpcodes& opcode) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(opcode));
#else
    switch (opcode) {
    case RefreshOpcodes::Isasl:
        return "ISASL";
    case RefreshOpcodes::Ssl:
        return "SSL";
    case RefreshOpcodes::Rbac:
        return "RBAC";
    }
    throw std::invalid_argument("to_string(const RefreshOpcodes&): unknown opcode");
#endif
}

std::ostream& operator<<(std::ostream& os, const RefreshOpcodes& o) {
    os << to_string(o);
    return os;
}

class RefreshValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<std::tuple<RefreshOpcodes, bool>> {
public:
    RefreshValidatorTest() : ValidatorTest(std::get<1>(GetParam())) {
    }

protected:
    protocol_binary_response_status validate() {
        auto opcode = (protocol_binary_command)std::get<0>(GetParam());
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

INSTANTIATE_TEST_CASE_P(
        RefreshOpcodes,
        RefreshValidatorTest,
        ::testing::Combine(::testing::Values(RefreshOpcodes::Isasl,
                                             RefreshOpcodes::Ssl,
                                             RefreshOpcodes::Rbac),
                           ::testing::Bool()),

);

TEST_P(RefreshValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(RefreshValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test CmdTimer
class CmdTimerValidatorTest : public ValidatorTest,
                              public ::testing::WithParamInterface<bool> {
public:
    CmdTimerValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 1;
        request.message.header.request.bodylen = htonl(1);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_CMD_TIMER,
                                       static_cast<void*>(&request));
    }
};

TEST_P(CmdTimerValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test GetCtrlToken
class GetCtrlTokenValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
    GetCtrlTokenValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN,
                                       static_cast<void*>(&request));
    }
};

TEST_P(GetCtrlTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test SetCtrlToken
class SetCtrlTokenValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
    SetCtrlTokenValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.bodylen = htonl(8);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.body.new_cas = 1;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_set_ctrl_token &request =
        *reinterpret_cast<protocol_binary_request_set_ctrl_token*>(blob);
};

TEST_P(SetCtrlTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(SetCtrlTokenValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidNewCas) {
    request.message.body.new_cas = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS
class GetAllVbSeqnoValidatorTest : public ValidatorTest,
                                   public ::testing::WithParamInterface<bool> {
public:
    GetAllVbSeqnoValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_get_all_vb_seqnos &request =
        *reinterpret_cast<protocol_binary_request_get_all_vb_seqnos*>(blob);
};

TEST_P(GetAllVbSeqnoValidatorTest, CorrectMessageNoState) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, CorrectMessageWithState) {
    EXPECT_EQ(4, sizeof(vbucket_state_t));
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    request.message.body.state =
        static_cast<vbucket_state_t>(htonl(vbucket_state_active));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidVbucketState) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);

    for (int ii = 0; ii < 100; ++ii) {
        request.message.body.state = static_cast<vbucket_state_t>(htonl(ii));
        if (is_valid_vbucket_state_t(static_cast<vbucket_state_t>(ii))) {
            EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
        } else {
            EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
        }
    }
}

// PROTOCOL_BINARY_CMD_GET_LOCKED
class GetLockedValidatorTest : public ValidatorTest,
                               public ::testing::WithParamInterface<bool> {
public:
    GetLockedValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_LOCKED,
                                       static_cast<void*>(&request));
    }
};

TEST_P(GetLockedValidatorTest, CorrectMessageDefaultTimeout) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetLockedValidatorTest, CorrectMessageExplicitTimeout) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(14);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetLockedValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetLockedValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetLockedValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetLockedValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetLockedValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetLockedValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetLockedValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// PROTOCOL_BINARY_CMD_UNLOCK
class UnlockValidatorTest : public ValidatorTest,
                            public ::testing::WithParamInterface<bool> {
public:
    UnlockValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.cas = 0xdeadbeef;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_UNLOCK_KEY,
                                       static_cast<void*>(&request));
    }
};

TEST_P(UnlockValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(UnlockValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(UnlockValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(UnlockValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(UnlockValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(UnlockValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(UnlockValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(UnlockValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test config_reload
class ConfigReloadValidatorTest : public ValidatorTest,
                                  public ::testing::WithParamInterface<bool> {
public:
public:
    ConfigReloadValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                       static_cast<void*>(&request));
    }
};

TEST_P(ConfigReloadValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// PROTOCOL_BINARY_CMD_EVICT_KEY
class EvictKeyValidatorTest : public ValidatorTest,
                              public ::testing::WithParamInterface<bool> {
public:
    EvictKeyValidatorTest() : ValidatorTest(GetParam()) {
    }
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.cas = 0;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_EVICT_KEY,
                                       static_cast<void*>(&request));
    }
};

TEST_P(EvictKeyValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0xff;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class RevokeUserPermissionsValidatorTest
    : public ValidatorTest,
      public ::testing::WithParamInterface<bool> {
public:
    RevokeUserPermissionsValidatorTest() : ValidatorTest(GetParam()) {
    }
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.cas = 0;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_EVICT_KEY,
                                       static_cast<void*>(&request));
    }
};

TEST_P(RevokeUserPermissionsValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0xff;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, MissingKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class ErrorContextTest  : public ValidatorTest,
                          public ::testing::WithParamInterface<bool> {
public:
    ErrorContextTest() : ValidatorTest(GetParam()) {
    }

protected:
    std::string validate_error_context(protocol_binary_command opcode) {
        void* packet = static_cast<void*>(&request);
        return ValidatorTest::validate_error_context(opcode, packet);
    }
};

TEST_P(ErrorContextTest, ValidHeader) {
    // Error context should not be set on valid request
    EXPECT_EQ("", validate_error_context(PROTOCOL_BINARY_CMD_NOOP));
}

TEST_P(ErrorContextTest, InvalidHeader) {
    // Magic invalid
    request.message.header.request.magic = 0;
    EXPECT_EQ("Request header invalid",
              validate_error_context(PROTOCOL_BINARY_CMD_NOOP));

    // Extlen + Keylen > Bodylen
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.setExtlen(8);
    request.message.header.request.setKeylen(10);
    request.message.header.request.setBodylen(12);
    EXPECT_EQ("Request header invalid",
              validate_error_context(PROTOCOL_BINARY_CMD_ADD));
}

TEST_P(ErrorContextTest, InvalidDatatype) {
    // Nonexistent datatype
    request.message.header.request.datatype = mcbp::datatype::highest + 1;
    EXPECT_EQ("Request datatype invalid",
              validate_error_context(PROTOCOL_BINARY_CMD_NOOP));

    // Noop command does not accept JSON
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ("Request datatype invalid",
              validate_error_context(PROTOCOL_BINARY_CMD_NOOP));
}

TEST_P(ErrorContextTest, InvalidExtras) {
    // Noop command does not accept extras
    request.message.header.request.setExtlen(4);
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(4);
    EXPECT_EQ("Request must not include extras",
              validate_error_context(PROTOCOL_BINARY_CMD_NOOP));

    // Add command requires extras
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(10);
    request.message.header.request.setBodylen(14);
    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(PROTOCOL_BINARY_CMD_ADD));
}

TEST_P(ErrorContextTest, InvalidKey) {
    // Noop command does not accept key
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(8);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ("Request must not include key",
              validate_error_context(PROTOCOL_BINARY_CMD_NOOP));

    // Add command requires key
    request.message.header.request.setExtlen(8);
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ("Request must include key",
              validate_error_context(PROTOCOL_BINARY_CMD_ADD));
}

TEST_P(ErrorContextTest, InvalidValue) {
    // Noop command does not accept value
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ("Request must not include value",
              validate_error_context(PROTOCOL_BINARY_CMD_NOOP));

    // Create bucket command requires value
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(8);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ("Request must include value",
              validate_error_context(PROTOCOL_BINARY_CMD_CREATE_BUCKET));
}

TEST_P(ErrorContextTest, InvalidCas) {
    // Unlock command requires CAS
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(8);
    request.message.header.request.setBodylen(8);
    request.message.header.request.setCas(0);
    EXPECT_EQ("Request CAS must be set",
              validate_error_context(PROTOCOL_BINARY_CMD_UNLOCK_KEY));

    // Noop command does not accept CAS
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(0);
    request.message.header.request.setCas(10);
    EXPECT_EQ("Request CAS must not be set",
              validate_error_context(PROTOCOL_BINARY_CMD_NOOP));
}

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        AddValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SetReplaceValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        AppendPrependValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DeleteValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        IncrementDecrementValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        QuitValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        FlushValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        NoopValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        VersionValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        StatValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        VerbosityValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        HelloValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SaslListMechValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SaslAuthValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetErrmapValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        IoctlGetValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        IoctlSetValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        AuditPutValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        AuditConfigReloadValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ShutdownValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpOpenValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpAddStreamValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpCloseStreamValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpGetFailoverLogValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpStreamReqValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpStreamEndValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpSnapshotMarkerValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpMutationValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpDeletionValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpExpirationValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpSetVbucketStateValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpNoopValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpBufferAckValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpControlValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ObserveSeqnoValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SetDriftCounterStateValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetAdjustedTimeValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        CmdTimerValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetCtrlTokenValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SetCtrlTokenValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetAllVbSeqnoValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetLockedValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        UnlockValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ConfigReloadValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        EvictKeyValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        RevokeUserPermissionsValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ErrorContextTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

} // namespace test
} // namespace mcbp
