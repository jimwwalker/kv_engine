/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <protocol/connection/client_mcbp_commands.h>

#include <memcached/range_scan_id.h>
#include <platform/base64.h>

class RangeScanTest : public TestappXattrClientTest {
public:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
        start = Couchbase::Base64::encode("user");
        end = Couchbase::Base64::encode("user\xFF");

        config = {{"range", {{"start", start}, {"end", end}}}};
    }

    std::string start;
    std::string end;
    nlohmann::json config;
};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        RangeScanTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No)),
        PrintToStringCombinedName());

TEST_P(RangeScanTest, CreateInvalid) {
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::RangeScanCreate);
    userConnection->sendCommand(cmd);
    BinprotResponse resp;
    userConnection->recvResponse(resp);
    // No value, so invalid
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // Not JSON
    cmd.setValue("...");
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    userConnection->sendCommand(cmd);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // JSON but no datatype
    cmd.setValue(config.dump());
    cmd.setDatatype(cb::mcbp::Datatype::Raw);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(RangeScanTest, CreateCancel) {
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::RangeScanCreate);
    cmd.setValue(config.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    userConnection->sendCommand(cmd);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());

    BinprotGenericCommand scanCancel(cb::mcbp::ClientOpcode::RangeScanCancel);
    scanCancel.setExtras(std::string_view{
            reinterpret_cast<const char*>(id.data), sizeof(id)});
    userConnection->sendCommand(scanCancel);
    userConnection->recvResponse(resp);

    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    userConnection->sendCommand(scanCancel);
    userConnection->recvResponse(resp);

    // And gone
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());
}