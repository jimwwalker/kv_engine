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

#include <xattr/blob.h>
#include <xattr/utils.h>

#include <mcbp/codec/range_scan_continue_codec.h>
#include <memcached/range_scan_id.h>
#include <platform/base64.h>

#include <unordered_set>

class RangeScanTest : public TestappXattrClientTest {
public:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
        mInfo = storeTestKeys();

        start = cb::base64::encode("user", false);
        end = cb::base64::encode("user\xFF", false);

        // Setup to scan for user prefixed docs. Utilise wait_for_seqno
        config = {{"range", {{"start", start}, {"end", end}}},
                  {"snapshot_requirements",
                   {{"seqno", mInfo.seqno},
                    {"vb_uuid", mInfo.vbucketuuid},
                    {"timeout_ms", 120000}}}};

        // if snappy evict so values comes from disk and we can validate snappy
        if (::testing::get<3>(GetParam()) == ClientSnappySupport::Yes) {
            adminConnection->executeInBucket(
                    bucketName, [this](auto& connection) {
                        for (const auto& key : userKeys) {
                            connection.evict(key, Vbid(0));
                        }
                    });
        }
    }

    std::unordered_set<std::string> userKeys = {"user-alan",
                                                "useralan",
                                                "user.claire",
                                                "user::zoe",
                                                "user:aaaaaaaa",
                                                "users"};
    std::vector<std::string> otherKeys = {
            "useq", "uses", "abcd", "uuu", "uuuu", "xyz"};

    MutationInfo storeTestKeys() {
        for (const auto& key : userKeys) {
            store_document(key, key);
        }

        for (const auto& key : otherKeys) {
            store_document(key, key);
        }

        Document doc;
        doc.value = "persist me";
        doc.info.id = "final";
        return userConnection->mutate(doc, Vbid(0), MutationType::Set);
    }

    MutationInfo mInfo;
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

TEST_P(RangeScanTest, KeyOnly) {
    config["key_only"] = true;

    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::RangeScanCreate);
    cmd.setValue(config.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    userConnection->sendCommand(cmd);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());

    // scan with a 2 item limit
    do {
        cb::mcbp::request::RangeScanContinuePayload extras(id, 2, 0);
        BinprotGenericCommand scanContinue(
                cb::mcbp::ClientOpcode::RangeScanContinue);
        scanContinue.setExtras(extras.getBuffer());
        userConnection->sendCommand(scanContinue);

        // Keep reading until we get the 0 length response which indicates the
        // end of the continue sequence
        while (true) {
            userConnection->recvResponse(resp);
            // 0 denotes the end of the sequence
            if (resp.getBodylen() == 0) {
                break;
            }
            ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

            cb::mcbp::response::RangeScanContinueKeyPayload payload(
                    resp.getDataView());

            auto key = payload.next();
            while (key.data()) {
                EXPECT_EQ(1, userKeys.count(std::string{key})) << key;
                key = payload.next();
            }
        }
        ASSERT_TRUE(resp.getStatus() == cb::mcbp::Status::Success ||
                    resp.getStatus() == cb::mcbp::Status::RangeScanMore)
                << resp.getStatus();
    } while (resp.getStatus() != cb::mcbp::Status::Success);
}

TEST_P(RangeScanTest, ValueScan) {
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::RangeScanCreate);
    cmd.setValue(config.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);

    userConnection->sendCommand(cmd);
    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());

    cb::mcbp::request::RangeScanContinuePayload extras(id, 0, 0);
    BinprotGenericCommand cmd2(cb::mcbp::ClientOpcode::RangeScanContinue);
    cmd2.setExtras(extras.getBuffer());
    userConnection->sendCommand(cmd2);

    // Keep reading until we get the 0 length response
    while (true) {
        userConnection->recvResponse(resp);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        // 0 bodylen marks the end of the sequence
        if (resp.getBodylen() == 0) {
            break;
        }

        cb::mcbp::response::RangeScanContinueValuePayload payload(
                resp.getDataView());

        auto record = payload.next();
        while (record.key.data()) {
            EXPECT_EQ(1, userKeys.count(std::string{record.key}));

            if (::testing::get<3>(GetParam()) == ClientSnappySupport::Yes) {
                ASSERT_TRUE(
                        mcbp::datatype::is_snappy(record.meta.getDatatype()));
                cb::compression::Buffer buffer;
                EXPECT_TRUE(cb::compression::inflate(
                        cb::compression::Algorithm::Snappy,
                        record.value,
                        buffer));
                EXPECT_EQ(1,
                          userKeys.count(std::string{std::string_view{buffer}}))
                        << record.key;
            } else {
                EXPECT_EQ(1, userKeys.count(std::string{record.value}));
                ;
            }

            record = payload.next();
        }
        ASSERT_TRUE(resp.getStatus() == cb::mcbp::Status::Success ||
                    resp.getStatus() == cb::mcbp::Status::RangeScanMore)
                << resp.getStatus();
    }
    // Scan should be complete
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // Scan is now unknown (no more continue)
    userConnection->sendCommand(cmd2);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());

    // No cancel
    BinprotGenericCommand cmd3(cb::mcbp::ClientOpcode::RangeScanCancel);
    cmd3.setExtrasValue(id);
    userConnection->sendCommand(cmd3);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());
}