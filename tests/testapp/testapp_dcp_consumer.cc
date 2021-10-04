/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

enum class AlwaysBuffer { Yes, No };

std::string to_string(AlwaysBuffer value) {
    switch (value) {
    case AlwaysBuffer::Yes:
        return "AlwaysBufferYes";
    case AlwaysBuffer::No:
        return "AlwaysBufferNo";
    }
    throw std::invalid_argument("to_string(AlwaysBuffer)");
}

class DcpConsumerBufferAckTest
    : public TestappTest,
      public ::testing::WithParamInterface<::testing::tuple<TransportProtocols,
                                                            XattrSupport,
                                                            ClientJSONSupport,
                                                            ClientSnappySupport,
                                                            AlwaysBuffer>> {
public:
    void SetUp() override {
        conn = &getAdminConnection();
        conn->selectBucket(bucketName);
        conn->setFeature(cb::mcbp::Feature::JSON, true);
        conn->setFeature(cb::mcbp::Feature::SNAPPY, true);
        conn->setFeature(cb::mcbp::Feature::XATTR, true);
        conn->setFeature(cb::mcbp::Feature::Collections, true);
        conn->setVbucket(Vbid{0}, vbucket_state_replica, {/*no json*/});
        conn->dcpOpenConsumer("replication:client->server");
        // Configure 0 bytes - every input will generate an ACK
        conn->dcpControl("connection_buffer_size", "0");

        if (testAlwaysBuffered()) {
            conn->dcpControl("always_buffer_operations", "true");
        }
        conn->dcpAddStream(Vbid{0});

        // After AddStream the consumer will send back a number of control
        // messages and one GetErrorMap (for producer detection). We will ack
        // success to all of these commands. When we see stream-request then
        // we can send a failover table and run the test code
        Frame frame;
        auto stepDcp = [&frame, this]() {
            conn->recvFrame(frame);
            EXPECT_EQ(cb::mcbp::Magic::ClientRequest, frame.getMagic());
            return frame.getRequest();
        };

        while (true) {
            const auto* request = stepDcp();
            if (request->getClientOpcode() ==
                cb::mcbp::ClientOpcode::DcpStreamReq) {
                // Respond with success with a failover log
                conn->dcpStreamRequestResponse(request->getOpaque(),
                                               {{0xdeadbeefull, 0}});
                break;
            } else {
                conn->sendCommand(BinprotCommandResponse{
                        request->getClientOpcode(), request->getOpaque()});
            }
        }

        // And finally AddStream response now that the stream is ready.
        BinprotResponse rsp;
        conn->recvResponse(rsp);
        ASSERT_TRUE(rsp.isSuccess());

        // Generate a document
        const auto key = DocKey::create(CollectionID::Default, "key");
        doc.info.id = key;
        doc.info.cas = nextCas();
        generateDocumentValue(getValue());
    }

    std::string getValue() const {
        auto rv = memcached_cfg.dump();

        if (!testJson()) {
            // Same length, but not json
            std::replace(rv.begin(), rv.end(), '{', 'q');
            std::replace(rv.begin(), rv.end(), '}', 'r');
        }
        return rv;
    }

    std::string getVeryCompressibleValue() const {
        nlohmann::json value;
        value["aaa"] = std::string(500, 'a');
        std::string rv = value.dump();
        if (!testJson()) {
            // Same length, but not json
            std::replace(rv.begin(), rv.end(), '{', 'q');
            std::replace(rv.begin(), rv.end(), '}', 'r');
        }
        return rv;
    }

    std::string getSmallValue() const {
        nlohmann::json value;
        value["k"] = "v";
        std::string rv = value.dump();
        if (!testJson()) {
            // Same length, but not json
            std::replace(rv.begin(), rv.end(), '{', 'q');
            std::replace(rv.begin(), rv.end(), '}', 'r');
        }
        return rv;
    }

    void generateDocumentValue(const std::string& value,
                               const std::string& xattrKey = "_system_key",
                               int xattrCount = 1) {
        doc.info.datatype = cb::mcbp::Datatype::Raw;
        if (testXattr()) {
            std::vector<std::pair<std::string, std::string>> xattrList;
            for (int ii = 0; ii < xattrCount; ii++) {
                auto index = std::to_string(ii);
                xattrList.emplace_back(xattrKey + index, index);
            }

            doc.value = createXattrValue(value, xattrList);
            doc.info.datatype = cb::mcbp::Datatype::Xattr;
        } else {
            doc.value = value;
        }

        if (testJson()) {
            doc.info.datatype =
                    cb::mcbp::Datatype(uint8_t(doc.info.datatype) |
                                       uint8_t(cb::mcbp::Datatype::JSON));
        }

        if (testSnappy()) {
            doc.compress();
        }
    }

    bool testXattr() const {
        return ::testing::get<1>(GetParam()) == XattrSupport::Yes;
    }
    bool testJson() const {
        return ::testing::get<2>(GetParam()) == ClientJSONSupport::Yes;
    }

    bool testSnappy() const {
        return ::testing::get<3>(GetParam()) == ClientSnappySupport::Yes;
    }

    bool testAlwaysBuffered() const {
        return ::testing::get<4>(GetParam()) == AlwaysBuffer::Yes;
    }

    static uint64_t nextSeqno() {
        return seqno++;
    }

    static uint64_t nextCas() {
        return cas++;
    }

    MemcachedConnection* conn;
    Document doc;
    static uint64_t seqno;
    static uint64_t cas;
};

uint64_t DcpConsumerBufferAckTest::seqno{1};
uint64_t DcpConsumerBufferAckTest::cas{1};

struct ToStringCombinedTestName {
    std::string operator()(
            const ::testing::TestParamInfo<::testing::tuple<TransportProtocols,
                                                            XattrSupport,
                                                            ClientJSONSupport,
                                                            ClientSnappySupport,
                                                            AlwaysBuffer>>&
                    info) const {
        std::string rv = to_string(::testing::get<0>(info.param)) + "_" +
                         to_string(::testing::get<1>(info.param)) + "_" +
                         to_string(::testing::get<2>(info.param)) + "_" +
                         to_string(::testing::get<3>(info.param)) + "_" +
                         to_string(::testing::get<4>(info.param));
        return rv;
    }
};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        DcpConsumerBufferAckTest,
        ::testing::Combine(
                ::testing::Values(TransportProtocols::McbpPlain),
                ::testing::Values(XattrSupport::Yes, XattrSupport::No),
                ::testing::Values(ClientJSONSupport::Yes,
                                  ClientJSONSupport::No),
                ::testing::Values(ClientSnappySupport::Yes,
                                  ClientSnappySupport::No),
                ::testing::Values(AlwaysBuffer::Yes, AlwaysBuffer::No)),
        ToStringCombinedTestName());

TEST_P(DcpConsumerBufferAckTest, Basic) {
    conn->recvDcpBufferAck(conn->dcpSnapshotMarkerV2(
            1 /*opaque */, seqno /*start*/, seqno + 2 /*end*/, 0 /*flags*/));

    conn->recvDcpBufferAck(conn->dcpMutation(doc, 1 /*opaque*/, nextSeqno()));

    // Do a delete with no value
    doc.value = {};
    doc.info.datatype = cb::mcbp::Datatype::Raw;
    doc.info.cas = nextCas();
    conn->recvDcpBufferAck(conn->dcpDeletionV2(doc, 1 /*opaque*/, nextSeqno()));
}

TEST_P(DcpConsumerBufferAckTest, DeleteWithValue) {
    conn->recvDcpBufferAck(conn->dcpSnapshotMarkerV2(
            1 /*opaque */, seqno /*start*/, seqno + 2 /*end*/, 0 /*flags*/));

    conn->recvDcpBufferAck(conn->dcpMutation(doc, 1 /*opaque*/, nextSeqno()));

    doc.info.cas = nextCas();

    auto tx = conn->dcpDeletionV2(doc, 1 /*opaque*/, nextSeqno());

    // Json values are only legal when combined with xattr
    if (testJson() && !testXattr()) {
        BinprotResponse rsp;
        conn->recvResponse(rsp);
        ASSERT_FALSE(rsp.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    } else {
        conn->recvDcpBufferAck(tx);
    }
}

// Similar to previous test but use a highly compressible 'body'
TEST_P(DcpConsumerBufferAckTest, DeleteWithCompressibleValue) {
    generateDocumentValue(getVeryCompressibleValue());
    std::cerr << doc.value.size() << std::endl;

    conn->recvDcpBufferAck(conn->dcpSnapshotMarkerV2(
            1 /*opaque */, seqno /*start*/, seqno + 2 /*end*/, 0 /*flags*/));

    conn->recvDcpBufferAck(conn->dcpMutation(doc, 1 /*opaque*/, nextSeqno()));

    doc.info.cas = nextCas();

    auto tx = conn->dcpDeletionV2(doc, 1 /*opaque*/, nextSeqno());

    // Json values are only legal when combined with xattr
    if (testJson() && !testXattr()) {
        BinprotResponse rsp;
        conn->recvResponse(rsp);
        ASSERT_FALSE(rsp.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    } else {
        conn->recvDcpBufferAck(tx);
    }
}

// Similar to previous test but use many highly compressible 'xattr' i.e. the
// majority of the value is xattr data
// MB-47318: Issue detected that a buffered delete can ack more than we sent.
// This seems to occur when value is compressed and DCP buffers the delete, the
// delete triggers value sanitisation code and results in an ACK using the
// decompressed size, which this test forces to be larger.
TEST_P(DcpConsumerBufferAckTest, DeleteWithLargeXattrs) {
    std::string xattrKey = "_" + std::string(5, 'a');
    generateDocumentValue(getSmallValue(), xattrKey, 10);

    conn->recvDcpBufferAck(conn->dcpSnapshotMarkerV2(
            1 /*opaque */, seqno /*start*/, seqno + 2 /*end*/, 0 /*flags*/));

    conn->recvDcpBufferAck(conn->dcpMutation(doc, 1 /*opaque*/, nextSeqno()));

    doc.info.cas = nextCas();

    auto tx = conn->dcpDeletionV2(doc, 1 /*opaque*/, nextSeqno());

    // Json values are only legal when combined with xattr
    if (testJson() && !testXattr()) {
        BinprotResponse rsp;
        conn->recvResponse(rsp);
        ASSERT_FALSE(rsp.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    } else {
        conn->recvDcpBufferAck(tx);
    }
}