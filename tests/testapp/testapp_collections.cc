/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <random>

class CollectionsTest : public TestappClientTest {
    void SetUp() override {
        TestappClientTest::SetUp();
        // Default engine does not support changing the collection configuration
        if (!mcd_env->getTestBucket().supportsCollections() ||
            mcd_env->getTestBucket().getName() == "default_engine") {
            return;
        }
    }

public:
    struct WorkerContext {
        Vbid vbid = Vbid(0);
        std::unique_ptr<MemcachedConnection> conn;
        std::chrono::steady_clock::time_point nextCompactionCheck;
        std::chrono::steady_clock::time_point nextLog;
        std::chrono::steady_clock::time_point nextThrottleCheck;
        size_t sets{0};
        std::chrono::microseconds throttle = std::chrono::microseconds::zero();
        size_t throttle_sets{0};
    };
    void worker(Vbid vbid);
    void monitorFragmentation(WorkerContext& context);
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         CollectionsTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

static const std::array<const nlohmann::json, 20> objects = {
        "compression_mode=active;dbname=./"
        "ep_testsuite_basic.value_eviction.comp_active.db",
        "(QueuedImmediateExecutor.cpp.o)) was built for newer 'macOS' "
        "version "
        "(12.6) than being linked (11.0)",
        42,
        "UUID:100-163021a1du",
        0xdeadbeef,
        "kv_engine/engines/ep/tests/ep_testsuite_basic.cc",
        123,
        "SUCCESS",
        777111,
        "xyz",
        0xcafef00d12345678,
        "foo",
        3,
        "bar",
        55,
        "baz",
        8121999.19,
        "another string",
        9.0 / 3.0,
        "GUID:91234567890123456789012345678901"};

nlohmann::json createDoc(int nFields,
                         std::mt19937& gen,
                         std::uniform_int_distribution<>& objectDist) {
    nlohmann::json doc;
    for (int i = 0; i < nFields; i++) {
        doc[std::to_string(i)] = objects[objectDist(gen)];
    }
    return doc;
}

void CollectionsTest::worker(Vbid vbid) {
    WorkerContext context;
    context.vbid = vbid;
    context.conn = adminConnection->clone();
    context.nextCompactionCheck =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
    context.nextLog =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
    context.nextThrottleCheck =
            std::chrono::steady_clock::now() + std::chrono::seconds(1);
    context.conn->authenticate("@admin", "password", "PLAIN");
    context.conn->selectBucket(bucketName);

    // Set the vBucket state. Set a single replica so that any
    // SyncWrites can be completed.
    nlohmann::json meta;
    meta["topology"] = nlohmann::json::array({{"active"}});
    context.conn->setVbucket(context.vbid, vbucket_state_active, meta);

    const size_t nKeys = 126992;
    const size_t minSets = 100;
    const size_t rate = 300;
    const size_t minDocSize = 5;
    const size_t maxDocSize = 22;

    std::mt19937 gen(1);
    std::uniform_int_distribution<> docSizeDist(minDocSize, maxDocSize);
    std::uniform_int_distribution<> objectDist(0, objects.size() - 1);
    auto docSize = docSizeDist(gen);
    size_t sets = 0;
    while (true) {
        for (size_t k = 0; k < nKeys; ++k) {
            Document document;
            document.value = createDoc(docSize, gen, objectDist).dump();
            document.info.id = "this-is-a-key-" + std::to_string(k);
            document.info.cas = mcbp::cas::Wildcard;
            document.info.datatype = cb::mcbp::Datatype::JSON;
            document.compress();
            context.conn->mutate(document, context.vbid, MutationType::Set);
            ++sets;
            monitorFragmentation(context);

            if (std::chrono::steady_clock::now() > context.nextThrottleCheck) {
                const auto setsPerSec = sets - context.throttle_sets;
                std::cout << setsPerSec << " sets/sec" << std::endl;
                if (setsPerSec > minSets) {
                    if (setsPerSec > rate) {
                        context.throttle += std::chrono::microseconds(30);
                    } else if (context.throttle >
                               std::chrono::microseconds::zero()) {
                        context.throttle -= std::chrono::microseconds(30);
                    }
                }
                context.throttle_sets = sets;
                context.nextThrottleCheck = std::chrono::steady_clock::now() +
                                            std::chrono::seconds(1);
            }

            if (context.throttle > std::chrono::microseconds::zero()) {
                std::this_thread::sleep_for(context.throttle);
            }
        }
    }
}

void CollectionsTest::monitorFragmentation(WorkerContext& context) {
    const int compactionFrag = 20; // 20% fragmentation
    if (std::chrono::steady_clock::now() > context.nextCompactionCheck) {
        auto stats = userConnection->stats("diskinfo detail");
        auto key1 = fmt::format("vb_{}:data_size", context.vbid.get());
        auto key2 = fmt::format("vb_{}:file_size", context.vbid.get());
        auto dataSize = stats.find(key1);
        auto fileSize = stats.find(key2);
        double fragmentation = 1.0;
        if (dataSize != stats.end() && fileSize != stats.end()) {
            const auto dataSizeValue = dataSize->get<uint64_t>();
            const auto fileSizeValue = fileSize->get<uint64_t>();
            fragmentation = (double)(fileSizeValue - dataSizeValue) /
                            (double)fileSizeValue;
            fragmentation = fragmentation * 100.0;
            std::cout << "Fragmentation: " << fragmentation
                      << "%% Data Size: " << dataSizeValue
                      << " File Size: " << fileSizeValue << std::endl;
        }
        if (1 || fragmentation > compactionFrag) {
            // Compact the database
            BinprotCompactDbCommand compactDbCommand;
            compactDbCommand.setVBucket(context.vbid);
            // compactDbCommand.setDbFileId(context.vbid);

            Frame frame;
            compactDbCommand.encode(frame.payload);
            context.conn->sendFrame(frame);
            bool found = false;
            do {
                frame.reset();
                context.conn->recvFrame(frame);
                const auto* header = frame.getHeader();
                if (header->isResponse() &&
                    header->getResponse().getClientOpcode() ==
                            cb::mcbp::ClientOpcode::CompactDb) {
                    found = true;
                }
            } while (!found);

            EXPECT_EQ(cb::mcbp::Status::Success,
                      frame.getResponse()->getStatus())
                    << "Compaction request was not successful";
            context.nextCompactionCheck =
                    std::chrono::steady_clock::now() + std::chrono::seconds(10);
        } else {
            context.nextCompactionCheck =
                    std::chrono::steady_clock::now() + std::chrono::seconds(5);
        }
    }
}

// Check that an unknown scope/collection error returns the expected JSON
TEST_P(CollectionsTest, ManifestUidInResponse) {
    std::vector<std::thread> threads;
    for (int i = 0; i < 1; ++i) {
        threads.emplace_back([this, i]() { worker(Vbid(i)); });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}
