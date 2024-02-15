/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../kvstore_test.h"
#include "kvstore/kvstore_config.h"
#include "snapshots/snapshots.h"

#include <filesystem>

// std::variant<cb::engine_errc, nlohmann::json> Snapshots::prepare(
//       KVStoreIface& kvs, std::string_view path, Vbid vbid) {

// cb::engine_errc Snapshots::release(std::string_view path,
//                                  std::string_view uuid) {

// cb::engine_errc Snapshots::release(std::string_view path, Vbid vbid) {

class SnapshotsTests : public KVStoreParamTest {
public:
    void SetUp() override {
        KVStoreParamTest::SetUp();
    }

    bool released(std::string_view uuid, Vbid vb) {
        const auto p1 =
                std::filesystem::path{config.getDbname()} / "snapshots" / uuid;
        const auto p2 = std::filesystem::path{config.getDbname()} /
                        "snapshots" / std::to_string(vb.get());
        return !std::filesystem::exists(p1) && !std::filesystem::exists(p2);
    }
};

TEST_P(SnapshotsTests, prepare) {
    auto rv = Snapshots::prepare(*kvstore, config.getDbname(), Vbid(0));
    ASSERT_TRUE(std::holds_alternative<nlohmann::json>(rv));
    auto json = std::get<nlohmann::json>(rv);
    EXPECT_TRUE(json.contains("uuid"));
    EXPECT_TRUE(json.contains("files"));
    auto& files = json["files"];
    ASSERT_EQ(1, files.size());
    EXPECT_TRUE(files.at(0).contains("path"));
    EXPECT_TRUE(files.at(0).contains("size"));
}

TEST_P(SnapshotsTests, prepare_release_by_uuid) {
    auto rv = Snapshots::prepare(*kvstore, config.getDbname(), Vbid(0));
    ASSERT_TRUE(std::holds_alternative<nlohmann::json>(rv));
    auto json = std::get<nlohmann::json>(rv);
    EXPECT_TRUE(json.contains("uuid"));
    EXPECT_EQ(cb::engine_errc::success,
              Snapshots::release(config.getDbname(),
                                 json["uuid"].get<std::string>()));
    EXPECT_TRUE(released(json["uuid"].get<std::string>(), Vbid(0)))
            << "Not released from dir:" << config.getDbname();
}

TEST_P(SnapshotsTests, prepare_release_by_vb) {
    auto rv = Snapshots::prepare(*kvstore, config.getDbname(), Vbid(0));
    ASSERT_TRUE(std::holds_alternative<nlohmann::json>(rv));
    auto json = std::get<nlohmann::json>(rv);
    EXPECT_TRUE(json.contains("uuid"));
    EXPECT_EQ(cb::engine_errc::success,
              Snapshots::release(config.getDbname(), Vbid(0)));
    EXPECT_TRUE(released(json["uuid"].get<std::string>(), Vbid(0)))
            << "Not released from dir:" << config.getDbname();
}

static std::string testParams[] = {"couchdb"};

INSTANTIATE_TEST_SUITE_P(
        SnapshotsTests,
        SnapshotsTests,
        ::testing::ValuesIn(testParams),
        [](const ::testing::TestParamInfo<std::string>& testInfo) {
            return testInfo.param;
        });