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


#include "tests/module_tests/evp_store_single_threaded_test.h"

#include "snapshots/cache.h"
#include "tests/mock/mock_ep_bucket.h"

#include <gtest/gtest.h>

class SnapshotEngineTest : public SingleThreadedEPBucketTest,
                           public ::testing::WithParamInterface<std::string> {
public:
    void SetUp() override {
        SingleThreadedEPBucketTest::SetUp();
    }
};

TEST_P(SnapshotEngineTest, nmvb) {
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
    setVBucketState(vbid, vbucket_state_replica);
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot_no_disk_state) {
    // Active VB with nothing on disk
    setVBucketState(vbid, vbucket_state_active);

    // this is returning failed, but should not be something to fail rebalance.
    EXPECT_EQ(cb::engine_errc::failed,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json manifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, [&manifest](auto& m) { manifest = m; }));
    EXPECT_TRUE(manifest.contains("uuid"));

    if (!isMagma()) {
        EXPECT_EQ(1, manifest["files"].size());
        EXPECT_EQ(1, manifest["files"][0]["id"]);
        EXPECT_EQ("0.couch.1", manifest["files"][0]["path"]);
        EXPECT_GT(manifest["files"][0]["size"], 0);
        nlohmann::json fileInfo;
        EXPECT_EQ(cb::engine_errc::success,
                  engine->get_snapshot_file_info(
                          *cookie,
                          manifest["uuid"].get<std::string>(),
                          manifest["files"][0]["id"],
                          [&fileInfo](auto& m) { fileInfo = m; }));
        EXPECT_EQ(manifest["files"][0], fileInfo) << fileInfo;

    } else {
        // Not sure what we can assume?
        FAIL() << "No magma testing\n";
    }

    auto& cache = dynamic_cast<MockEPBucket&>(*store).getSnapshotCache();
    EXPECT_EQ(1, cache.getSize());
    EXPECT_TRUE(cache.exists(vbid));
}

TEST_P(SnapshotEngineTest, prepare_release_snapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json manifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, [&manifest](auto& m) { manifest = m; }));

    auto& cache = dynamic_cast<MockEPBucket&>(*store).getSnapshotCache();
    EXPECT_EQ(1, cache.getSize());
    EXPECT_TRUE(cache.exists(vbid));

    EXPECT_EQ(cb::engine_errc::success,
              engine->release_snapshot(
                      *cookie, vbid, manifest["uuid"].get<std::string>()));

    EXPECT_EQ(0, cache.getSize());
    EXPECT_FALSE(cache.exists(vbid));
}

TEST_P(SnapshotEngineTest, prepare_release_by_vb) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json manifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, [&manifest](auto& m) { manifest = m; }));

    auto& cache = dynamic_cast<MockEPBucket&>(*store).getSnapshotCache();
    EXPECT_EQ(1, cache.getSize());
    EXPECT_TRUE(cache.exists(vbid));
    EXPECT_EQ(cb::engine_errc::success,
              engine->release_snapshot(*cookie, vbid, {}));

    EXPECT_EQ(0, cache.getSize());
    EXPECT_FALSE(cache.exists(vbid));
}

TEST_P(SnapshotEngineTest, prepare_purge_snapshots) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    setVBucketStateAndRunPersistTask(Vbid{1}, vbucket_state_active);
    nlohmann::json manifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, [&manifest](auto& m) { manifest = m; }));
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(*cookie, Vbid{1}, [&manifest](auto& m) {
                  manifest = m;
              }));

    auto& cache = dynamic_cast<MockEPBucket&>(*store).getSnapshotCache();
    EXPECT_EQ(2, cache.getSize());
    EXPECT_TRUE(cache.exists(vbid));
    EXPECT_TRUE(cache.exists(Vbid{1}));

    // Directly test the purge function which takes an age parameter. Pass zero
    // to force purge everything
    EXPECT_EQ(2,
              cache.purge(store->getConfiguration().getDbname(),
                          std::chrono::seconds(0)));
    EXPECT_EQ(0, cache.getSize());
    EXPECT_FALSE(cache.exists(vbid));
    EXPECT_FALSE(cache.exists(Vbid{1}));
}

static std::string PrintToStringParamName(
        const testing::TestParamInfo<std::string>& info) {
    return info.param;
}

// todo: add magma (and maybe nexus)
INSTANTIATE_TEST_SUITE_P(SnapshotEngineTests,
                         SnapshotEngineTest,
                         ::testing::Values("persistent_couchdb"),
                         PrintToStringParamName);