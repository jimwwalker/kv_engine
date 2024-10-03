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
#ifdef EP_USE_MAGMA

#include "evp_store_single_threaded_test.h"
#include "kvstore/magma-kvstore/magma-kvstore.h"
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#include <folly/portability/GTest.h>

/**
 * Test fixture for single-threaded tests on EPBucket/Magma.
 */
class SingleThreadedMagmaTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        config_string += "magma_fusion_endpoint_uri=" + fusionURI;
        STParameterizedBucketTest::SetUp();
    }

protected:
    const std::string fusionURI = "fusion://localhost:10000";
};

TEST_P(SingleThreadedMagmaTest, FusionEndpointUri) {
    auto& kvstore = dynamic_cast<MagmaKVStore&>(*store->getRWUnderlying(vbid));
    const auto& config =
            dynamic_cast<const MagmaKVStoreConfig&>(kvstore.getConfig());
    EXPECT_EQ(fusionURI, config.getFusionEndpointURI());
}

INSTANTIATE_TEST_SUITE_P(SingleThreadedMagmaTest,
                         SingleThreadedMagmaTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

#endif