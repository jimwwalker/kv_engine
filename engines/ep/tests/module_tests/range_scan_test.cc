/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill.h"
#include "kv_bucket.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

class RangeScanTest : public STParameterizedBucketTest {
    void SetUp() override {
        STParameterizedBucketTest::SetUp();

        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }
};

// Basic test covers current code (which doesn't do much)
TEST_P(RangeScanTest, create_and_run) {
    auto backfill = store->getVBucket(vbid)->createRangeScanTask(
            store->getEPEngine(), makeStoredDocKey("a"), makeStoredDocKey("b"));
    EXPECT_EQ(backfill_success, backfill->run()); // create
    EXPECT_EQ(backfill_finished, backfill->run()); // scan
}

INSTANTIATE_TEST_SUITE_P(
        FullAndvalueEviction,
        RangeScanTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);