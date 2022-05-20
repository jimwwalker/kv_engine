/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "buckets.h"
#include "stats.h"

#include <folly/portability/GTest.h>

TEST(BucketTest, Reset) {
#if defined(__linux) && defined(__x86_64__)
    static_assert(sizeof(Bucket) == 6248,
                  "Bucket size changed, the reset test must be updated with "
                  "the new members");
#endif

    class MockBucket : public Bucket {
    public:
        void testReset() {
            throttle_gauge.increment(5);
            throttle_limit = 1;
            num_throttled = 1;
            throttle_wait_time = 1;
            num_commands = 1;

            reset();
            EXPECT_EQ(0, throttle_limit);
            EXPECT_EQ(0, num_throttled);
            EXPECT_EQ(0, throttle_wait_time);
            EXPECT_EQ(0, num_commands);
            throttle_gauge.iterate([](auto val) { EXPECT_EQ(0, val); });
        }
    } bucket;

    bucket.testReset();
}
