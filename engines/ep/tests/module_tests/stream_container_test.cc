/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "dcp/stream_container.h"

#include <array>
#include <thread>

#include <gtest/gtest.h>

TEST(StreamContainerIterationTest, basic) {
    // Note: cannot construct empty
    // intended usage is that DcpProducer goes from an empty map to a map which
    // contains at least 1 stream.
    StreamContainer<int> c(100);

    int iterations = 0;
    for (auto handle = c.lock(); !handle.end(); handle.next()) {
        EXPECT_EQ(100, handle.get());
        iterations++;
    }
    EXPECT_EQ(1, iterations);

    iterations = 0;
    for (auto handle = c.wlock(); !handle.end(); handle.next()) {
        EXPECT_EQ(100, handle.get());
        iterations++;
    }
    EXPECT_EQ(1, iterations);

    iterations = 0;
    for (auto handle = c.startResumable(); !handle.complete(); handle.next()) {
        EXPECT_EQ(100, handle.get());
        iterations++;
    }
    EXPECT_EQ(1, iterations);
}

class StreamContainerTest : public ::testing::Test {
public:
    StreamContainerTest() : c{1} {
    }
    void SetUp() override {
        auto wh = c.wlock();
        wh.push_front(2);
        wh.push_front(3);
        wh.push_front(4);
        wh.push_front(5);
    }

    StreamContainer<int> c;

    void iterateContainer(const std::vector<int>& expected, int stop = 0) {
        auto expectItr = expected.begin();
        for (auto itr = c.startResumable(); !itr.complete(); itr.next()) {
            EXPECT_EQ(*expectItr, itr.get());
            if (itr.get() == stop) {
                return;
            }
            expectItr++;
        }
        EXPECT_EQ(expectItr, expected.end());
    }
};

TEST_F(StreamContainerTest, resumable_iteration) {
    // Iterate a full cycle
    iterateContainer({{5, 4, 3, 2, 1}});

    // Now interrupt the iteration early
    iterateContainer({{5, 4, 3}}, 3);

    // Iterate a full cycle and expect the first element to be 2
    iterateContainer({{2, 1, 5, 4, 3}});
}

// Test iteration when a push_front is interleaved
TEST_F(StreamContainerTest, resumable_iteration_with_push_front) {
    // interrupt the iteration early
    iterateContainer({{5, 4, 3}}, 3);

    // And we change the container... resume is reset
    c.wlock().push_front(6);

    // Expect to start at the new element
    iterateContainer({{6, 5, 4, 3, 2, 1}});
}

// Test iteration when a erase is interleaved
TEST_F(StreamContainerTest, resumable_iteration_with_erase) {
    // interrupt the iteration early
    iterateContainer({{5, 4, 3}}, 3);

    // And we change the container... resume is reset
    {
        auto wh = c.wlock();
        for (; !wh.end(); wh.next()) {
            if (wh.get() == 4) {
                wh.erase();
                break;
            }
        }
    }

    iterateContainer({{5, 3, 2, 1}});
}

TEST_F(StreamContainerTest, size_erase_and_empty) {
    EXPECT_FALSE(c.wlock().empty());
    EXPECT_EQ(5, c.lock().size());
    std::vector<int> expected = {{5, 4, 3, 2, 1}};
    for (auto e : expected) {
        auto wh = c.wlock();
        EXPECT_EQ(e, wh.get());
        wh.erase();
    }
    EXPECT_TRUE(c.wlock().empty());
    EXPECT_EQ(0, c.lock().size());
}

TEST_F(StreamContainerTest, swap) {
    // interrupt the iteration early
    iterateContainer({{5, 4, 3}}, 3);
    int e = 99;
    c.wlock().swap(e);
    EXPECT_EQ(5, e);
    // swap doesn't change StreamContainer membership, resume continues
    iterateContainer({{2, 1, 99, 4, 3}});
}
