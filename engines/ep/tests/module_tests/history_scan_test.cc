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

#include "collections/collections_dcp_test.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"

#include <memcached/protocol_binary.h>

using namespace cb::mcbp;
using namespace cb::mcbp::request;
using namespace mcbp::systemevent;

// HistoryScanTest sub-classes collections DCP to give access to useful
// utilities for testing the "change stream" backfill feature.
class HistoryScanTest : public CollectionsDcpParameterizedTest {
public:
};

// Test that a scan which starts below the history window delivers two snapshots
TEST_P(HistoryScanTest, TwoSnapshots) {
    // This writes to fruit and vegetable
    setupTwoCollections();

    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();

    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(6, producers->last_flags);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(5, producers->last_snap_end_seqno);
    // mvs/hcs need testing and sorting out. Suspect this first snap excludes

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(1, producers->last_byseqno);

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(2, producers->last_byseqno);

    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(3, producers->last_byseqno);

    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(4, producers->last_byseqno);

    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(5, producers->last_byseqno);

    // 1/2 of the seqno index is "history"
    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    // Historical disk snapshot, and it may contain duplicate keys
    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK | MARKER_FLAG_HISTORY |
                      MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS,
              producers->last_flags);
    EXPECT_EQ(6, producers->last_snap_start_seqno);
    EXPECT_EQ(10, producers->last_snap_end_seqno);

    // @todo: change the test and validate this contains "versions"
    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(6, producers->last_byseqno);

    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(7, producers->last_byseqno);

    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(8, producers->last_byseqno);

    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(9, producers->last_byseqno);

    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(10, producers->last_byseqno);
}

// Test that a scan which starts inside the history snapshot, only delivers the
// history window
TEST_P(HistoryScanTest, AllHistory) {
    // This writes to fruit and vegetable
    setupTwoCollections();

    ensureDcpWillBackfill();

    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();
}

// Test OSO switches to history
TEST_P(HistoryScanTest, OSOThenHistory) {
    // This writes to fruit and vegetable
    auto highSeqno = setupTwoCollections().second;

    ensureDcpWillBackfill();

    // Filter on vegetable collection (this will request from seqno:0)
    createDcpObjects({{R"({"collections":["a"]})"}},
                     OutOfOrderSnapshots::Yes,
                     0,
                     false,
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();

    // see comment in CollectionsOSODcpTest.basic
    consumer->snapshotMarker(1, replicaVB, 0, highSeqno, 0, 0, highSeqno);

    // Manually step the producer and inspect all callbacks
    stepAndExpect(ClientOpcode::DcpOsoSnapshot);
    EXPECT_EQ(ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    EXPECT_EQ("vegetable", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    uint64_t txHighSeqno = 0;
    std::array<std::string, 4> keys = {{"a", "b", "c", "d"}};

    for (auto& k : keys) {
        // Now we get the mutations, they aren't guaranteed to be in seqno
        // order, but we know that for now they will be in key order.
        stepAndExpect(ClientOpcode::DcpMutation);
        EXPECT_EQ(ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(k, producers->last_key) << producers->last_byseqno;
        EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
        txHighSeqno = std::max(txHighSeqno, producers->last_byseqno.load());
    }

    // Now we get the end message
    stepAndExpect(ClientOpcode::DcpOsoSnapshot);
    EXPECT_EQ(ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);

    // Now we get the second snapshot
    stepAndExpect(ClientOpcode::DcpSnapshotMarker);

    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK | MARKER_FLAG_HISTORY |
                      MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS,
              producers->last_flags);
    stepAndExpect(ClientOpcode::DcpSystemEvent);

    // And all keys in seq order. Setup created in order b, d, a, c
    std::array<std::string, 4> keySeqnoOrder = {{"b", "d", "a", "c"}};
    for (auto& k : keySeqnoOrder) {
        // Now we get the mutations, they aren't guaranteed to be in seqno
        // order, but we know that for now they will be in key order.
        stepAndExpect(ClientOpcode::DcpMutation);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    }
}

// A dropped collection can still exist inside the history window, test it is
// not observable by DCP change stream when backfilling
TEST_P(HistoryScanTest, BackfillWithDroppedCollection) {
}

INSTANTIATE_TEST_SUITE_P(HistoryScanTests,
                         HistoryScanTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);