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

#pragma once

#include "tests/mock/mock_dcp.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"

class MockDcpConsumer;
class MockDcpProducer;

class CollectionsDcpTestProducers : public MockDcpMessageProducers {
public:
    CollectionsDcpTestProducers(EngineIface* engine = nullptr)
        : MockDcpMessageProducers(engine) {
    }
    ~CollectionsDcpTestProducers() {
    }

    ENGINE_ERROR_CODE system_event(uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   mcbp::systemevent::version version,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData) override;

    MockDcpConsumer* consumer = nullptr;
    Vbid replicaVB;
};

class CollectionsDcpTest : public SingleThreadedKVBucketTest {
public:
    CollectionsDcpTest();

    // Setup a producer/consumer ready for the test
    void SetUp() override;
    Collections::VB::PersistedManifest getManifest(Vbid vb) const;

    void createDcpStream(boost::optional<cb::const_char_buffer> collections);

    void createDcpConsumer();

    void createDcpObjects(boost::optional<cb::const_char_buffer> collections);

    void TearDown() override;

    void teardown();

    void runCheckpointProcessor();

    void notifyAndStepToCheckpoint(
            cb::mcbp::ClientOpcode expectedOp =
                    cb::mcbp::ClientOpcode::DcpSnapshotMarker,
            bool fromMemory = true);

    /// the vectors are ordered, so front() is the first item we expect to see
    void testDcpCreateDelete(
            const std::vector<CollectionEntry::Entry>& expectedCreates,
            const std::vector<CollectionEntry::Entry>& expectedDeletes,
            int expectedMutations,
            bool fromMemory = true,
            const std::vector<ScopeEntry::Entry>& expectedScopeCreates = {},
            const std::vector<ScopeEntry::Entry>& expectedScopeDrops = {});

    void resetEngineAndWarmup(std::string new_config = "");

    static ENGINE_ERROR_CODE dcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie);

    const void* cookieC;
    const void* cookieP;
    std::unique_ptr<CollectionsDcpTestProducers> producers;
    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockDcpConsumer> consumer;
    Vbid replicaVB;
};
