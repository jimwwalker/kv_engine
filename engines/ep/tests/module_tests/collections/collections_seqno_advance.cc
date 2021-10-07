#include "tests/mock/mock_dcp_producer.h"

#include "tests/mock/mock_stream.h"

#include "test_manifest.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/dcp_stream_test.h"
#include "tests/module_tests/test_helpers.h"

#include "dcp/response.h"
#include "kv_bucket.h"
#include "vbucket.h"
#include <memcached/dcp_stream_id.h>
#include <deque>
#include <utility>

// These enums control the test input
enum class InputType {
    Mutation,
    Prepare,
};

enum class ForStream {
    Yes,
    No,
};

// Owns the setup, need a mock producer and mock stream so we can drive
// processItems directly.
class CollectionsSeqnoAdvance : public SingleThreadedKVBucketTest,
                                public ::testing::WithParamInterface<
                                        std::tuple<InputType, ForStream, int>> {
public:
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        auto meta = nlohmann::json{
                {"topology", nlohmann::json::array({{"active", "replica"}})}};
        ASSERT_EQ(cb::engine_errc::success,
                  engine->getKVBucket()->setVBucketState(
                          vbid, vbucket_state_active, &meta));

        producer = std::make_shared<MockDcpProducer>(*engine,
                                                     cookie,
                                                     "CollectionsSeqnoAdvance",
                                                     0,
                                                     false /*startTask*/);

        auto vb = engine->getVBucket(vbid);

        // Create two custom collections, but the test only cares about fruit
        CollectionsManifest cm;
        cm.add(CollectionEntry::vegetable);
        cm.add(CollectionEntry::fruit);
        vb->updateFromManifest(makeManifest(cm));
        stream =
                std::make_shared<MockActiveStream>(engine.get(),
                                                   producer,
                                                   0,
                                                   0 /*opaque*/,
                                                   *vb,
                                                   0,
                                                   ~0,
                                                   0,
                                                   0,
                                                   0,
                                                   IncludeValue::Yes,
                                                   IncludeXattrs::Yes,
                                                   IncludeDeletedUserXattrs::No,
                                                   R"({"collections":["9"]})");
    }

    void TearDown() override {
        // Now generate the final input as per the config
        setupOneOperation(
                seqno, std::get<0>(GetParam()), std::get<1>(GetParam()));

        generateExpectedResponses();

        stream->public_processItems(input);
        for (const auto& e : expected.responses) {
            auto rsp = stream->public_nextQueuedItem(
                    static_cast<DcpProducer&>(*producer));
            ASSERT_TRUE(rsp) << "DCP response expected:" << e->to_string();
            std::cerr << rsp->to_string() << std::endl;
            EXPECT_EQ(*e, *rsp);
            if (*e != *rsp) {
                if (rsp->getEvent() == DcpResponse::Event::Mutation) {
                    const auto* r =
                            static_cast<const MutationResponse*>(rsp.get());
                    std::cerr << *r->getItem();
                }
                if (e->getEvent() == DcpResponse::Event::SeqnoAdvanced) {
                    std::cerr << "SnoAdv:" << e->getBySeqno().value() << "\n";
                }
            }
        }
        stream.reset();
        producer.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    void generateExpectedResponses();

    int getInputSize() const {
        return std::get<2>(GetParam());
    }

    void setupOneOperation(uint64_t seqno, InputType type, ForStream fs) {
        switch (fs) {
        case ForStream::Yes: {
            queueOperation(type, myCollection, seqno);
            break;
        }
        case ForStream::No: {
            queueOperation(type, CollectionEntry::vegetable, seqno);
            break;
        }
        }
    }

    void queueOperation(InputType type, CollectionID cid, uint64_t seqno) {
        switch (type) {
        case InputType::Mutation: {
            queueMutation(cid, seqno);
            break;
        }
        case InputType::Prepare: {
            queuePrepare(cid, seqno);
            break;
        }
        }
    }

    void queueMutation(CollectionID cid, uint64_t seqno) {
        auto item = makeCommittedItem(makeStoredDocKey("k", cid), "value");
        item->setBySeqno(seqno);
        input.items.emplace_back(item);
    }

    void queuePrepare(CollectionID cid, uint64_t seqno) {
        auto item = makePendingItem(makeStoredDocKey("k", cid), "value");
        item->setBySeqno(seqno);
        input.items.emplace_back(item);
    }

    uint64_t seqno{1};

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;

    // The collection that the stream is interested in.
    CollectionID myCollection = CollectionEntry::fruit.getId();

    ActiveStream::OutstandingItemsResult input;

    class ExpectedResponses {
    public:
        ExpectedResponses(Vbid vbid, CollectionID myCollection)
            : vbid(vbid), myCollection(myCollection) {
        }

        void snapshotMemory(uint64_t start, uint64_t end) {
            responses.push_front(
                    std::make_unique<SnapshotMarker>(0 /*opaque*/,
                                                     vbid,
                                                     start,
                                                     end,
                                                     MARKER_FLAG_MEMORY,
                                                     std::nullopt,
                                                     std::nullopt,
                                                     std::nullopt,
                                                     cb::mcbp::DcpStreamId{}));
        }

        void seqnoAdvanced(uint64_t seqno) {
            responses.push_back(std::make_unique<SeqnoAdvanced>(
                    0, vbid, cb::mcbp::DcpStreamId{}, seqno));
        }

        std::optional<uint64_t> generateResponse(queued_item& item) {
            if (item->getKey().getCollectionID() == myCollection) {
                if (item->shouldReplicate(false)) {
                    mutation(item);
                }
                return item->getBySeqno();
            }
            return std::nullopt;
        }

        void clear() {
            responses.clear();
        }

        void mutation(queued_item& item) {
            responses.push_back(std::make_unique<MutationResponse>(
                    item,
                    0 /*opaque*/,
                    IncludeValue::Yes,
                    IncludeXattrs::Yes,
                    IncludeDeleteTime::No,
                    IncludeDeletedUserXattrs::No,
                    DocKeyEncodesCollectionId::Yes,
                    EnableExpiryOutput::No,
                    cb::mcbp::DcpStreamId{}));
        }

        std::deque<std::unique_ptr<DcpResponse>> responses;
        Vbid vbid;
        CollectionID myCollection;
    } expected{vbid, myCollection};
};

void CollectionsSeqnoAdvance::generateExpectedResponses() {
    // Generate the expected output from input
    ASSERT_FALSE(input.items.empty());

    // Break the snapshot into two parts and generate the expected DcpResponse
    // objects.

    // Part one is the snapshot marker which has no collection so is always
    // expected, provided one item for myCollection is visible.
    // However the snapshot end seqno cannot be known until we've seen the
    // items. So it is pushed to the front after walking the items

    // Part two is the 'middle', every item between first and last
    // These are mutations, deletions, expirations, system-events, prepares and
    // aborts. Only when the collection-id of the item matches myCollection
    // would we expect to see it
    std::optional<uint64_t> myHighCollectionSeqno;
    auto itr = input.items.begin();
    auto lastMyCollectionItem = itr;

    for (; itr != input.items.end(); itr++) {
        auto seq = expected.generateResponse(*itr);
        if (seq) {
            myHighCollectionSeqno = seq;
            // And track the highest my-collection item
            lastMyCollectionItem = itr;
        }
    }

    // If the greatest my collection item doesn't replicate
    if (!(*lastMyCollectionItem)->shouldReplicate(false)) {
        expected.seqnoAdvanced((*lastMyCollectionItem)->getBySeqno());
    }

    // Finally if nothing affected the collection, then nothing is expected.
    if (!myHighCollectionSeqno) {
        expected.clear();
    } else {
        // else a snapshot will be seen
        expected.snapshotMemory(0, myHighCollectionSeqno.value());
    }
}

TEST_P(CollectionsSeqnoAdvance, mixed) {
    // Generate alternating inputs of mutations
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(seqno++, InputType::Mutation, ForStream::Yes);
        setupOneOperation(seqno++, InputType::Mutation, ForStream::No);
    }
}

TEST_P(CollectionsSeqnoAdvance, allForStream) {
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(seqno++, InputType::Mutation, ForStream::Yes);
    }
}

TEST_P(CollectionsSeqnoAdvance, noneForStream) {
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(seqno++, InputType::Mutation, ForStream::No);
    }
}

TEST_P(CollectionsSeqnoAdvance, prepareForMeMutationForOther) {
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(seqno++, InputType::Prepare, ForStream::Yes);
        setupOneOperation(seqno++, InputType::Mutation, ForStream::No);
    }
}

TEST_P(CollectionsSeqnoAdvance, prepareNotForMe) {
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(seqno++, InputType::Prepare, ForStream::No);
    }
}

const InputType inputs1[] = {InputType::Mutation, InputType::Prepare};
const ForStream inputs2[] = {ForStream::Yes, ForStream::No};

std::string to_string(InputType type) {
    switch (type) {
    case InputType::Prepare: {
        return "Prepare";
    }
    case InputType::Mutation: {
        return "Mutation";
    }
    }
    throw std::invalid_argument("to_string(Mutation) invalid input");
}

std::string to_string(ForStream fs) {
    switch (fs) {
    case ForStream::Yes: {
        return "for_stream";
    }
    case ForStream::No: {
        return "not_for_stream";
    }
    }
    throw std::invalid_argument("to_string(Mutation) invalid input");
}

std::string printTestName(
        const testing::TestParamInfo<CollectionsSeqnoAdvance::ParamType>&
                info) {
    return "snapshot_size_" + std::to_string(std::get<2>(info.param)) +
           "_with_an_extra_" + to_string(std::get<0>(info.param)) + "_" +
           to_string(std::get<1>(info.param));
}

INSTANTIATE_TEST_SUITE_P(CollectionsSeqnoAdvance,
                         CollectionsSeqnoAdvance,
                         ::testing::Combine(::testing::ValuesIn(inputs1),
                                            ::testing::ValuesIn(inputs2),
                                            ::testing::Values(0, 1, 2, 3)),
                         printTestName);
