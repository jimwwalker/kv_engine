/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "passive_stream.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/events_generated.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/consumer.h"
#include "dcp/response.h"
#include "durability/durability_monitor.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "replicationthrottle.h"
#include "vbucket.h"

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <platform/optional.h>
#include <statistics/cbstat_collector.h>

#include <memory>

const std::string passiveStreamLoggingPrefix =
        "DCP (Consumer): **Deleted conn**";

PassiveStream::PassiveStream(EventuallyPersistentEngine* e,
                             std::shared_ptr<DcpConsumer> c,
                             const std::string& name,
                             uint32_t flags,
                             uint32_t opaque,
                             Vbid vb,
                             uint64_t st_seqno,
                             uint64_t en_seqno,
                             uint64_t vb_uuid,
                             uint64_t snap_start_seqno,
                             uint64_t snap_end_seqno,
                             uint64_t vb_high_seqno,
                             const Collections::ManifestUid vb_manifest_uid)
    : Stream(name,
             flags,
             opaque,
             vb,
             st_seqno,
             en_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno),
      engine(e),
      consumerPtr(c),
      last_seqno(vb_high_seqno, {*this}),
      cur_snapshot_start(0, {*this}),
      cur_snapshot_end(0, {*this}),
      cur_snapshot_type(Snapshot::None),
      cur_snapshot_ack(false),
      cur_snapshot_prepare(false),
      vb_manifest_uid(vb_manifest_uid),
      alwaysBufferOperations(c->shouldBufferOperations()),
      flatBuffersSystemEventsEnabled(c->areFlatBuffersSystemEventsEnabled()) {
    std::lock_guard<std::mutex> lh(streamMutex);
    streamRequest_UNLOCKED(vb_uuid);
    itemsReady.store(true);
}

PassiveStream::~PassiveStream() {
    uint32_t unackedBytes = clearBuffer_UNLOCKED();
    if (state_ != StreamState::Dead) {
        // Destructed a "live" stream, log it.
        log(spdlog::level::level_enum::info,
            "({}) Destructing stream."
            " last_seqno is {}, unAckedBytes is {}.",
            vb_,
            last_seqno.load(),
            unackedBytes);
    }
}

void PassiveStream::streamRequest(uint64_t vb_uuid) {
    {
        std::unique_lock<std::mutex> lh(streamMutex);
        streamRequest_UNLOCKED(vb_uuid);
    }
    notifyStreamReady();
}

void PassiveStream::streamRequest_UNLOCKED(uint64_t vb_uuid) {
    auto stream_req_value = createStreamReqValue();

    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    pushToReadyQ(std::make_unique<StreamRequest>(vb_,
                                                 opaque_,
                                                 flags_,
                                                 start_seqno_,
                                                 end_seqno_,
                                                 start_seqno_ ? vb_uuid : 0,
                                                 snap_start_seqno_,
                                                 snap_end_seqno_,
                                                 stream_req_value));

    const char* type = (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER)
                               ? "takeover stream"
                               : "stream";

    log(spdlog::level::level_enum::info,
        "({}) Attempting to add {}: opaque_:{}, start_seqno_:{}, "
        "end_seqno_:{}, vb_uuid:{}, snap_start_seqno_:{}, snap_end_seqno_:{}, "
        "last_seqno:{}, stream_req_value:{}",
        vb_,
        type,
        opaque_,
        start_seqno_,
        end_seqno_,
        vb_uuid,
        snap_start_seqno_,
        snap_end_seqno_,
        last_seqno.load(),
        stream_req_value.empty() ? "none" : stream_req_value);
}

uint32_t PassiveStream::setDead(cb::mcbp::DcpStreamEndStatus status) {
    /* Hold buffer lock so that we clear out all items before we set the stream
       to dead state. We do not want to add any new message to the buffer or
       process any items in the buffer once we set the stream state to dead. */
    std::unique_lock<std::mutex> lg(buffer.bufMutex);
    const auto unackedBytes = clearBuffer_UNLOCKED();

    std::lock_guard<std::mutex> slh(streamMutex);
    if (transitionState(StreamState::Dead)) {
        const auto severity =
                status == cb::mcbp::DcpStreamEndStatus::Disconnected
                        ? spdlog::level::level_enum::warn
                        : spdlog::level::level_enum::info;
        log(severity,
            "({}) Setting stream to dead state, last_seqno is {}, unAckedBytes "
            "is {}, status is {}",
            vb_,
            last_seqno.load(),
            unackedBytes,
            cb::mcbp::to_string(status));
    }

    return unackedBytes;
}

uint32_t PassiveStream::moveFlowControlBytes() {
    std::unique_lock<std::mutex> lg(buffer.bufMutex);
    return std::exchange(buffer.bytes, 0);
}

std::string PassiveStream::getStreamTypeName() const {
    return "Passive";
}

std::string PassiveStream::getStateName() const {
    return to_string(state_);
}

bool PassiveStream::isActive() const {
    return state_ != StreamState::Dead;
}

bool PassiveStream::isPending() const {
    return state_ == StreamState::Pending;
}

void PassiveStream::acceptStream(cb::mcbp::Status status, uint32_t add_opaque) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        log(spdlog::level::level_enum::warn,
            "({}) PassiveStream::acceptStream(): status:{} - Unable to find "
            "VBucket - cannot accept Stream.",
            vb_,
            status);
        return;
    }

    auto consumer = consumerPtr.lock();
    if (!consumer) {
        log(spdlog::level::level_enum::warn,
            "({}) PassiveStream::acceptStream(): status:{} - Unable to lock "
            "Consumer - cannot accept Stream.",
            vb_,
            status);
        return;
    }

    // We use the cur_snapshot_prepare member to determine if we should
    // notify the PDM of any Memory snapshots. It is set when we see a
    // prepare in any snapshot. Consider the following snapshot:
    //
    // [1:Prepare(A), 2:Mutation(B)] Type = Memory
    //
    // If we have only received and persisted the following sequence of events
    // but then restart, we would fail to notify the PDM of the complete
    // snapshot:
    //
    // 1) SnapshotMarker (1-2) Type = Memory
    // 2) Prepare (1)                        <- Persisted to disk
    //
    // To solve this, we can fix the cur_snapshot_prepare state on
    // PassiveStream acceptance. The PDM already avoids acking back the same
    // seqno, so notifying an extra snapshot shouldn't matter, and even if we
    // did ack back the same seqno, the ADM should already deal with weakly
    // monotonic acks as we ack back the HPS on stream connection.
    cur_snapshot_prepare = true;

    // SyncReplication: About to commence accepting data on this stream. Check
    // if the associated consumer supports SyncReplication, so we can later
    // correctly process Snapshot Markers.
    supportsSyncReplication = consumer->isSyncReplicationEnabled();

    // as above, but check if FlatBuffers was enabled
    flatBuffersSystemEventsEnabled =
            consumer->areFlatBuffersSystemEventsEnabled();

    // For SyncReplication streams lookup the highPreparedSeqno to check if
    // we need to re-ACK (after accepting the stream).
    const int64_t highPreparedSeqno =
            supportsSyncReplication ? vb->getHighPreparedSeqno() : 0;

    std::unique_lock<std::mutex> lh(streamMutex);
    if (isPending()) {
        pushToReadyQ(std::make_unique<AddStreamResponse>(
                add_opaque, opaque_, status));
        if (status == cb::mcbp::Status::Success) {
            // Before we receive/process anything else, send a seqno ack if we
            // are a stream for a pre-existing vBucket to ensure that the
            // replica can commit any in-flight SyncWrites if no further
            // SyncWrites are done and no disk snapshots processed by this
            // replica.
            if (highPreparedSeqno) {
                pushToReadyQ(std::make_unique<SeqnoAcknowledgement>(
                        opaque_, vb_, highPreparedSeqno));
            }
            transitionState(StreamState::Reading);
        } else {
            transitionState(StreamState::Dead);
        }
        lh.unlock();
        notifyStreamReady();
    }
}

void PassiveStream::reconnectStream(VBucketPtr& vb,
                                    uint32_t new_opaque,
                                    uint64_t start_seqno) {
    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    vb_uuid_ = start_seqno ? vb->failovers->getLatestEntry().vb_uuid : 0;

    snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
    if (info.range.getEnd() == info.start) {
        info.range.setStart(info.start);
    }

    auto stream_req_value = createStreamReqValue();

    {
        std::lock_guard<std::mutex> lh(streamMutex);

        snap_start_seqno_ = info.range.getStart();
        start_seqno_ = info.start;
        snap_end_seqno_ = info.range.getEnd();
        last_seqno.reset(start_seqno);
        // The start_seqno & cur_snapshot_end shouldn't be less than start_seqno
        // to set it's starting val to start_seqno
        cur_snapshot_start.reset(start_seqno);
        cur_snapshot_end.reset(start_seqno);

        log(spdlog::level::level_enum::info,
            "({}) Attempting to reconnect stream with opaque {}, start seq "
            "no {}, end seq no {}, snap start seqno {}, snap end seqno {}, and "
            "vb manifest uid {}",
            vb_,
            new_opaque,
            start_seqno,
            end_seqno_,
            snap_start_seqno_,
            snap_end_seqno_,
            stream_req_value.empty() ? "none" : stream_req_value);

        pushToReadyQ(std::make_unique<StreamRequest>(vb_,
                                                     new_opaque,
                                                     flags_,
                                                     start_seqno,
                                                     end_seqno_,
                                                     vb_uuid_,
                                                     snap_start_seqno_,
                                                     snap_end_seqno_,
                                                     stream_req_value));
    }
    notifyStreamReady();
}

cb::engine_errc PassiveStream::messageReceived(
        std::unique_ptr<DcpResponse> dcpResponse, UpdateFlowControl& ufc) {
    if (!dcpResponse) {
        return cb::engine_errc::invalid_arguments;
    }

    if (!isActive()) {
        // If the Stream isn't active, *but* the object is still receiving
        // messages from the DcpConsumer that means the stream is still
        // registered in the streams map and hence we should ignore any
        // messages (until STREAM_END is received and the stream is removed form
        // the map).
        return cb::engine_errc::success;
    }

    auto seqno = dcpResponse->getBySeqno();
    if (seqno) {
        if (uint64_t(*seqno) <= last_seqno.load()) {
            log(spdlog::level::level_enum::warn,
                "({}) Erroneous (out of sequence) message ({}) received, "
                "with opaque: {}, its seqno ({}) is not "
                "greater than last received seqno ({}); "
                "Dropping mutation!",
                vb_,
                dcpResponse->to_string(),
                opaque_,
                *seqno,
                last_seqno.load());
            return cb::engine_errc::out_of_range;
        }
    } else if (dcpResponse->getEvent() == DcpResponse::Event::SnapshotMarker) {
        auto s = static_cast<SnapshotMarker*>(dcpResponse.get());
        uint64_t snapStart = s->getStartSeqno();
        uint64_t snapEnd = s->getEndSeqno();
        if (snapStart < last_seqno.load() && snapEnd <= last_seqno.load()) {
            log(spdlog::level::level_enum::warn,
                "({}) Erroneous snapshot marker received, with "
                "opaque: {}, its start "
                "({}), and end ({}) are less than last "
                "received seqno ({}); Dropping marker!",
                vb_,
                opaque_,
                snapStart,
                snapEnd,
                last_seqno.load());
            return cb::engine_errc::out_of_range;
        }
    }

    switch (engine->getReplicationThrottle().getStatus()) {
    case ReplicationThrottle::Status::Disconnect:
        log(spdlog::level::level_enum::warn,
            "{} Disconnecting the connection as there is "
            "no memory to complete replication",
            vb_);
        return cb::engine_errc::disconnect;
    case ReplicationThrottle::Status::Process:
        if (buffer.empty() && !alwaysBufferOperations) {
            // Memory available and no message buffered -> process the response
            const auto ret = processMessage(dcpResponse.get());
            const auto err = ret.getError();
            if (err == cb::engine_errc::no_memory) {
                if (engine->getReplicationThrottle().doDisconnectOnNoMem()) {
                    log(spdlog::level::level_enum::warn,
                        "{} Disconnecting the connection as there is no "
                        "memory to complete replication; process dcp "
                        "event returned no memory",
                        vb_);
                    return cb::engine_errc::disconnect;
                }
            }
            if (err == cb::engine_errc::success && seqno) {
                last_seqno.store(*seqno);
            }
            if (err != cb::engine_errc::temporary_failure &&
                err != cb::engine_errc::no_memory) {
                return err;
            }
        }
        break;
    case ReplicationThrottle::Status::Pause:
        /* Do nothing specific here, we buffer item for this case and
           other cases below */
        break;
    }

    // Only buffer if the stream is not dead
    if (isActive()) {
        buffer.push({std::move(dcpResponse), ufc.release()});
    }
    return cb::engine_errc::temporary_failure;
}

process_items_error_t PassiveStream::processBufferedMessages(
        uint32_t& processed_bytes, size_t batchSize) {
    std::unique_lock<std::mutex> lh(buffer.bufMutex);
    uint32_t count = 0;
    uint32_t total_bytes_processed = 0;
    bool failed = false, noMem = false;

    while (count < batchSize && !buffer.messages.empty()) {
        /* If the stream is in dead state we should not process any remaining
           items in the buffer, we should rather clear them */
        if (!isActive()) {
            total_bytes_processed += clearBuffer_UNLOCKED();
            processed_bytes = total_bytes_processed;
            return all_processed;
        }

        // MB-31410: The front-end thread can process new incoming messages
        // only /after/ all the buffered ones have been processed.
        // So, here we get only a reference. We remove the message from the
        // buffer later, only /after/ we have processed it.
        // That is because the front-end thread checks if buffer.empty() for
        // deciding if it's time to start again processing new incoming
        // mutations. That happens in PassiveStream::messageReceived.
        auto [response, message_bytes] = buffer.moveFromFront(lh);

        // Release bufMutex whilst we attempt to process the message
        // a lock inversion exists with connManager if we hold this.
        lh.unlock();

        // MB-31410: Only used for testing
        processBufferedMessages_postFront_Hook();

        const auto seqno = response->getBySeqno();

        const auto ret = processMessage(response.get());

        const auto err = ret.getError();
        if (err == cb::engine_errc::temporary_failure ||
            err == cb::engine_errc::no_memory) {
            failed = true;
            if (err == cb::engine_errc::no_memory) {
                noMem = true;
            }
        }

        // If we failed and the stream is not dead, just break the loop and
        // return. We will try again with processing the message at the next
        // run.
        // Note:
        //     1) no need to re-acquire bufMutex here
        //     2) we have not removed the unique_ptr from the buffer yet, but
        //        we must give the item back to the buffer queue
        if (failed && isActive()) {
            lh.lock();
            // isActive should be false if the queue was emptied, but check
            // anyway so we're more robust against any future code changes to
            // isActive and closeStream
            if (!buffer.messages.empty()) {
                buffer.moveToFront(lh, {std::move(response), message_bytes});
            }
            lh.unlock();
            break;
        }

        // At this point we have processed the message successfully,
        // then we can remove it from the buffer.
        // Note: we need to re-acquire bufMutex to update the buffer safely
        lh.lock();
        buffer.pop_front(lh);

        count++;
        if (err != cb::engine_errc::out_of_range) {
            total_bytes_processed += message_bytes;
        }
        if (err == cb::engine_errc::success && seqno) {
            last_seqno.store(*seqno);
        }
    }

    processed_bytes = total_bytes_processed;

    if (failed) {
        if (noMem && engine->getReplicationThrottle().doDisconnectOnNoMem()) {
            log(spdlog::level::level_enum::warn,
                "{} Processor task indicating disconnection as "
                "there is no memory to complete replication; process dcp "
                "event returned no memory ",
                vb_);
            return stop_processing;
        }
        return cannot_process;
    }

    return all_processed;
}

cb::engine_errc PassiveStream::processMessageInner(
        MutationConsumerMessage* message) {
    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return cb::engine_errc::disconnect;
    }

    if (uint64_t(*message->getBySeqno()) < cur_snapshot_start.load() ||
        uint64_t(*message->getBySeqno()) > cur_snapshot_end.load()) {
        log(spdlog::level::level_enum::warn,
            "({}) Erroneous {} [sequence "
            "number does not fall in the expected snapshot range : "
            "{{snapshot_start ({}) <= seq_no ({}) <= "
            "snapshot_end ({})]; Dropping the {}!",
            vb_,
            message->to_string(),
            cur_snapshot_start.load(),
            *message->getBySeqno(),
            cur_snapshot_end.load(),
            message->to_string());
        return cb::engine_errc::out_of_range;
    }

    // MB-17517: Check for the incoming item's CAS validity. We /shouldn't/
    // receive anything without a valid CAS, however given that versions without
    // this check may send us "bad" CAS values, we should regenerate them (which
    // is better than rejecting the data entirely).
    if (!Item::isValidCas(message->getItem()->getCas())) {
        log(spdlog::level::level_enum::warn,
            "Invalid CAS ({:#x}) received for {} {{{}, seqno:{}}}. "
            "Regenerating new CAS",
            message->getItem()->getCas(),
            message->to_string(),
            vb_,
            message->getItem()->getBySeqno());
        message->getItem()->setCas();
    }

    auto ret = cb::engine_errc::failed;
    DeleteSource deleteSource = DeleteSource::Explicit;

    switch (message->getEvent()) {
    case DcpResponse::Event::Mutation:
        ret = engine->getKVBucket()->setWithMeta(*message->getItem(),
                                                 0,
                                                 nullptr,
                                                 consumer->getCookie(),
                                                 permittedVBStates,
                                                 CheckConflicts::No,
                                                 true,
                                                 GenerateBySeqno::No,
                                                 GenerateCas::No,
                                                 message->getExtMetaData());
        break;
    case DcpResponse::Event::Expiration:
        deleteSource = DeleteSource::TTL;
        // fallthrough with deleteSource updated
    case DcpResponse::Event::Deletion:
        if (message->getItem()->getNBytes() == 0) {
            uint64_t delCas = 0;
            ItemMetaData meta = message->getItem()->getMetaData();
            ret = engine->getKVBucket()->deleteWithMeta(
                    message->getItem()->getKey(),
                    delCas,
                    nullptr,
                    message->getVBucket(),
                    consumer->getCookie(),
                    permittedVBStates,
                    CheckConflicts::No,
                    meta,
                    GenerateBySeqno::No,
                    GenerateCas::No,
                    *message->getBySeqno(),
                    message->getExtMetaData(),
                    deleteSource);
            if (ret == cb::engine_errc::no_such_key) {
                ret = cb::engine_errc::success;
            }
        } else {
            // The deletion has a value, send it through the setWithMeta path to
            // process it correctly
            ret = engine->getKVBucket()->setWithMeta(*message->getItem(),
                                                     0,
                                                     nullptr,
                                                     consumer->getCookie(),
                                                     permittedVBStates,
                                                     CheckConflicts::No,
                                                     true,
                                                     GenerateBySeqno::No,
                                                     GenerateCas::No,
                                                     message->getExtMetaData());
        }
        break;
    case DcpResponse::Event::Prepare:
        ret = engine->getKVBucket()->prepare(*message->getItem(),
                                             consumer->getCookie());
        // If the stream has received and successfully processed a pending
        // SyncWrite, then we have to flag that the Replica must notify the
        // DurabilityMonitor at snapshot-end received for the DM to move the
        // HighPreparedSeqno.
        if (ret == cb::engine_errc::success) {
            cur_snapshot_prepare.store(true);
        }
        break;
    case DcpResponse::Event::Commit:
    case DcpResponse::Event::Abort:
    case DcpResponse::Event::SetVbucket:
    case DcpResponse::Event::StreamReq:
    case DcpResponse::Event::StreamEnd:
    case DcpResponse::Event::SnapshotMarker:
    case DcpResponse::Event::AddStream:
    case DcpResponse::Event::SystemEvent:
    case DcpResponse::Event::SeqnoAcknowledgement:
    case DcpResponse::Event::OSOSnapshot:
    case DcpResponse::Event::SeqnoAdvanced:
        throw std::invalid_argument(
                "PassiveStream::processMessageInner: invalid event " +
                std::string(message->to_string()));
    }

    return ret;
}

void PassiveStream::seqnoAck(int64_t seqno) {
    // Only send a seqnoAck if we have an active stream that the producer has
    // responded with Success to the stream request
    if (!isActive() || isPending()) {
        log(spdlog::level::level_enum::warn,
            "{} Could not ack seqno {} because stream was in StreamState:{} "
            "Expected it to be in state {}",
            vb_,
            seqno,
            to_string(state_.load()),
            to_string(StreamState::Reading));
        return;
    }

    {
        std::lock_guard<std::mutex> lh(streamMutex);
        if (!isActive()) {
            return;
        }

        pushToReadyQ(
                std::make_unique<SeqnoAcknowledgement>(opaque_, vb_, seqno));
    }
    notifyStreamReady();
}

std::string PassiveStream::to_string(StreamState st) {
    switch (st) {
    case StreamState::Pending:
        return "pending";
    case StreamState::Reading:
        return "reading";
    case StreamState::Dead:
        return "dead";
    }
    throw std::invalid_argument("PassiveStream::to_string(StreamState): " +
                                std::to_string(int(st)));
}

cb::engine_errc PassiveStream::processCommit(
        const CommitSyncWriteConsumer& commit) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    // The state of the VBucket should never change during a commit, because
    // VBucket::commit() may generated expired items.
    // NOTE: Theoretically this will never occur, because we kill all streams
    // when changing the VBucket state.
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (!permittedVBStates.test(vb->getState())) {
        return cb::engine_errc::not_my_vbucket;
    }

    return vb->commit(rlh,
                      commit.getKey(),
                      commit.getPreparedSeqno(),
                      *commit.getBySeqno(),
                      vb->lockCollections(commit.getKey()));
}

cb::engine_errc PassiveStream::processAbort(
        const AbortSyncWriteConsumer& abort) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    // The state of the VBucket should never change during an abort, because
    // VBucket::abort() may generated expired items.
    // NOTE: Theoretically this will never occur, because we kill all streams
    // when changing the VBucket state.
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());

    if (!permittedVBStates.test(vb->getState())) {
        return cb::engine_errc::not_my_vbucket;
    }

    return vb->abort(rlh,
                     abort.getKey(),
                     abort.getPreparedSeqno(),
                     abort.getAbortSeqno(),
                     vb->lockCollections(abort.getKey()));
}

cb::engine_errc PassiveStream::processSystemEvent(
        const SystemEventMessage& event) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (!permittedVBStates.test(vb->getState())) {
        return cb::engine_errc::not_my_vbucket;
    }

    cb::engine_errc rv = cb::engine_errc::success;

    if (flatBuffersSystemEventsEnabled) {
        rv = processSystemEventFlatBuffers(
                *vb, static_cast<const SystemEventConsumerMessage&>(event));
    } else {
        rv = processSystemEvent(*vb, event);
    }

    return rv;
}

cb::engine_errc PassiveStream::processSystemEvent(
        VBucket& vb, const SystemEventMessage& event) {
    Expects(!flatBuffersSystemEventsEnabled);
    // Depending on the event, extras is different and key may even be empty
    // The specific handler will know how to interpret.
    using mcbp::systemevent::id;
    switch (event.getSystemEvent()) {
    case id::CreateCollection:
        return processCreateCollection(vb, CreateCollectionEvent(event));
    case id::DeleteCollection:
        return processDropCollection(vb, DropCollectionEvent(event));
    case id::CreateScope:
        return processCreateScope(vb, CreateScopeEvent(event));
    case id::DropScope:
        return processDropScope(vb, DropScopeEvent(event));
    case id::ModifyCollection:
        [[fallthrough]]; // invalid event for non-FlatBuffers
    case id::FlushCollection:
        // Event unused since epoch of system events (7.0)
        return cb::engine_errc::invalid_arguments;
    }
    folly::assume_unreachable();
}

cb::engine_errc PassiveStream::processSystemEventFlatBuffers(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    Expects(flatBuffersSystemEventsEnabled);
    // Depending on the event, extras is different and key may even be empty
    // The specific handler will know how to interpret.
    using mcbp::systemevent::id;
    switch (event.getSystemEvent()) {
    case id::CreateCollection:
        return processCreateCollection(vb, event);
    case id::DeleteCollection:
        return processDropCollection(vb, event);
    case id::CreateScope:
        return processCreateScope(vb, event);
    case id::DropScope:
        return processDropScope(vb, event);
    case id::ModifyCollection:
        return processModifyCollection(vb, event);
    case id::FlushCollection:
        // Event unused since epoch of system events (7.0)
        return cb::engine_errc::invalid_arguments;
    }
    folly::assume_unreachable();
}

cb::engine_errc PassiveStream::processCreateCollection(
        VBucket& vb, const CreateCollectionEvent& event) {
    try {
        // This creation event comes from a node which didn't support
        // FlatBuffers. Assume "old" node so we default the CanDeduplicate and
        // Metered settings to Yes.
        vb.replicaCreateCollection(
                event.getManifestUid(),
                {event.getScopeID(), event.getCollectionID()},
                event.getKey(),
                event.getMaxTtl(),
                Collections::Metered::No,
                CanDeduplicate::Yes,
                event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processCreateCollection {} exception {}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processDropCollection(
        VBucket& vb, const DropCollectionEvent& event) {
    try {
        vb.replicaDropCollection(event.getManifestUid(),
                                 event.getCollectionID(),
                                 event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processDropCollection {} exception {}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processCreateScope(
        VBucket& vb, const CreateScopeEvent& event) {
    try {
        vb.replicaCreateScope(event.getManifestUid(),
                              event.getScopeID(),
                              event.getKey(),
                              event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processCreateScope {} exception {}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processDropScope(VBucket& vb,
                                                const DropScopeEvent& event) {
    try {
        vb.replicaDropScope(
                event.getManifestUid(), event.getScopeID(), event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processDropScope {} exception {}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processCreateCollection(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        // Decompose the FlatBuffers data.
        // Here we will use defaults when the producer is older.
        // The vbucket is now informed of the collection and it will regenerate
        // a new FlatBuffers system event using *this* system's schema+data.
        const auto& collection =
                Collections::VB::Manifest::getCollectionFlatbuffer(
                        event.getEventData());
        cb::ExpiryLimit maxTtl;
        if (collection.ttlValid()) {
            maxTtl = std::chrono::seconds(collection.maxTtl());
        }

        vb.replicaCreateCollection(
                Collections::ManifestUid{collection.uid()},
                {collection.scopeId(), collection.collectionId()},
                event.getKey(),
                maxTtl,
                Collections::getMetered(collection.metered()),
                getCanDeduplicateFromHistory(collection.history()),
                *event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processCreateCollection FlatBuffers {} exception "
            "{}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processModifyCollection(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        const auto& collection =
                Collections::VB::Manifest::getCollectionFlatbuffer(
                        event.getEventData());

        cb::ExpiryLimit maxTtl;
        if (collection.ttlValid()) {
            maxTtl = std::chrono::seconds(collection.maxTtl());
        }

        vb.replicaModifyCollection(
                Collections::ManifestUid{collection.uid()},
                collection.collectionId(),
                maxTtl,
                Collections::getMetered(collection.metered()),
                getCanDeduplicateFromHistory(collection.history()),
                *event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processModifyCollection flatbuffer {} exception {}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processDropCollection(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        const auto* collection =
                Collections::VB::Manifest::getDroppedCollectionFlatbuffer(
                        event.getEventData());
        vb.replicaDropCollection(Collections::ManifestUid{collection->uid()},
                                 collection->collectionId(),
                                 *event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processDropCollection FlatBuffers {} exception {}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processCreateScope(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        const auto* scope = Collections::VB::Manifest::getScopeFlatbuffer(
                event.getEventData());
        vb.replicaCreateScope(Collections::ManifestUid{scope->uid()},
                              scope->scopeId(),
                              event.getKey(),
                              *event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processCreateScope FlatBuffers {} exception {}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processDropScope(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        const auto* scope =
                Collections::VB::Manifest::getDroppedScopeFlatbuffer(
                        event.getEventData());
        vb.replicaDropScope(Collections::ManifestUid{scope->uid()},
                            scope->scopeId(),
                            *event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processDropScope FlatBuffers {} exception {}",
            vb.getId(),
            e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

// Helper function to avoid a Monotonic violation (same end-seqno) for the
// !HISTORY->HISTORY snapshot
static bool mustAssignEndSeqno(SnapshotMarker* marker, uint64_t endSeqno) {
    if (marker->getFlags() & MARKER_FLAG_MEMORY) {
        // Always assign and catch monotonic violations
        return true;
    }

    if (marker->getFlags() & MARKER_FLAG_HISTORY &&
        marker->getEndSeqno() == endSeqno) {
        // HISTORY disk snapshot marker can follow !HISTORY disk and they have
        // the same end-seqno. Skip the assignment and avoid the monotonic
        // exception
        return false;
    }

    // Always assign and catch monotonic violations
    return true;
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return;
    }
    // Vbucket must be in a permitted state to apply the snapshot
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (!permittedVBStates.test(vb->getState())) {
        return;
    }

    // cur_snapshot_start is initialised to 0 so only set it for numbers > 0,
    // as the first snapshot maybe have a snap_start_seqno of 0.
    if (marker->getStartSeqno() > 0) {
        cur_snapshot_start.store(marker->getStartSeqno());
    }

    if (mustAssignEndSeqno(marker, cur_snapshot_end)) {
        cur_snapshot_end.store(marker->getEndSeqno());
    }
    const auto prevSnapType = cur_snapshot_type.load();
    cur_snapshot_type.store((marker->getFlags() & MARKER_FLAG_DISK)
                                    ? Snapshot::Disk
                                    : Snapshot::Memory);

    auto checkpointType = marker->getFlags() & MARKER_FLAG_DISK
                                  ? CheckpointType::Disk
                                  : CheckpointType::Memory;

    const auto historical = marker->getFlags() & MARKER_FLAG_HISTORY
                                    ? CheckpointHistorical::Yes
                                    : CheckpointHistorical::No;

    // Check whether the snapshot can be considered as an initial disk
    // checkpoint for the replica.
    if (checkpointType == CheckpointType::Disk && vb->getHighSeqno() == 0) {
        checkpointType = CheckpointType::InitialDisk;
    }

    auto& ckptMgr = *vb->checkpointManager;

    // If this stream doesn't support SyncReplication (i.e. the producer
    // is a pre-MadHatter version) then we should consider the HCS to be
    // present but zero for disk snapshot (not possible for any
    // SyncWrites to have completed yet). If SyncReplication is
    // supported then use the value from the marker.
    const std::optional<uint64_t> hcs =
            (marker->getFlags() & MARKER_FLAG_DISK) && !supportsSyncReplication
                    ? 0
                    : marker->getHighCompletedSeqno();

    if (marker->getFlags() & MARKER_FLAG_DISK && !hcs) {
        const auto msg = fmt::format(
                "PassiveStream::processMarker: stream:{} {}, flags:{}, "
                "flagsDecoded:{}, snapStart:{}, snapEnd:{}, HCS:{} - "
                "missing HCS",
                name_,
                vb_,
                marker->getFlags(),
                dcpMarkerFlagsToString(marker->getFlags()),
                marker->getStartSeqno(),
                marker->getEndSeqno(),
                to_string_or_none(hcs));
        throw std::logic_error(msg);
    }

    if (marker->getFlags() & MARKER_FLAG_DISK) {
        // A replica could receive a duplicate DCP prepare during a disk
        // snapshot if it had previously received an uncompleted prepare.
        // We can receive a disk snapshot when we either:
        //     a) First connect
        //     b) Get cursor dropped by the active
        //
        // We selectively allow these prepares to overwrite the old one by
        // setting a duplicate prepare window in the vBucket. This will
        // allow any currently outstanding prepares to be overwritten, but
        // not any new ones.
        vb->setDuplicatePrepareWindow();
    }

    // We could be connected to a non sync-repl, so if the max-visible is
    // not transmitted (optional is false), set visible to snap-end
    const auto visibleSeq =
            marker->getMaxVisibleSeqno().value_or(marker->getEndSeqno());

    if (cur_snapshot_end < visibleSeq) {
        const auto msg = fmt::format(
                "PassiveStream::processMarker: snapEnd:{} < "
                "visibleSnapEnd:{}, snapStart:{}, hcs:{}, "
                "checkpointType:{}, historical:{}",
                cur_snapshot_end.load(),
                visibleSeq,
                cur_snapshot_start.load(),
                hcs ? std::to_string(*hcs) : "nullopt",
                ::to_string(checkpointType),
                ::to_string(historical));
        throw std::logic_error(msg);
    }

    if (checkpointType == CheckpointType::InitialDisk) {
        // Case: receiving the first snapshot in a Disk snapshot.
        // Note that replica may never execute here as the active may switch
        // directly to in-memory and send the first snapshot in a Memory
        // snapshot.

        vb->setReceivingInitialDiskSnapshot(true);
        ckptMgr.createSnapshot(cur_snapshot_start.load(),
                               cur_snapshot_end.load(),
                               hcs,
                               checkpointType,
                               visibleSeq,
                               historical);
    } else {
        // Case: receiving any type of snapshot (Disk/Memory).

        if (marker->getFlags() & MARKER_FLAG_CHK) {
            ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                   cur_snapshot_end.load(),
                                   hcs,
                                   checkpointType,
                                   visibleSeq,
                                   historical);
        } else {
            // MB-42780: In general we cannot merge multiple snapshots into
            // the same checkpoint. The only exception is for when replica
            // receives multiple Memory checkpoints in a row.
            // Since 6.5.0 the Active behaves correctly with regard to that
            // (ie, the Active always sets the MARKER_FLAG_CHK in a snapshot
            // transition tha involves Disk snapshots), but older Producers
            // may still miss the MARKER_FLAG_CHK.
            if (prevSnapType == Snapshot::Memory &&
                cur_snapshot_type == Snapshot::Memory) {
                ckptMgr.extendOpenCheckpoint(cur_snapshot_end.load(),
                                             visibleSeq);
            } else {
                ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                       cur_snapshot_end.load(),
                                       hcs,
                                       checkpointType,
                                       visibleSeq,
                                       historical);
            }
        }
    }

    if (marker->getFlags() & MARKER_FLAG_ACK) {
        cur_snapshot_ack = true;
    }
}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getKVBucket()->setVBucketState(
            vb_, state->getState(), {}, TransferVB::Yes);
    {
        std::lock_guard<std::mutex> lh(streamMutex);
        pushToReadyQ(std::make_unique<SetVBucketStateResponse>(
                opaque_, cb::mcbp::Status::Success));
    }
    notifyStreamReady();
}

void PassiveStream::handleSnapshotEnd(uint64_t seqno) {
    auto vb = engine->getVBucket(vb_);
    if (!vb) {
        return;
    }

    if (seqno != cur_snapshot_end.load()) {
        return;
    }

    if (cur_snapshot_type.load() == Snapshot::Disk) {
        vb->setReceivingInitialDiskSnapshot(false);
    }

    if (cur_snapshot_ack) {
        {
            std::lock_guard<std::mutex> lh(streamMutex);
            pushToReadyQ(std::make_unique<SnapshotMarkerResponse>(
                    opaque_, cb::mcbp::Status::Success));
        }
        notifyStreamReady();
        cur_snapshot_ack = false;
    }

    // Notify the PassiveDM that the snapshot-end mutation has been received on
    // PassiveStream, if the snapshot contains at least one Prepare. That is
    // necessary for unblocking the High Prepared Seqno in PassiveDM. Note that
    // the HPS is what the PassiveDM acks back to the Active. See comments in
    // PassiveDM for details.

    // Disk snapshots are subject to deduplication, and may be missing purged
    // aborts. We must notify the PDM even if we have not seen a prepare, to
    // account for possible unseen prepares.
    if (cur_snapshot_prepare || cur_snapshot_type.load() == Snapshot::Disk) {
        vb->notifyPassiveDMOfSnapEndReceived(seqno);
        cur_snapshot_prepare.store(false);
    }
}

void PassiveStream::addStats(const AddStatFn& add_stat, CookieIface& c) {
    Stream::addStats(add_stat, c);

    try {
        size_t bufferItems = 0;
        size_t bufferBytes = 0;
        {
            std::lock_guard<std::mutex> lg(buffer.bufMutex);
            bufferItems = buffer.messages.size();
            bufferBytes = buffer.bytes;
        }
        add_casted_stat("buffer_items", bufferItems, add_stat, c);
        add_casted_stat("buffer_bytes", bufferBytes, add_stat, c);
        add_casted_stat("last_received_seqno", last_seqno.load(), add_stat, c);
        add_casted_stat(
                "ready_queue_memory", getReadyQueueMemory(), add_stat, c);
        add_casted_stat("cur_snapshot_type",
                        ::to_string(cur_snapshot_type.load()),
                        add_stat,
                        c);

        if (cur_snapshot_type.load() != Snapshot::None) {
            add_casted_stat("cur_snapshot_start",
                            cur_snapshot_start.load(),
                            add_stat,
                            c);
            add_casted_stat(
                    "cur_snapshot_end", cur_snapshot_end.load(), add_stat, c);
        }

        add_casted_stat("cur_snapshot_prepare",
                        cur_snapshot_prepare.load(),
                        add_stat,
                        c);

        auto stream_req_value = createStreamReqValue();

        if (!stream_req_value.empty()) {
            add_casted_stat(
                    "vb_manifest_uid", stream_req_value.c_str(), add_stat, c);
        }

    } catch (std::exception& error) {
        EP_LOG_WARN("PassiveStream::addStats: Failed to build stats: {}",
                    error.what());
    }
}

std::unique_ptr<DcpResponse> PassiveStream::next() {
    std::lock_guard<std::mutex> lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return nullptr;
    }

    return popFromReadyQ();
}

uint32_t PassiveStream::clearBuffer_UNLOCKED() {
    uint32_t unackedBytes = buffer.bytes;
    buffer.messages.clear();
    buffer.bytes = 0;
    return unackedBytes;
}

bool PassiveStream::transitionState(StreamState newState) {
    log(spdlog::level::level_enum::debug,
        "PassiveStream::transitionState: ({}) "
        "Transitioning from {} to {}",
        vb_,
        to_string(state_.load()),
        to_string(newState));

    if (state_ == newState) {
        return false;
    }

    bool validTransition = false;
    switch (state_.load()) {
    case StreamState::Pending:
        if (newState == StreamState::Reading || newState == StreamState::Dead) {
            validTransition = true;
        }
        break;
    case StreamState::Reading:
        if (newState == StreamState::Dead) {
            validTransition = true;
        }
        break;

    case StreamState::Dead:
        // Once 'dead' shouldn't transition away from it.
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "PassiveStream::transitionState:"
                " newState (which is" +
                to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state_.load()) + ")");
    }

    state_ = newState;
    return true;
}

void PassiveStream::notifyStreamReady() {
    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return;
    }

    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        consumer->notifyStreamReady(vb_);
    }
}

const std::string PassiveStream::createStreamReqValue() const {
    nlohmann::json stream_req_json;
    std::ostringstream ostr;
    ostr << std::hex << static_cast<uint64_t>(vb_manifest_uid);
    stream_req_json["uid"] = ostr.str();
    return stream_req_json.dump();
}

template <typename... Args>
void PassiveStream::log(spdlog::level::level_enum severity,
                        const char* fmt,
                        Args... args) const {
    auto consumer = consumerPtr.lock();
    if (consumer) {
        consumer->getLogger().log(severity, fmt, args...);
    } else {
        if (getGlobalBucketLogger()->should_log(severity)) {
            getGlobalBucketLogger()->log(
                    severity,
                    std::string{passiveStreamLoggingPrefix}.append(fmt).data(),
                    args...);
        }
    }
}

void PassiveStream::maybeLogMemoryState(cb::engine_errc status,
                                        const std::string& msgType,
                                        int64_t seqno) {
    bool previousNoMem = isNoMemory.load();
    if (status == cb::engine_errc::no_memory && !previousNoMem) {
        log(spdlog::level::level_enum::warn,
            "{} Got error '{}' while trying to process "
            "{} with seqno:{}",
            vb_,
            cb::to_string(status),
            msgType,
            seqno);
        isNoMemory.store(true);
    } else if (status == cb::engine_errc::success && previousNoMem) {
        log(spdlog::level::level_enum::info,
            "{} PassiveStream resuming after no-memory backoff",
            vb_);
        isNoMemory.store(false);
    }
}

bool PassiveStream::Buffer::empty() const {
    std::lock_guard<std::mutex> lh(bufMutex);
    return messages.empty();
}

void PassiveStream::Buffer::push(PassiveStream::Buffer::BufferType bufferItem) {
    std::lock_guard<std::mutex> lg(bufMutex);
    bytes += bufferItem.second;
    messages.emplace_back(std::move(bufferItem));
}

void PassiveStream::Buffer::pop_front(const std::unique_lock<std::mutex>& lh) {
    if (messages.empty()) {
        return;
    }
    if (messages.front().first) {
        bytes -= messages.front().second;
    }

    messages.pop_front();
}

PassiveStream::Buffer::BufferType PassiveStream::Buffer::moveFromFront(
        const std::unique_lock<std::mutex>& lh) {
    bytes -= messages.front().second;
    return {std::move(messages.front().first), messages.front().second};
}

void PassiveStream::Buffer::moveToFront(
        const std::unique_lock<std::mutex>& lh,
        PassiveStream::Buffer::BufferType bufferItem) {
    bytes += bufferItem.second;
    messages.front().first = std::move(bufferItem.first);
}

std::string PassiveStream::Labeller::getLabel(const char* name) const {
    return fmt::format("PassiveStream({} {})::{}",
                       stream.getVBucket(),
                       stream.getName(),
                       name);
}

PassiveStream::ProcessMessageResult PassiveStream::processMessage(
        gsl::not_null<DcpResponse*> response) {
    auto vb = engine->getVBucket(vb_);
    if (!vb) {
        return {*this, cb::engine_errc::not_my_vbucket, {}};
    }

    cb::engine_errc ret = cb::engine_errc::success;
    auto* resp = response.get();
    switch (resp->getEvent()) {
    case DcpResponse::Event::Mutation:
        ret = processMessageInner(dynamic_cast<MutationConsumerMessage*>(resp));
        break;
    case DcpResponse::Event::Deletion:
        ret = processMessageInner(dynamic_cast<MutationConsumerMessage*>(resp));
        break;
    case DcpResponse::Event::Expiration:
        ret = processMessageInner(dynamic_cast<MutationConsumerMessage*>(resp));
        break;
    case DcpResponse::Event::Prepare: {
        auto* prepare = dynamic_cast<MutationConsumerMessage*>(resp);
        ret = processMessageInner(prepare);
        if (ret == cb::engine_errc::success) {
            Expects(prepare->getItem()->getBySeqno() ==
                    engine->getVBucket(vb_)->getHighSeqno());
        }
    } break;
    case DcpResponse::Event::Commit:
        ret = processCommit(dynamic_cast<CommitSyncWriteConsumer&>(*resp));
        break;
    case DcpResponse::Event::Abort:
        ret = processAbort(dynamic_cast<AbortSyncWriteConsumer&>(*resp));
        break;
    case DcpResponse::Event::SnapshotMarker:
        processMarker(dynamic_cast<SnapshotMarker*>(resp));
        break;
    case DcpResponse::Event::SetVbucket:
        processSetVBucketState(dynamic_cast<SetVBucketState*>(resp));
        break;
    case DcpResponse::Event::StreamEnd: {
        streamDeadHook();
        std::lock_guard<std::mutex> lh(streamMutex);
        transitionState(StreamState::Dead);
    } break;
    case DcpResponse::Event::SystemEvent: {
        ret = processSystemEvent(*dynamic_cast<SystemEventMessage*>(resp));
        break;
    }
    case DcpResponse::Event::StreamReq:
    case DcpResponse::Event::AddStream:
    case DcpResponse::Event::SeqnoAcknowledgement:
    case DcpResponse::Event::OSOSnapshot:
    case DcpResponse::Event::SeqnoAdvanced:
        // These are invalid events for this path, they are handled by
        // the DcpConsumer class
        throw std::invalid_argument(
                "PassiveStream::processMessage: invalid event " +
                std::string(resp->to_string()));
    }

    const auto seqno = resp->getBySeqno();

    const auto* mutation = dynamic_cast<MutationConsumerMessage*>(resp);
    if (mutation) {
        Expects(seqno);
        if (ret != cb::engine_errc::success) {
            // ENOMEM logging is handled by maybeLogMemoryState
            if (ret != cb::engine_errc::no_memory) {
                log(spdlog::level::level_enum::warn,
                    "PassiveStream::processMessage: {} Got error '{}' while "
                    "trying to process {} with seqno:{} cid:{}",
                    vb_,
                    cb::to_string(ret),
                    resp->to_string(),
                    *seqno,
                    mutation->getItem()->getKey().getCollectionID());
            }
        }
        maybeLogMemoryState(ret, resp->to_string(), *seqno);
    } else {
        if (ret != cb::engine_errc::success) {
            log(spdlog::level::level_enum::warn,
                "PassiveStream::processMessage: {} Got error '{}' while trying "
                "to process {} with seqno:{}",
                vb_,
                cb::to_string(ret),
                resp->to_string(),
                seqno ? std::to_string(*seqno) : "N/A");
        }
    }

    return {*this, ret, seqno};
}

PassiveStream::ProcessMessageResult::~ProcessMessageResult() {
    if (err == cb::engine_errc::success && seqno) {
        stream->handleSnapshotEnd(*seqno);
    }
}