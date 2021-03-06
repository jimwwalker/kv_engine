/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "dcp/producer.h"
#include "backfill.h"
#include "collections/filter.h"
#include "collections/manager.h"
#include "collections/vbucket_filter.h"
#include "common.h"
#include "dcp/backfill-manager.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "failover-table.h"

#include <memcached/server_api.h>
#include <vector>

const std::chrono::seconds DcpProducer::defaultDcpNoopTxInterval(20);

DcpProducer::BufferLog::State DcpProducer::BufferLog::getState_UNLOCKED() {
    if (isEnabled_UNLOCKED()) {
        if (isFull_UNLOCKED()) {
            return Full;
        } else {
            return SpaceAvailable;
        }
    }
    return Disabled;
}

void DcpProducer::BufferLog::setBufferSize(size_t maxBytes) {
    WriterLockHolder lh(logLock);
    this->maxBytes = maxBytes;
    if (maxBytes == 0) {
        bytesSent = 0;
        ackedBytes = 0;
    }
}

bool DcpProducer::BufferLog::insert(size_t bytes) {
    WriterLockHolder wlh(logLock);
    bool inserted = false;
    // If the log is not enabled
    // or there is space, allow the insert
    if (!isEnabled_UNLOCKED() || !isFull_UNLOCKED()) {
        bytesSent += bytes;
        inserted = true;
    }
    return inserted;
}

void DcpProducer::BufferLog::release_UNLOCKED(size_t bytes) {
    if (bytesSent >= bytes) {
        bytesSent -= bytes;
    } else {
        bytesSent = 0;
    }
}

bool DcpProducer::BufferLog::pauseIfFull() {
    ReaderLockHolder rlh(logLock);
    if (getState_UNLOCKED() == Full) {
        producer.setPaused(true);
        return true;
    }
    return false;
}

void DcpProducer::BufferLog::unpauseIfSpaceAvailable() {
    ReaderLockHolder rlh(logLock);
    if (getState_UNLOCKED() == Full) {
        LOG(EXTENSION_LOG_NOTICE,
            "%s Unable to notify paused connection "
            "because DcpProducer::BufferLog is full; ackedBytes:%lx, "
            "bytesSent:%lx, maxBytes:%lx",
            producer.logHeader(),
            ackedBytes,
            bytesSent,
            maxBytes);
    } else {
        producer.notifyPaused(true);
    }
}

void DcpProducer::BufferLog::acknowledge(size_t bytes) {
    WriterLockHolder wlh(logLock);
    State state = getState_UNLOCKED();
    if (state != Disabled) {
        release_UNLOCKED(bytes);
        ackedBytes += bytes;
        if (state == Full) {
            LOG(EXTENSION_LOG_NOTICE,
                "%s Notifying paused connection now that "
                "DcpProducer::Bufferlog is no longer full; ackedBytes:%lx, "
                "bytesSent:%lx, maxBytes:%lx",
                producer.logHeader(),
                ackedBytes,
                bytesSent,
                maxBytes);
            producer.notifyPaused(true);
        }
    }
}

void DcpProducer::BufferLog::addStats(ADD_STAT add_stat, const void *c) {
    ReaderLockHolder rlh(logLock);
    if (isEnabled_UNLOCKED()) {
        producer.addStat("max_buffer_bytes", maxBytes, add_stat, c);
        producer.addStat("unacked_bytes", bytesSent, add_stat, c);
        producer.addStat("total_acked_bytes", ackedBytes, add_stat, c);
        producer.addStat("flow_control", "enabled", add_stat, c);
    } else {
        producer.addStat("flow_control", "disabled", add_stat, c);
    }
}

DcpProducer::DcpProducer(EventuallyPersistentEngine& e,
                         const void* cookie,
                         const std::string& name,
                         uint32_t flags,
                         cb::const_byte_buffer jsonFilter,
                         bool startTask)
    : Producer(e, cookie, name),
      rejectResp(NULL),
      notifyOnly((flags & DCP_OPEN_NOTIFIER) != 0),
      lastSendTime(ep_current_time()),
      log(*this),
      itemsSent(0),
      totalBytesSent(0),
      mutationType(((flags & DCP_OPEN_NO_VALUE) != 0)
                           ? DcpProducer::MutationType::KeyOnly
                           : DcpProducer::MutationType::KeyAndValue),
      filter(e.getKVBucket()->getCollectionsManager().makeFilter(
              (flags & DCP_OPEN_COLLECTIONS) != 0,
              {reinterpret_cast<const char*>(jsonFilter.data()),
               jsonFilter.size()})) {
    setSupportAck(true);
    setReserved(true);
    setPaused(true);

    logger.setId(e.getServerApi()->cookie->get_log_info(cookie).first);
    if (notifyOnly) {
        setLogHeader("DCP (Notifier) " + getName() + " -");
    } else {
        setLogHeader("DCP (Producer) " + getName() + " -");
    }
    // Reduce the minimum log level of view engine DCP streams as they are
    // extremely noisy due to creating new stream, per vbucket,per design doc
    // every ~10s.
    if (name.find("eq_dcpq:mapreduce_view") != std::string::npos ||
        name.find("eq_dcpq:spatial_view") != std::string::npos) {
        logger.min_log_level = EXTENSION_LOG_WARNING;
    }

    engine_.setDCPPriority(getCookie(), CONN_PRIORITY_MED);
    priority.assign("medium");

    // The consumer assigns opaques starting at 0 so lets have the producer
    //start using opaques at 10M to prevent any opaque conflicts.
    noopCtx.opaque = 10000000;
    noopCtx.sendTime = ep_current_time();

    // This is for backward compatibility with Couchbase 3.0. In 3.0 we set the
    // noop interval to 20 seconds by default, but in post 3.0 releases we set
    // it to be higher by default. Starting in 3.0.1 the DCP consumer sets the
    // noop interval of the producer when connecting so in an all 3.0.1+ cluster
    // this value will be overridden. In 3.0 however we do not set the noop
    // interval so setting this value will make sure we don't disconnect on
    // accident due to the producer and the consumer having a different noop
    // interval.
    noopCtx.dcpNoopTxInterval = defaultDcpNoopTxInterval;
    noopCtx.dcpIdleTimeout = std::chrono::seconds(
            engine_.getConfiguration().getDcpIdleTimeout());
    noopCtx.pendingRecv = false;
    noopCtx.enabled = false;

    enableExtMetaData = false;
    enableValueCompression = false;

    // Cursor dropping is disabled for replication connections by default,
    // but will be enabled through a control message to support backward
    // compatibility. For all other type of DCP connections, cursor dropping
    // will be enabled by default.
    if (name.find("replication") < name.length()) {
        supportsCursorDropping = false;
    } else {
        supportsCursorDropping = true;
    }

    backfillMgr.reset(new BackfillManager(engine_));

    if (startTask) {
        createCheckpointProcessorTask();
        scheduleCheckpointProcessorTask();
    }
}

DcpProducer::~DcpProducer() {
    backfillMgr.reset();
    delete rejectResp;

    if (checkpointCreatorTask) {
        ExecutorPool::get()->cancel(checkpointCreatorTask->getId());
    }
}

ENGINE_ERROR_CODE DcpProducer::streamRequest(uint32_t flags,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint64_t vbucket_uuid,
                                             uint64_t snap_start_seqno,
                                             uint64_t snap_end_seqno,
                                             uint64_t *rollback_seqno,
                                             dcp_add_failover_log callback) {

    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    VBucketPtr vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    if ((flags & DCP_ADD_STREAM_ACTIVE_VB_ONLY) &&
        (vb->getState() != vbucket_state_active)) {
        LOG(EXTENSION_LOG_NOTICE, "%s (vb %d) Stream request failed because "
            "the vbucket is in %s state, only active vbuckets were requested",
            logHeader(), vbucket, vb->toString(vb->getState()));
        return ENGINE_NOT_MY_VBUCKET;
    }
    if (vb->checkpointManager.getOpenCheckpointId() == 0) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "this vbucket is in backfill state", logHeader(), vbucket);
        return ENGINE_TMPFAIL;
    }

    if (!notifyOnly && start_seqno > end_seqno) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "the start seqno (%" PRIu64 ") is larger than the end seqno "
            "(%" PRIu64 "); "
            "Incorrect params passed by the DCP client",
            logHeader(), vbucket, start_seqno, end_seqno);
        return ENGINE_ERANGE;
    }

    if (!notifyOnly && !(snap_start_seqno <= start_seqno &&
        start_seqno <= snap_end_seqno)) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "the snap start seqno (%" PRIu64 ") <= start seqno (%" PRIu64 ")"
            " <= snap end seqno (%" PRIu64 ") is required", logHeader(), vbucket,
            snap_start_seqno, start_seqno, snap_end_seqno);
        return ENGINE_ERANGE;
    }

    bool add_vb_conn_map = true;
    {
        // Need to synchronise the search and conditional erase,
        // therefore use external locking here.
        std::lock_guard<StreamsMap> guard(streams);
        auto it = streams.find(vbucket, guard);
        if (it.second) {
            auto& stream = it.first;
            if (stream->isActive()) {
                LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed"
                    " because a stream already exists for this vbucket",
                    logHeader(), vbucket);
                return ENGINE_KEY_EEXISTS;
            } else {
                streams.erase(vbucket, guard);

                // Don't need to add an entry to vbucket-to-conns map
                add_vb_conn_map = false;
            }
        }
    }

    // If we are a notify stream then we can't use the start_seqno supplied
    // since if it is greater than the current high seqno then it will always
    // trigger a rollback. As a result we should use the current high seqno for
    // rollback purposes.
    uint64_t notifySeqno = start_seqno;
    if (notifyOnly && start_seqno > static_cast<uint64_t>(vb->getHighSeqno())) {
        start_seqno = static_cast<uint64_t>(vb->getHighSeqno());
    }

    std::pair<bool, std::string> need_rollback =
            vb->failovers->needsRollback(start_seqno,
                                         vb->getHighSeqno(),
                                         vbucket_uuid,
                                         snap_start_seqno,
                                         snap_end_seqno,
                                         vb->getPurgeSeqno(),
                                         rollback_seqno);

    if (need_rollback.first) {
        LOG(EXTENSION_LOG_WARNING,
            "%s (vb %d) Stream request requires rollback to seqno:%" PRIu64
            " because %s. Client requested"
            " seqnos:{%" PRIu64 ",%" PRIu64 "}"
            " snapshot:{%" PRIu64 ",%" PRIu64 "}"
            " uuid:%" PRIu64,
            logHeader(),
            vbucket,
            *rollback_seqno,
            need_rollback.second.c_str(),
            start_seqno,
            end_seqno,
            snap_start_seqno,
            snap_end_seqno,
            vbucket_uuid);
        return ENGINE_ROLLBACK;
    }

    ENGINE_ERROR_CODE rv = vb->failovers->addFailoverLog(getCookie(), callback);
    if (rv != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Couldn't add failover log to "
            "stream request due to error %d", logHeader(), vbucket, rv);
        return rv;
    }

    if (flags & DCP_ADD_STREAM_FLAG_LATEST) {
        end_seqno = vb->getHighSeqno();
    }

    if (flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
        end_seqno = engine_.getKVBucket()->getLastPersistedSeqno(vbucket);
    }

    if (!notifyOnly && start_seqno > end_seqno) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "the start seqno (%" PRIu64 ") is larger than the end seqno (%"
            PRIu64 "), stream request flags %d, vb_uuid %" PRIu64
            ", snapStartSeqno %" PRIu64 ", snapEndSeqno %" PRIu64
            "; should have rolled back instead",
            logHeader(), vbucket, start_seqno, end_seqno, flags, vbucket_uuid,
            snap_start_seqno, snap_end_seqno);
        return ENGINE_ERANGE;
    }

    if (!notifyOnly && start_seqno > static_cast<uint64_t>(vb->getHighSeqno()))
    {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "the start seqno (%" PRIu64 ") is larger than the vb highSeqno (%"
            PRId64 "), stream request flags is %d, vb_uuid %" PRIu64
            ", snapStartSeqno %" PRIu64 ", snapEndSeqno %" PRIu64
            "; should have rolled back instead",
            logHeader(), vbucket, start_seqno, vb->getHighSeqno(), flags,
            vbucket_uuid, snap_start_seqno, snap_end_seqno);
        return ENGINE_ERANGE;
    }

    // Create the filter for the stream
    std::unique_ptr<Collections::VB::Filter> vbFilter;
    try {
        vbFilter = std::make_unique<Collections::VB::Filter>(*filter,
                                                             vb->getManifest());
    } catch(std::exception& e) {
        LOG(EXTENSION_LOG_INFO,
            "%s (vb %d) Stream request filter failed construction e.what:%s",
            logHeader(),
            vbucket,
            e.what());
        return ENGINE_UNKNOWN_COLLECTION;
    }

    stream_t s;
    if (notifyOnly) {
        s = new NotifierStream(&engine_,
                               this,
                               getName(),
                               flags,
                               opaque,
                               vbucket,
                               notifySeqno,
                               end_seqno,
                               vbucket_uuid,
                               snap_start_seqno,
                               snap_end_seqno,
                               std::move(vbFilter));
    } else {
        s = new ActiveStream(&engine_,
                             this,
                             getName(),
                             flags,
                             opaque,
                             vbucket,
                             start_seqno,
                             end_seqno,
                             vbucket_uuid,
                             snap_start_seqno,
                             snap_end_seqno,
                             (mutationType == MutationType::KeyOnly),
                             std::move(vbFilter));
    }

    {
        ReaderLockHolder rlh(vb->getStateLock());
        if (vb->getState() == vbucket_state_dead) {
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
                    "this vbucket is in dead state", logHeader(), vbucket);
            return ENGINE_NOT_MY_VBUCKET;
        }

        if (!notifyOnly) {
            // MB-19428: Only activate the stream if we are adding it to the
            // streams map.
            static_cast<ActiveStream*>(s.get())->setActive();
        }
        streams.insert(std::make_pair(vbucket, s));
    }

    notifyStreamReady(vbucket);

    if (add_vb_conn_map) {
        connection_t conn(this);
        engine_.getDcpConnMap().addVBConnByVBId(conn, vbucket);
    }

    return rv;
}

ENGINE_ERROR_CODE DcpProducer::getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                              dcp_add_failover_log callback) {
    (void) opaque;
    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    VBucketPtr vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Get Failover Log failed "
            "because this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    return vb->failovers->addFailoverLog(getCookie(), callback);
}

ENGINE_ERROR_CODE DcpProducer::step(struct dcp_message_producers* producers) {
    setLastWalkTime();

    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE ret;
    if ((ret = maybeDisconnect()) != ENGINE_FAILED) {
          return ret;
    }

    if ((ret = maybeSendNoop(producers)) != ENGINE_FAILED) {
        return ret;
    }

    DcpResponse *resp;
    if (rejectResp) {
        resp = rejectResp;
        rejectResp = NULL;
    } else {
        resp = getNextItem();
        if (!resp) {
            return ENGINE_SUCCESS;
        }
    }

    Item* itmCpy = nullptr;
    auto* mutationResponse = dynamic_cast<MutationProducerResponse*>(resp);
    if (mutationResponse != nullptr) {
        try {
            itmCpy = mutationResponse->getItemCopy();
        } catch (const std::bad_alloc&) {
            rejectResp = resp;
            LOG(EXTENSION_LOG_WARNING,
                "%s (vb %d) ENOMEM while trying to copy "
                "item with seqno %" PRIu64 "before streaming it",
                logHeader(),
                mutationResponse->getVBucket(),
                *mutationResponse->getBySeqno());
            return ENGINE_ENOMEM;
        }

        if (enableValueCompression) {
            /**
             * If value compression is enabled, the producer will need
             * to snappy-compress the document before transmitting.
             * Compression will obviously be done only if the datatype
             * indicates that the value isn't compressed already.
             */
            uint32_t sizeBefore = itmCpy->getNBytes();
            if (!itmCpy->compressValue(
                            engine_.getDcpConnMap().getMinCompressionRatio())) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s Failed to snappy compress an uncompressed value!",
                    logHeader());
            }
            uint32_t sizeAfter = itmCpy->getNBytes();

            if (sizeAfter < sizeBefore) {
                log.acknowledge(sizeBefore - sizeAfter);
            }
        }
    }

    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL,
                                                                     true);
    switch (resp->getEvent()) {
        case DcpResponse::Event::StreamEnd:
        {
            StreamEndResponse *se = static_cast<StreamEndResponse*>(resp);
            ret = producers->stream_end(getCookie(), se->getOpaque(),
                                        se->getVbucket(), se->getFlags());
            break;
        }
        case DcpResponse::Event::Mutation:
        {
            if (itmCpy == nullptr) {
                throw std::logic_error(
                    "DcpProducer::step(Mutation): itmCpy must be != nullptr");
            }
            std::pair<const char*, uint16_t> meta{nullptr, 0};
            if (mutationResponse->getExtMetaData()) {
                meta = mutationResponse->getExtMetaData()->getExtMeta();
            }
            ret = producers->mutation(
                    getCookie(),
                    mutationResponse->getOpaque(),
                    itmCpy,
                    mutationResponse->getVBucket(),
                    *mutationResponse->getBySeqno(),
                    mutationResponse->getRevSeqno(),
                    0 /* lock time */,
                    meta.first,
                    meta.second,
                    mutationResponse->getItem()->getNRUValue(),
                    mutationResponse->getCollectionLen());
            break;
        }
        case DcpResponse::Event::Deletion:
        {
            if (itmCpy == nullptr) {
                throw std::logic_error(
                    "DcpProducer::step(Deletion): itmCpy must be != nullptr");
            }
            std::pair<const char*, uint16_t> meta{nullptr, 0};
            if (mutationResponse->getExtMetaData()) {
                meta = mutationResponse->getExtMetaData()->getExtMeta();
            }
            ret = producers->deletion(getCookie(),
                                      mutationResponse->getOpaque(),
                                      itmCpy,
                                      mutationResponse->getVBucket(),
                                      *mutationResponse->getBySeqno(),
                                      mutationResponse->getRevSeqno(),
                                      meta.first,
                                      meta.second,
                                      mutationResponse->getCollectionLen());
            break;
        }
        case DcpResponse::Event::SnapshotMarker:
        {
            SnapshotMarker *s = static_cast<SnapshotMarker*>(resp);
            ret = producers->marker(getCookie(), s->getOpaque(),
                                    s->getVBucket(),
                                    s->getStartSeqno(),
                                    s->getEndSeqno(),
                                    s->getFlags());
            break;
        }
        case DcpResponse::Event::SetVbucket:
        {
            SetVBucketState *s = static_cast<SetVBucketState*>(resp);
            ret = producers->set_vbucket_state(getCookie(), s->getOpaque(),
                                               s->getVBucket(), s->getState());
            break;
        }
        case DcpResponse::Event::SystemEvent: {
            SystemEventProducerMessage* s =
                    static_cast<SystemEventProducerMessage*>(resp);
            ret = producers->system_event(
                    getCookie(),
                    s->getOpaque(),
                    s->getVBucket(),
                    s->getSystemEvent(),
                    *s->getBySeqno(),
                    {reinterpret_cast<const uint8_t*>(s->getKey().data()),
                     s->getKey().size()},
                    s->getEventData());
            break;
        }
        default:
        {
            LOG(EXTENSION_LOG_WARNING, "%s Unexpected dcp event (%s), "
                "disconnecting", logHeader(),
                resp->to_string());
            ret = ENGINE_DISCONNECT;
            break;
        }
    }

    ObjectRegistry::onSwitchThread(epe);

    if (ret == ENGINE_E2BIG) {
        rejectResp = resp;
    } else {
        delete resp;
    }

    lastSendTime = ep_current_time();
    return (ret == ENGINE_SUCCESS) ? ENGINE_WANT_MORE : ret;
}

ENGINE_ERROR_CODE DcpProducer::bufferAcknowledgement(uint32_t opaque,
                                                     uint16_t vbucket,
                                                     uint32_t buffer_bytes) {
    lastReceiveTime = ep_current_time();
    log.acknowledge(buffer_bytes);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpProducer::control(uint32_t opaque, const void* key,
                                       uint16_t nkey, const void* value,
                                       uint32_t nvalue) {
    lastReceiveTime = ep_current_time();
    const char* param = static_cast<const char*>(key);
    std::string keyStr(static_cast<const char*>(key), nkey);
    std::string valueStr(static_cast<const char*>(value), nvalue);

    if (strncmp(param, "connection_buffer_size", nkey) == 0) {
        uint32_t size;
        if (parseUint32(valueStr.c_str(), &size)) {
            /* Size 0 implies the client (DCP consumer) does not support
               flow control */
            log.setBufferSize(size);
            return ENGINE_SUCCESS;
        }
    } else if (strncmp(param, "stream_buffer_size", nkey) == 0) {
        LOG(EXTENSION_LOG_WARNING, "%s The ctrl parameter stream_buffer_size is"
            "not supported by this engine", logHeader());
        return ENGINE_ENOTSUP;
    } else if (strncmp(param, "enable_noop", nkey) == 0) {
        if (valueStr == "true") {
            noopCtx.enabled = true;
        } else {
            noopCtx.enabled = false;
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "enable_ext_metadata", nkey) == 0) {
        if (valueStr == "true") {
            enableExtMetaData = true;
        } else {
            enableExtMetaData = false;
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "enable_value_compression", nkey) == 0) {
        if (valueStr == "true") {
            enableValueCompression = true;
        } else {
            enableValueCompression = false;
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "supports_cursor_dropping", nkey) == 0) {
        if (valueStr == "true") {
            supportsCursorDropping = true;
        } else {
            supportsCursorDropping = false;
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "set_noop_interval", nkey) == 0) {
        uint32_t noopInterval;
        if (parseUint32(valueStr.c_str(), &noopInterval)) {
            /*
             * We need to ensure that we only set the noop interval to a value
             * that is a multiple of the connection manager interval. The reason
             * is that if there is no DCP traffic we snooze for the connection
             * manager interval before sending the noop.
             */
            if (noopInterval % engine_.getConfiguration().
                    getConnectionManagerInterval() == 0) {
                noopCtx.dcpNoopTxInterval = std::chrono::seconds(noopInterval);
                return ENGINE_SUCCESS;
            } else {
                LOG(EXTENSION_LOG_WARNING, "%s The ctrl parameter "
                    "set_noop_interval is being set to %" PRIu32 " seconds."
                    "This is not a multiple of the connectionManagerInterval "
                    "of %" PRIu64 " seconds, and so is not supported.",
                    logHeader(), noopInterval,
                    uint64_t(engine_.getConfiguration().
                             getConnectionManagerInterval()));
                return ENGINE_EINVAL;
            }
        }
    } else if(strncmp(param, "set_priority", nkey) == 0) {
        if (valueStr == "high") {
            engine_.setDCPPriority(getCookie(), CONN_PRIORITY_HIGH);
            priority.assign("high");
            return ENGINE_SUCCESS;
        } else if (valueStr == "medium") {
            engine_.setDCPPriority(getCookie(), CONN_PRIORITY_MED);
            priority.assign("medium");
            return ENGINE_SUCCESS;
        } else if (valueStr == "low") {
            engine_.setDCPPriority(getCookie(), CONN_PRIORITY_LOW);
            priority.assign("low");
            return ENGINE_SUCCESS;
        }
    }

    LOG(EXTENSION_LOG_WARNING, "%s Invalid ctrl parameter '%s' for %s",
        logHeader(), valueStr.c_str(), keyStr.c_str());

    return ENGINE_EINVAL;
}

bool DcpProducer::handleResponse(protocol_binary_response_header* resp) {
    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return false;
    }

    uint8_t opcode = resp->response.opcode;
    if (opcode == PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE ||
        opcode == PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER) {
        protocol_binary_response_dcp_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_dcp_stream_req*>(resp);
        uint32_t opaque = pkt->message.header.response.opaque;


        // Search for an active stream with the same opaque as the response.
        auto itr = streams.find_if(
            [opaque](const StreamsMap::value_type& s) {
                const auto& stream = s.second;
                if (stream && stream->isTypeActive()) {
                    ActiveStream* as = static_cast<ActiveStream*>(stream.get());
                    return (as && opaque == stream->getOpaque());
                } else {
                    return false;
                }
            }
        );

        if (itr.second) {
            ActiveStream *as = static_cast<ActiveStream*>(itr.first.get());
            if (opcode == PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE) {
                as->setVBucketStateAckRecieved();
            } else if (opcode == PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER) {
                as->snapshotMarkerAckReceived();
            }
        }

        return true;
    } else if (opcode == PROTOCOL_BINARY_CMD_DCP_MUTATION ||
        opcode == PROTOCOL_BINARY_CMD_DCP_DELETION ||
        opcode == PROTOCOL_BINARY_CMD_DCP_EXPIRATION ||
        opcode == PROTOCOL_BINARY_CMD_DCP_STREAM_END) {
        // TODO: When nacking is implemented we need to handle these responses
        return true;
    } else if (opcode == PROTOCOL_BINARY_CMD_DCP_NOOP) {
        if (noopCtx.opaque == resp->response.opaque) {
            noopCtx.pendingRecv = false;
            return true;
        }
    }

    LOG(EXTENSION_LOG_WARNING, "%s Trying to handle an unknown response %d, "
        "disconnecting", logHeader(), opcode);

    return false;
}

ENGINE_ERROR_CODE DcpProducer::closeStream(uint32_t opaque, uint16_t vbucket) {
    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    auto it = streams.erase(vbucket);

    ENGINE_ERROR_CODE ret;
    if (!it.second) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Cannot close stream because no "
            "stream exists for this vbucket", logHeader(), vbucket);
        return ENGINE_KEY_ENOENT;
    } else {
        auto& stream = it.first;
        if (!stream->isActive()) {
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Cannot close stream because "
                "stream is already marked as dead", logHeader(), vbucket);
            connection_t conn(this);
            engine_.getDcpConnMap().removeVBConnByVBId(conn, vbucket);
            ret = ENGINE_KEY_ENOENT;
        } else {
            stream->setDead(END_STREAM_CLOSED);
            connection_t conn(this);
            engine_.getDcpConnMap().removeVBConnByVBId(conn, vbucket);
            ret = ENGINE_SUCCESS;
        }
    }

    return ret;
}

void DcpProducer::notifyBackfillManager() {
    backfillMgr->wakeUpTask();
}

bool DcpProducer::recordBackfillManagerBytesRead(size_t bytes, bool force) {
    if (force) {
        backfillMgr->bytesForceRead(bytes);
        return true;
    }
    return backfillMgr->bytesCheckAndRead(bytes);
}

void DcpProducer::recordBackfillManagerBytesSent(size_t bytes) {
    backfillMgr->bytesSent(bytes);
}

void DcpProducer::scheduleBackfillManager(VBucket& vb,
                                          const active_stream_t& s,
                                          uint64_t start,
                                          uint64_t end) {
    backfillMgr->schedule(vb, s, start, end);
}

void DcpProducer::addStats(ADD_STAT add_stat, const void *c) {
    Producer::addStats(add_stat, c);

    addStat("items_sent", getItemsSent(), add_stat, c);
    addStat("items_remaining", getItemsRemaining(), add_stat, c);
    addStat("total_bytes_sent", getTotalBytes(), add_stat, c);
    addStat("last_sent_time", lastSendTime, add_stat, c);
    addStat("last_receive_time", lastReceiveTime, add_stat, c);
    addStat("noop_enabled", noopCtx.enabled, add_stat, c);
    addStat("noop_wait", noopCtx.pendingRecv, add_stat, c);
    addStat("priority", priority.c_str(), add_stat, c);
    addStat("enable_ext_metadata", enableExtMetaData ? "enabled" : "disabled",
            add_stat, c);
    addStat("enable_value_compression",
            enableValueCompression ? "enabled" : "disabled",
            add_stat, c);
    addStat("cursor_dropping",
            supportsCursorDropping ? "ELIGIBLE" : "NOT_ELIGIBLE",
            add_stat, c);

    // Possible that the producer has had its streams closed and hence doesn't
    // have a backfill manager anymore.
    if (backfillMgr) {
        backfillMgr->addStats(this, add_stat, c);
    }

    log.addStats(add_stat, c);

    addStat("num_streams", streams.size(), add_stat, c);

    // Make a copy of all valid streams (under lock), and then call addStats
    // for each one. (Done in two stages to minmise how long we have the
    // streams map locked for).
    std::vector<StreamsMap::mapped_type> valid_streams;

    streams.for_each(
        [&valid_streams](const StreamsMap::value_type& element) {
            valid_streams.push_back(element.second);
        }
    );
    for (const auto& stream : valid_streams) {
        stream->addStats(add_stat, c);
    }
}

void DcpProducer::addTakeoverStats(ADD_STAT add_stat, const void* c,
                                   const VBucket& vb) {

    auto stream = findStream(vb.getId());
    if (stream) {
        if (stream->isTypeActive()) {
            ActiveStream* as = static_cast<ActiveStream*>(stream.get());
            as->addTakeoverStats(add_stat, c, vb);
        } else {
            LOG(EXTENSION_LOG_WARNING, "%s (vb:%" PRIu16 ") "
                "DcpProducer::addTakeoverStats Stream type is %s and not the "
                "expected Active",
                logHeader(), vb.getId(), to_string(stream->getType()).c_str());
        }
    } else {
        LOG(EXTENSION_LOG_NOTICE, "%s (vb:%" PRIu16 ") "
            "DcpProducer::addTakeoverStats Unable to find stream",
            logHeader(), vb.getId());
    }
}

void DcpProducer::aggregateQueueStats(ConnCounter& aggregator) {
    aggregator.conn_queueDrain += itemsSent;
    aggregator.conn_totalBytes += totalBytesSent;
    aggregator.conn_queueRemaining += getItemsRemaining();
    aggregator.conn_queueBackfillRemaining += totalBackfillBacklogs;
}

void DcpProducer::notifySeqnoAvailable(uint16_t vbucket, uint64_t seqno) {
    auto stream = findStream(vbucket);
    if (stream && stream->isActive()) {
        stream->notifySeqnoAvailable(seqno);
    }
}

void DcpProducer::vbucketStateChanged(uint16_t vbucket, vbucket_state_t state) {
    auto stream = findStream(vbucket);
    if (stream) {
        LOG(EXTENSION_LOG_INFO, "%s (vb %" PRIu16 ") State changed to "
            "%s, closing active stream!",
            logHeader(), vbucket, VBucket::toString(state));
        stream->setDead(END_STREAM_STATE);
    }
}

bool DcpProducer::handleSlowStream(uint16_t vbid,
                                   const std::string &name) {
    if (supportsCursorDropping) {
        auto stream = findStream(vbid);
        if (stream) {
            if (stream->getName().compare(name) == 0) {
                ActiveStream* as = static_cast<ActiveStream*>(stream.get());
                as->handleSlowStream();
                return true;
            }
        }
    }
    return false;
}

void DcpProducer::closeAllStreams() {
    lastReceiveTime = ep_current_time();
    std::vector<uint16_t> vbvector;
    {
        // Need to synchronise the disconnect and clear, therefore use
        // external locking here.
        std::lock_guard<StreamsMap> guard(streams);

        streams.for_each(
            [&vbvector](StreamsMap::value_type& iter) {
                vbvector.push_back(iter.first);
                iter.second->setDead(END_STREAM_DISCONNECTED);
            },
            guard);

        streams.clear(guard);
    }
    connection_t conn(this);
    for (const auto vbid: vbvector) {
         engine_.getDcpConnMap().removeVBConnByVBId(conn, vbid);
    }

    // Destroy the backfillManager. (BackfillManager task also
    // may hold a weak reference to it while running, but that is
    // guaranteed to decay and free the BackfillManager once it
    // completes run().
    // This will terminate any tasks and delete any backfills
    // associated with this Producer.  This is necessary as if we
    // don't, then the RCPtr references which exist between
    // DcpProducer and ActiveStream result in us leaking DcpProducer
    // objects (and Couchstore vBucket files, via DCPBackfill task).
    backfillMgr.reset();
}

const char* DcpProducer::getType() const {
    if (notifyOnly) {
        return "notifier";
    } else {
        return "producer";
    }
}

DcpResponse* DcpProducer::getNextItem() {
    do {
        setPaused(false);

        uint16_t vbucket = 0;
        while (ready.popFront(vbucket)) {
            if (log.pauseIfFull()) {
                ready.pushUnique(vbucket);
                return NULL;
            }

            DcpResponse* op = NULL;
            stream_t stream;
            {
                stream = findStream(vbucket);
                if (!stream) {
                    continue;
                }
            }

            op = stream->next();

            if (!op) {
                // stream is empty, try another vbucket.
                continue;
            }

            switch (op->getEvent()) {
                case DcpResponse::Event::SnapshotMarker:
                case DcpResponse::Event::Mutation:
                case DcpResponse::Event::Deletion:
                case DcpResponse::Event::Expiration:
                case DcpResponse::Event::StreamEnd:
                case DcpResponse::Event::SetVbucket:
                case DcpResponse::Event::SystemEvent:
                    break;
                default:
                    throw std::logic_error(
                            std::string("DcpProducer::getNextItem: "
                            "Producer (") + logHeader() + ") is attempting to "
                            "write an unexpected event:" +
                            op->to_string());
            }

            ready.pushUnique(vbucket);

            if (op->getEvent() == DcpResponse::Event::Mutation ||
                op->getEvent() == DcpResponse::Event::Deletion ||
                op->getEvent() == DcpResponse::Event::Expiration ||
                op->getEvent() == DcpResponse::Event::SystemEvent) {
                itemsSent++;
            }

            totalBytesSent.fetch_add(op->getMessageSize());

            return op;
        }

        // flag we are paused
        setPaused(true);

        // re-check the ready queue.
        // A new vbucket could of became ready and the notifier could of seen
        // paused = false, so reloop so we don't miss an operation.
    } while(!ready.empty());

    return NULL;
}

void DcpProducer::setDisconnect(bool disconnect) {
    ConnHandler::setDisconnect(disconnect);

    if (disconnect) {
        streams.for_each(
            [](StreamsMap::value_type& iter){
                iter.second->setDead(END_STREAM_DISCONNECTED);
            }
        );
    }
}

void DcpProducer::notifyStreamReady(uint16_t vbucket) {
    if (ready.pushUnique(vbucket)) {
        log.unpauseIfSpaceAvailable();
    }
}

void DcpProducer::notifyPaused(bool schedule) {
    engine_.getDcpConnMap().notifyPausedConnection(this, schedule);
}

ENGINE_ERROR_CODE DcpProducer::maybeDisconnect() {
    const auto now = ep_current_time();
    std::chrono::seconds elapsedTime(now - lastReceiveTime);
    if (noopCtx.enabled && elapsedTime > noopCtx.dcpIdleTimeout) {
        LOG(EXTENSION_LOG_NOTICE,
            "%s Disconnecting because a message has not been received for "
            "%" PRIu64 "s. lastSendTime:%" PRIu32 ", lastReceiveTime:%" PRIu64
            ", noopCtx {sendTime:%" PRIu32 ", opaque: %" PRIu32 ", pendingRecv:%s}",
            logHeader(),
            uint64_t(noopCtx.dcpIdleTimeout.count()),
            (now - lastSendTime),
            uint64_t(elapsedTime.count()),
            (now - noopCtx.sendTime),
            noopCtx.opaque,
            noopCtx.pendingRecv ? "true" : "false");
        return ENGINE_DISCONNECT;
    }
    // Returning ENGINE_FAILED means ignore and continue
    // without disconnecting
    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE DcpProducer::maybeSendNoop(
        struct dcp_message_producers* producers) {
    if (!noopCtx.enabled) {
        // Returning ENGINE_FAILED means ignore and continue
        // without sending a noop
        return ENGINE_FAILED;
    }
    std::chrono::seconds elapsedTime(ep_current_time() - noopCtx.sendTime);

    // Check to see if waiting for a noop reply.
    // If not try to send a noop to the consumer if the interval has passed
    if (!noopCtx.pendingRecv && elapsedTime >= noopCtx.dcpNoopTxInterval) {
        EventuallyPersistentEngine *epe = ObjectRegistry::
                onSwitchThread(NULL, true);
        ENGINE_ERROR_CODE ret = producers->noop(getCookie(), ++noopCtx.opaque);
        ObjectRegistry::onSwitchThread(epe);

        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
            noopCtx.pendingRecv = true;
            noopCtx.sendTime = ep_current_time();
            lastSendTime = noopCtx.sendTime;
        }
      return ret;
    }
    // We have already sent a noop and are awaiting a receive or
    // the time interval has not passed.  In either case continue
    // without sending a noop.
    return ENGINE_FAILED;
}

bool DcpProducer::isTimeForNoop() {
    // Not Implemented
    return false;
}

void DcpProducer::setTimeForNoop() {
    // Not Implemented
}

void DcpProducer::clearQueues() {
    streams.for_each(
        [](StreamsMap::value_type& iter) {
            iter.second->clear();
        }
    );
}

size_t DcpProducer::getBackfillQueueSize() {
    return totalBackfillBacklogs;
}

size_t DcpProducer::getItemsSent() {
    return itemsSent;
}

size_t DcpProducer::getItemsRemaining() {
    size_t remainingSize = 0;
    streams.for_each(
        [&remainingSize](const StreamsMap::value_type& iter) {
            if (iter.second->isTypeActive()) {
                ActiveStream *as = static_cast<ActiveStream *>(iter.second.get());
                remainingSize += as->getItemsRemaining();
            }
        }
    );

    return remainingSize;
}

size_t DcpProducer::getTotalBytes() {
    return totalBytesSent;
}

std::vector<uint16_t> DcpProducer::getVBVector() {
    std::vector<uint16_t> vbvector;
    streams.for_each(
        [&vbvector](StreamsMap::value_type& iter) {
        vbvector.push_back(iter.first);
    });
    return vbvector;
}

bool DcpProducer::bufferLogInsert(size_t bytes) {
    return log.insert(bytes);
}

void DcpProducer::createCheckpointProcessorTask() {
    checkpointCreatorTask =
            std::make_shared<ActiveStreamCheckpointProcessorTask>(engine_);
}

void DcpProducer::scheduleCheckpointProcessorTask() {
    ExecutorPool::get()->schedule(checkpointCreatorTask);
}

void DcpProducer::scheduleCheckpointProcessorTask(const stream_t& s) {
    if (!checkpointCreatorTask) {
        throw std::logic_error(
                "DcpProducer::scheduleCheckpointProcessorTask task is null");
    }
    static_cast<ActiveStreamCheckpointProcessorTask*>(checkpointCreatorTask.get())
        ->schedule(s);
}

void DcpProducer::clearCheckpointProcessorTaskQueues() {
    if (!checkpointCreatorTask) {
        throw std::logic_error(
                "DcpProducer::clearCheckpointProcessorTaskQueues task is null");
    }
    static_cast<ActiveStreamCheckpointProcessorTask*>(checkpointCreatorTask.get())
        ->clearQueues();
}

SingleThreadedRCPtr<Stream> DcpProducer::findStream(uint16_t vbid) {
    auto it = streams.find(vbid);
    if (it.second) {
        return it.first;
    } else {
        return SingleThreadedRCPtr<Stream>();
    }
}
