/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include <dcp/dcp-types.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>

#include <relaxed_atomic.h>

extern std::vector<std::pair<uint64_t, uint64_t> > dcp_failover_log;

ENGINE_ERROR_CODE mock_dcp_add_failover_log(vbucket_failover_t* entry,
                                            size_t nentries,
                                            gsl::not_null<const void*> cookie);

class MockDcpMessageProducers : public dcp_message_producers {
public:
    MockDcpMessageProducers(EngineIface* engine = nullptr);

    ENGINE_ERROR_CODE get_failover_log(uint32_t opaque, Vbid vbucket) override;

    ENGINE_ERROR_CODE stream_req(uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags,
                                 uint64_t start_seqno,
                                 uint64_t end_seqno,
                                 uint64_t vbucket_uuid,
                                 uint64_t snap_start_seqno,
                                 uint64_t snap_end_seqno,
                                 const std::string& request_value) override;

    ENGINE_ERROR_CODE add_stream_rsp(uint32_t opaque,
                                     uint32_t stream_opaque,
                                     cb::mcbp::Status status) override;

    ENGINE_ERROR_CODE marker_rsp(uint32_t opaque,
                                 cb::mcbp::Status status) override;

    ENGINE_ERROR_CODE set_vbucket_state_rsp(uint32_t opaque,
                                            cb::mcbp::Status status) override;

    ENGINE_ERROR_CODE stream_end(uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags,
                                 cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE marker(uint32_t opaque,
                             Vbid vbucket,
                             uint64_t start_seqno,
                             uint64_t end_seqno,
                             uint32_t flags,
                             boost::optional<uint64_t> high_completed_seqno,
                             boost::optional<uint64_t> maxVisibleSeqno,
                             cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE mutation(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t lock_time,
                               uint8_t nru,
                               cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE deletion(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE deletion_v2(uint32_t opaque,
                                  cb::unique_item_ptr itm,
                                  Vbid vbucket,
                                  uint64_t by_seqno,
                                  uint64_t rev_seqno,
                                  uint32_t delete_time,
                                  cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE expiration(uint32_t opaque,
                                 cb::unique_item_ptr itm,
                                 Vbid vbucket,
                                 uint64_t by_seqno,
                                 uint64_t rev_seqno,
                                 uint32_t delete_time,
                                 cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE set_vbucket_state(uint32_t opaque,
                                        Vbid vbucket,
                                        vbucket_state_t state) override;
    ENGINE_ERROR_CODE noop(uint32_t opaque) override;
    ENGINE_ERROR_CODE buffer_acknowledgement(uint32_t opaque,
                                             Vbid vbucket,
                                             uint32_t buffer_bytes) override;
    ENGINE_ERROR_CODE control(uint32_t opaque,
                              cb::const_char_buffer key,
                              cb::const_char_buffer value) override;
    ENGINE_ERROR_CODE get_error_map(uint32_t opaque, uint16_t version) override;
    // Change the status code returned from mutation() to the specified value.
    void setMutationStatus(ENGINE_ERROR_CODE code);

    ENGINE_ERROR_CODE system_event(uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   mcbp::systemevent::version version,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData,
                                   cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE prepare(uint32_t opaque,
                              cb::unique_item_ptr itm,
                              Vbid vbucket,
                              uint64_t by_seqno,
                              uint64_t rev_seqno,
                              uint32_t lock_time,
                              uint8_t nru,
                              DocumentState document_state,
                              cb::durability::Level level) override;

    ENGINE_ERROR_CODE seqno_acknowledged(uint32_t opaque,
                                         Vbid vbucket,
                                         uint64_t prepared_seqno) override;

    ENGINE_ERROR_CODE commit(uint32_t opaque,
                             Vbid vbucket,
                             const DocKey& key,
                             uint64_t prepare_seqno,
                             uint64_t commit_seqno) override;

    ENGINE_ERROR_CODE abort(uint32_t opaque,
                            Vbid vbucket,
                            const DocKey& key,
                            uint64_t prepared_seqno,
                            uint64_t abort_seqno) override;

    ENGINE_ERROR_CODE oso_snapshot(uint32_t opaque,
                                   Vbid vbucket,
                                   uint32_t flags,
                                   cb::mcbp::DcpStreamId sid) override;

    void clear_dcp_data();

    cb::mcbp::ClientOpcode last_op;
    cb::mcbp::Status last_status;
    uint8_t last_nru;
    Vbid last_vbucket;
    uint32_t last_opaque;
    uint32_t last_flags;
    uint32_t last_stream_opaque;
    uint32_t last_locktime;
    uint32_t last_packet_size;
    uint64_t last_cas;
    uint64_t last_start_seqno;
    uint64_t last_end_seqno;
    uint64_t last_vbucket_uuid;
    uint64_t last_snap_start_seqno;
    uint64_t last_snap_end_seqno;
    cb::RelaxedAtomic<uint64_t> last_byseqno;
    uint64_t last_revseqno;
    CollectionID last_collection_id;
    ScopeID last_scope_id;
    uint32_t last_delete_time;
    std::string last_meta;
    std::string last_value;
    std::string last_key;
    vbucket_state_t last_vbucket_state;
    protocol_binary_datatype_t last_datatype;
    mcbp::systemevent::id last_system_event;
    std::vector<uint8_t> last_system_event_data;
    mcbp::systemevent::version last_system_event_version;
    uint64_t last_collection_manifest_uid;
    cb::mcbp::DcpStreamId last_stream_id;
    std::string last_collection_filter;
    uint64_t last_prepared_seqno;
    uint64_t last_high_completed_seqno;
    uint64_t last_commit_seqno;
    uint64_t last_abort_seqno;
    uint32_t last_oso_snapshot_flags;

protected:
    /// Helper method for deletion / deletion_v2 / expiration
    ENGINE_ERROR_CODE deletionInner(uint32_t opaque,
                                    cb::unique_item_ptr itm,
                                    Vbid vbucket,
                                    uint64_t by_seqno,
                                    uint64_t rev_seqno,
                                    uint32_t deleteTime,
                                    uint32_t extlen,
                                    DeleteSource deleteSource,
                                    cb::mcbp::DcpStreamId sid);

    ENGINE_ERROR_CODE mutationStatus = ENGINE_SUCCESS;

    ENGINE_ERROR_CODE handleMutationOrPrepare(cb::mcbp::ClientOpcode opcode,
                                              uint32_t opaque,
                                              cb::unique_item_ptr itm,
                                              Vbid vbucket,
                                              uint64_t by_seqno,
                                              uint64_t rev_seqno,
                                              uint32_t lock_time,
                                              cb::const_char_buffer meta,
                                              uint8_t nru);
};
