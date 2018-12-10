/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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
#include "nobucket.h"

#include <memcached/dcp.h>
#include <memcached/server_log_iface.h>

#include <cstdlib>
#include <stdexcept>
#include <string>

#include <spdlog/logger.h>

/**
 * The NoBucket is a bucket that just returns "ENGINE_NO_BUCKET" for "all"
 * operations. The main purpose for having this bucket is to reduce the
 * code complexity so we don't have to had checks all over the place to
 * see if a client is connected to a bucket or not (we may just associate
 * the connection to this bucket and it'll handle the appropriate
 * command.
 */
class NoBucket : public EngineIface, public DcpIface {
public:
    NoBucket() {
        EngineIface::collections.set_manifest = collections_set_manifest;
        EngineIface::collections.get_manifest = collections_get_manifest;
        EngineIface::collections.get_collection_id =
                collections_get_collection_id;
    };

    ENGINE_ERROR_CODE initialize(const char* config_str) override {
        return ENGINE_SUCCESS;
    }

    void destroy(bool) override {
        delete this;
    }

    cb::EngineErrorItemPair allocate(gsl::not_null<const void*>,
                                     const DocKey&,
                                     const size_t,
                                     const int,
                                     const rel_time_t,
                                     uint8_t,
                                     Vbid) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    std::pair<cb::unique_item_ptr, item_info> allocate_ex(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            Vbid vbucket) override {
        throw cb::engine_error(cb::engine_errc::no_bucket, "no bucket");
    }

    ENGINE_ERROR_CODE remove(
            gsl::not_null<const void*>,
            const DocKey&,
            uint64_t&,
            Vbid,
            boost::optional<cb::durability::Requirements> durability,
            mutation_descr_t&) override {
        return ENGINE_NO_BUCKET;
    }

    void release(gsl::not_null<item*>) override {
        throw std::logic_error(
                "NoBucket::item_release: no items should have"
                " been allocated from this engine");
    }

    cb::EngineErrorItemPair get(gsl::not_null<const void*>,
                                const DocKey&,
                                Vbid,
                                DocStateFilter) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    cb::EngineErrorItemPair get_if(
            gsl::not_null<const void*>,
            const DocKey&,
            Vbid,
            std::function<bool(const item_info&)>) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    cb::EngineErrorMetadataPair get_meta(gsl::not_null<const void*> cookie,
                                         const DocKey& key,
                                         Vbid vbucket) override {
        return cb::EngineErrorMetadataPair(cb::engine_errc::no_bucket, {});
    }

    cb::EngineErrorItemPair get_locked(gsl::not_null<const void*>,
                                       const DocKey&,
                                       Vbid,
                                       uint32_t) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    ENGINE_ERROR_CODE unlock(gsl::not_null<const void*>,
                             const DocKey&,
                             Vbid,
                             uint64_t) override {
        return ENGINE_NO_BUCKET;
    }

    cb::EngineErrorItemPair get_and_touch(
            gsl::not_null<const void*> cookie,
            const DocKey&,
            Vbid,
            uint32_t,
            boost::optional<cb::durability::Requirements>) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    ENGINE_ERROR_CODE store(gsl::not_null<const void*>,
                            gsl::not_null<item*>,
                            uint64_t&,
                            ENGINE_STORE_OPERATION,
                            boost::optional<cb::durability::Requirements>,
                            DocumentState) override {
        return ENGINE_NO_BUCKET;
    }

    cb::EngineErrorCasPair store_if(
            gsl::not_null<const void*>,
            gsl::not_null<item*>,
            uint64_t,
            ENGINE_STORE_OPERATION,
            cb::StoreIfPredicate,
            boost::optional<cb::durability::Requirements>,
            DocumentState) override {
        return {cb::engine_errc::no_bucket, 0};
    }

    ENGINE_ERROR_CODE flush(gsl::not_null<const void*>) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE get_stats(gsl::not_null<const void*>,
                                cb::const_char_buffer key,
                                ADD_STAT) override {
        return ENGINE_NO_BUCKET;
    }

    void reset_stats(gsl::not_null<const void*> cookie) override {
    }

    ENGINE_ERROR_CODE unknown_command(const void*,
                                      const cb::mcbp::Request& request,
                                      ADD_RESPONSE) override {
        return ENGINE_NO_BUCKET;
    }

    void item_set_cas(gsl::not_null<item*>, uint64_t) override {
        throw std::logic_error(
                "NoBucket::item_set_cas: no items should have"
                " been allocated from this engine");
    }

    void item_set_datatype(gsl::not_null<item*>,
                           protocol_binary_datatype_t) override {
        throw std::logic_error(
                "NoBucket::item_set_datatype: no items should have"
                " been allocated from this engine");
    }

    bool get_item_info(gsl::not_null<const item*>,
                       gsl::not_null<item_info*>) override {
        throw std::logic_error(
                "NoBucket::get_item_info: no items should have"
                " been allocated from this engine");
    }

    // DcpIface implementation ////////////////////////////////////////////////

    ENGINE_ERROR_CODE step(
            gsl::not_null<const void*>,
            gsl::not_null<struct dcp_message_producers*>) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE open(gsl::not_null<const void*>,
                           uint32_t,
                           uint32_t,
                           uint32_t,
                           cb::const_char_buffer) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE add_stream(gsl::not_null<const void*>,
                                 uint32_t,
                                 Vbid,
                                 uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE close_stream(gsl::not_null<const void*>,
                                   uint32_t,
                                   Vbid,
                                   DcpStreamId) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE stream_req(
            gsl::not_null<const void*>,
            uint32_t,
            uint32_t,
            Vbid,
            uint64_t,
            uint64_t,
            uint64_t,
            uint64_t,
            uint64_t,
            uint64_t*,
            dcp_add_failover_log,
            boost::optional<cb::const_char_buffer>) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE get_failover_log(
            gsl::not_null<const void*>,
            uint32_t,
            Vbid,
            ENGINE_ERROR_CODE (*)(vbucket_failover_t*,
                                  size_t,
                                  gsl::not_null<const void*>)) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE stream_end(gsl::not_null<const void*>,
                                 uint32_t,
                                 Vbid,
                                 uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE snapshot_marker(gsl::not_null<const void*>,
                                      uint32_t,
                                      Vbid,
                                      uint64_t,
                                      uint64_t,
                                      uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE mutation(gsl::not_null<const void*>,
                               uint32_t,
                               const DocKey&,
                               cb::const_byte_buffer,
                               size_t,
                               uint8_t,
                               uint64_t,
                               Vbid,
                               uint32_t,
                               uint64_t,
                               uint64_t,
                               uint32_t,
                               uint32_t,
                               cb::const_byte_buffer,
                               uint8_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE deletion(gsl::not_null<const void*>,
                               uint32_t,
                               const DocKey&,
                               cb::const_byte_buffer,
                               size_t,
                               uint8_t,
                               uint64_t,
                               Vbid,
                               uint64_t,
                               uint64_t,
                               cb::const_byte_buffer) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE deletion_v2(gsl::not_null<const void*>,
                                  uint32_t,
                                  const DocKey&,
                                  cb::const_byte_buffer,
                                  size_t,
                                  uint8_t,
                                  uint64_t,
                                  Vbid,
                                  uint64_t,
                                  uint64_t,
                                  uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE expiration(gsl::not_null<const void*>,
                                 uint32_t,
                                 const DocKey&,
                                 cb::const_byte_buffer,
                                 size_t,
                                 uint8_t,
                                 uint64_t,
                                 Vbid,
                                 uint64_t,
                                 uint64_t,
                                 uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE set_vbucket_state(gsl::not_null<const void*>,
                                        uint32_t,
                                        Vbid,
                                        vbucket_state_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE noop(gsl::not_null<const void*> cookie,
                           uint32_t opaque) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE buffer_acknowledgement(gsl::not_null<const void*>,
                                             uint32_t,
                                             Vbid,
                                             uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE control(gsl::not_null<const void*>,
                              uint32_t,
                              cb::const_char_buffer,
                              cb::const_char_buffer) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE response_handler(
            gsl::not_null<const void*>,
            const protocol_binary_response_header*) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE system_event(gsl::not_null<const void*> cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   mcbp::systemevent::version version,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE prepare(
            gsl::not_null<const void*> cookie,
            uint32_t opaque,
            const DocKey& key,
            cb::const_byte_buffer value,
            size_t priv_bytes,
            uint8_t datatype,
            uint64_t cas,
            Vbid vbucket,
            uint32_t flags,
            uint64_t by_seqno,
            uint64_t rev_seqno,
            uint32_t expiration,
            uint32_t lock_time,
            uint8_t nru,
            DocumentState document_state,
            cb::durability::Requirements durability) override {
        return ENGINE_NO_BUCKET;
    }
    ENGINE_ERROR_CODE seqno_acknowledged(gsl::not_null<const void*> cookie,
                                         uint32_t opaque,
                                         uint64_t in_memory_seqno,
                                         uint64_t on_disk_seqno) override {
        return ENGINE_NO_BUCKET;
    }
    ENGINE_ERROR_CODE commit(gsl::not_null<const void*> cookie,
                             uint32_t opaque,
                             uint64_t prepared_seqno,
                             uint64_t commit_seqno) override {
        return ENGINE_NO_BUCKET;
    }

private:
    static cb::engine_error collections_set_manifest(
            gsl::not_null<EngineIface*> handle, cb::const_char_buffer json) {
        return {cb::engine_errc::no_bucket,
                "nobucket::collections_set_manifest"};
    }

    static cb::EngineErrorStringPair collections_get_manifest(
            gsl::not_null<EngineIface*> handle) {
        return {cb::engine_errc::no_bucket,
                "nobucket::collections_get_manifest"};
    }

    static cb::EngineErrorGetCollectionIDResult collections_get_collection_id(
            gsl::not_null<EngineIface*> handle, cb::const_char_buffer path) {
        return {cb::engine_errc::no_bucket, 0, 0};
    }
};

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_no_bucket_instance(GET_SERVER_API get_server_api,
                                            EngineIface** handle) {
    try {
        NoBucket* engine = new NoBucket();
        *handle = reinterpret_cast<EngineIface*>(engine);
    } catch (std::bad_alloc& e) {
        auto logger = get_server_api()->log->get_spdlogger();
        logger->warn("NoBucket: failed to create engine: {}", e.what());
        return ENGINE_FAILED;
    }

    return ENGINE_SUCCESS;
}

MEMCACHED_PUBLIC_API
void destroy_engine(void) {
    // Empty
}
