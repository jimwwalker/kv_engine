/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <daemon/mcaudit.h>
#include <daemon/mcbp.h>
#include "executors.h"
#include "utilities.h"

void dcp_open_executor(Cookie& cookie) {
    auto packet = cookie.getPacket(Cookie::PacketContent::Full);
    const auto* req = reinterpret_cast<const protocol_binary_request_dcp_open*>(
            packet.data());

    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    connection.enableDatatype(cb::mcbp::Feature::SNAPPY);
    connection.enableDatatype(cb::mcbp::Feature::JSON);

    uint32_t flags = ntohl(req->message.body.flags);
    const bool dcpNotifier = (flags & DCP_OPEN_NOTIFIER) == DCP_OPEN_NOTIFIER;

    if (ret == ENGINE_SUCCESS) {
        cb::rbac::Privilege privilege = cb::rbac::Privilege::DcpProducer;
        if (dcpNotifier) {
            privilege = cb::rbac::Privilege::DcpConsumer;
        }

        ret = mcbp::checkPrivilege(cookie, privilege);

        const uint16_t nkey = ntohs(req->message.header.request.keylen);
        const uint32_t valuelen = ntohl(req->message.header.request.bodylen) -
                                  nkey - req->message.header.request.extlen;

        const auto* name =
                reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes));

        auto* c = &connection;
        auto* theCookie = &cookie;
        auto dcpOpen = [=, &flags, &theCookie]() -> ENGINE_ERROR_CODE {
            return c->getBucketEngine()->dcp.open(
                    c->getBucketEngineAsV0(),
                    theCookie,
                    req->message.header.request.opaque,
                    ntohl(req->message.body.seqno),
                    flags,
                    {name, nkey},
                    {req->bytes + sizeof(req->bytes) + nkey, valuelen});
        };

        // Collections Prototype: The following code allows the bucket to decide
        // if this stream should be forced to being collection aware. So we can
        // run with collections enabled, but with a non-collection bucket and a
        // collection bucket. This is only whilst collections are in development
        if (ret == ENGINE_SUCCESS) {
            ret = dcpOpen();
            if (settings.isCollectionsPrototypeEnabled() &&
                ret != ENGINE_SUCCESS) {
                flags |= DCP_OPEN_COLLECTIONS;
                ret = dcpOpen();
                LOG_NOTICE(
                        &connection,
                        "%u: Retried DCP open with DCP_OPEN_COLLECTIONS ret:%d",
                        connection.getId(),
                        ret);
            }
        }
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS: {
        const bool dcpXattrAware = (flags & DCP_OPEN_INCLUDE_XATTRS) != 0 &&
                                   connection.selectedBucketIsXattrEnabled();
        const bool dcpNoValue = (flags & DCP_OPEN_NO_VALUE) != 0;
        const bool dcpCollections = (flags & DCP_OPEN_COLLECTIONS) != 0;
        const bool dcpDeleteTimes =
                (flags & DCP_OPEN_INCLUDE_DELETE_TIMES) != 0;
        connection.setDcpXattrAware(dcpXattrAware);
        connection.setDcpNoValue(dcpNoValue);
        connection.setDcpCollectionAware(dcpCollections);
        connection.setDcpDeleteTimeEnabled(dcpDeleteTimes);

        // @todo Keeping this as NOTICE while waiting for ns_server
        //       support for xattr over DCP (to make it easier to debug
        ///      see MB-22468
        LOG_NOTICE(
                &connection,
                "%u: DCP connection opened successfully. flags:{%s%s%s%s%s} %s",
                connection.getId(),
                dcpNotifier ? "NOTIFIER " : "",
                dcpXattrAware ? "INCLUDE_XATTRS " : "",
                dcpNoValue ? "NO_VALUE " : "",
                dcpCollections ? "COLLECTIONS " : "",
                dcpDeleteTimes ? "DELETE_TIMES " : "",
                connection.getDescription().c_str());

        audit_dcp_open(&connection);
        cookie.sendResponse(cb::mcbp::Status::Success);
        break;
    }

    case ENGINE_DISCONNECT:
        connection.setState(McbpStateMachine::State::closing);
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}
