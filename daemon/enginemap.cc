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
#include "enginemap.h"
#include "engines/crash_engine/crash_engine_public.h"
#include "engines/default_engine/default_engine_public.h"
#include "engines/ep/src/ep_engine_public.h"
#include "engines/ewouldblock_engine/ewouldblock_engine_public.h"
#include "engines/nobucket/nobucket_public.h"
#include "logger/logger.h"

#include <platform/dirutils.h>
#include <string>

EngineIface* new_engine_instance(BucketType type,
                                 const std::string& name,
                                 GET_SERVER_API get_server_api) {
    EngineIface* ret = nullptr;
    ENGINE_ERROR_CODE status = ENGINE_KEY_ENOENT;
    switch (type) {
    case BucketType::NoBucket: {
        status = create_no_bucket_instance(get_server_api, &ret);
        break;
    }
    case BucketType::Memcached: {
        status = create_memcache_instance(get_server_api, &ret);
        break;
    }
    case BucketType::Couchstore: {
        status = create_ep_engine_instance(get_server_api, &ret);
        break;
    }
    case BucketType::EWouldBlock: {
        status = create_ewouldblock_instance(get_server_api, &ret);
        break;
    }
    case BucketType::Unknown: {
        // fall through with status == ENGINE_KEY_ENOENT
        break;
    }
    }

    if (status != ENGINE_SUCCESS) {
        throw std::runtime_error(
                "new_engine_instance(): Failed to create name:" + name +
                " of type:" + to_string(type) +
                " error:" + cb::to_string(cb::to_engine_errc(status)));
    }

    return ret;
}

void create_crash_instance() {
    EngineIface* h;
    if (create_crash_engine_instance(nullptr, &h) != ENGINE_SUCCESS) {
        throw std::runtime_error(
                "create_crash_instance(): Failed to create instance of crash "
                "engine");
    }
    h->initialize(nullptr);
}

BucketType module_to_bucket_type(const std::string& module) {
    std::string nm = cb::io::basename(module.c_str());
    if (nm == "nobucket.so") {
        return BucketType::NoBucket;
    } else if (nm == "default_engine.so") {
        return BucketType::Memcached;
    } else if (nm == "ep.so") {
        return BucketType::Couchstore;
    } else if (nm == "ewouldblock_engine.so") {
        return BucketType::EWouldBlock;
    }
    return BucketType::Unknown;
}

void shutdown_engine(BucketType type) {
    switch (type) {
    case BucketType::NoBucket: {
        destroy_no_bucket_engine();
        break;
    }
    case BucketType::Memcached: {
        destroy_default_engine();
        break;
    }
    case BucketType::Couchstore: {
        destroy_ep_engine();
        break;
    }
    case BucketType::EWouldBlock: {
        destroy_ewouldblock_engine();
        break;
    }
    case BucketType::Unknown: {
        throw std::runtime_error("shutdown_engine(): unknown type:" +
                                 to_string(type));
    }
    }
}

void shutdown_all_engines(void) {
    // Using this fall-through switch so that any new BucketTypes are considered
    // for shutdown.
    switch (BucketType::NoBucket) {
    case BucketType::NoBucket: {
        shutdown_engine(BucketType::NoBucket);
    }
    case BucketType::Memcached: {
        shutdown_engine(BucketType::Memcached);
    }
    case BucketType::Couchstore: {
        shutdown_engine(BucketType::Couchstore);
    }
    case BucketType::EWouldBlock: {
        shutdown_engine(BucketType::EWouldBlock);
    }
    case BucketType::Unknown: {
        // do nothing
    }
    }
}
