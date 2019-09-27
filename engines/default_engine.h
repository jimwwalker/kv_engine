/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <memcached/engine.h>
#include <memcached/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

    MEMCACHED_PUBLIC_API
    ENGINE_ERROR_CODE create_memcache_instance(GET_SERVER_API get_server_api,
                                               EngineIface** handle);

#ifdef __cplusplus
}
#endif
