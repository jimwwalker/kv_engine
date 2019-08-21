#pragma once

// external symbols for ep.so

extern "C" {
MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_ep_engine_instance(GET_SERVER_API get_server_api,
                                            EngineIface** handle);

MEMCACHED_PUBLIC_API
void destroy_ep_engine(void);
}