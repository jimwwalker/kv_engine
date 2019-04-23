/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "mock_hooks_api.h"

/*
 * A mock implementation of the getHooksApi() function (and associated
 * functions).
 *
 * All hook functions will do nothing.
 */

extern "C" {
    static int mock_get_extra_stats_size() {
        return 0;
    }

    static void mock_get_allocator_stats(allocator_stats*) {
        // Empty
    }

    static size_t mock_get_allocation_size(const void*) {
        return 0;
    }
}

ServerAllocatorIface* getHooksApi(void) {
    static ServerAllocatorIface hooksApi;
    hooksApi.get_extra_stats_size = mock_get_extra_stats_size;
    hooksApi.get_allocator_stats = mock_get_allocator_stats;
    hooksApi.get_allocation_size = mock_get_allocation_size;
    return &hooksApi;
}
