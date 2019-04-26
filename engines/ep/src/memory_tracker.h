/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#include <memcached/server_allocator_iface.h>
#include <platform/platform_thread.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>

/**
 * This class is used by ep-engine to hook into memcached's memory tracking
 * capabilities.
 */
class MemoryTracker {
public:
    ~MemoryTracker();

    /* Creates the singleton instance of the MemoryTracker (if it doesn't exist).
     * Thread-safe, so ok for multiple threads to attempt to create at the
     * same time.
     * @return The MemoryTracker singleton.
     */
    static MemoryTracker* getInstance(const ServerAllocatorIface& hook_api_);

    static void destroyInstance();

    void getAllocatorStats(std::map<std::string, size_t> &alloc_stats);

    void updateStats();

    void getDetailedStats(char* buffer, int size);

private:
    MemoryTracker(const ServerAllocatorIface& hooks_api_);

    // Helper function for construction - connects the tracker
    // to the memory allocator via alloc_hooks.
    void connectHooks();

    // Function for the stats updater main loop.
    static void statsThreadMainLoop(void* arg);


    // Singleton memory tracker and mutex guarding it's creation.
    static std::atomic<MemoryTracker*> instance;
    static std::mutex instance_mutex;

    cb_thread_t statsThreadId;
    allocator_stats stats;

    // Mutex guarding the shutdown condvar.
    std::mutex mutex;
    // Condition variable used to signal shutdown to the stats thread.
    std::condition_variable shutdown_cv;

    // Should the stats thread keep running
    std::atomic<bool> statsThreadRunnable{false};

    // Memory allocator hooks API to use (needed by New / Delete hook
    // functions)
    ServerAllocatorIface hooks_api;
};
