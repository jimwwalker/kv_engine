/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

/*
 * Benchmark memory accounting
 */

#include <benchmark/benchmark.h>

#include <platform/sysinfo.h>

#include "stats.h"

class TestEpStats : public EPStats {
public:
    void clearMemUsed() {
        localMemCounter.get()->used = 0;
    }
};

class MemoryAllocationStat : public benchmark::Fixture {
public:
    TestEpStats stats;
};

BENCHMARK_DEFINE_F(MemoryAllocationStat, AllocNRead1)(benchmark::State& state) {
    if (state.thread_index == 0) {
        stats.reset();
        stats.memoryTrackerEnabled = true;
    }

    stats.memAllocated(0);
    while (state.KeepRunning()) {
        // range = allocations per read
        for (int i = 0; i < state.range(0); i++) {
            // Do this if/else to match the branching needed when we have the
            // corestore version
            if (i == 0) {
                stats.memAllocated(128);
            } else {
                stats.memAllocated(128);
            }
        }
        stats.getTotalMemoryUsed();
    }
}


BENCHMARK_DEFINE_F(MemoryAllocationStat, AllocNReadM)(benchmark::State& state) {
    if (state.thread_index == 0) {
        stats.reset();
        stats.memoryTrackerEnabled = true;
        stats.mem_merge_count_threshold = ~1; // Disable counting
        stats.mem_merge_bytes_threshold = 10240; // only work with bytes alloc
    }

    stats.memAllocated(0); // this ensures TLS is allocated outside of timing
    while (state.KeepRunning()) {
        // range = allocations per read
        for (int i = 0; i < state.range(0); i++) {
            if (i == 0) {
                stats.clearMemUsed();
                stats.memAllocated(128);
            } else {
                stats.memAllocated(128);
            }
        }
        for (int j = 0; j < state.range(1); j++) {
            stats.getTotalMemoryUsed();
        }
    }
}

// Test covers a range seen from a running cluster (with pillowfight load)
// The range was discovered by counting calls to memAllocated/deallocated and
// then logging how many had occurred for each read (getTotalMemoryUsed)
BENCHMARK_REGISTER_F(MemoryAllocationStat, AllocNRead1)
        ->Threads(cb::get_cpu_count() * 4)
        ->RangeMultiplier(2)
        ->Range(0, 4000);

BENCHMARK_REGISTER_F(MemoryAllocationStat, AllocNReadM)
        ->Threads(cb::get_cpu_count() * 4)
        ->RangeMultiplier(2)
        ->Ranges({{0, 4000}, {128, 4000}});
