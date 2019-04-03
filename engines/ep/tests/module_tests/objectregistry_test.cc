/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include <folly/portability/GTest.h>
#include <jemalloc/jemalloc.h>
#include <array>
#include <memory>

class JimsJeMallocModelTest : virtual public ::testing::Test {
protected:
    void SetUp() override {
    }
    void TearDown() override {
    }

};

using Ptr = std::unique_ptr<char>;

// Engine allocates all memory through the given arena
class Engine {
public:
    Engine() {
        unsigned a = 0;
        size_t sz = sizeof(unsigned);
        if (je_mallctl("arenas.create", (void*)&a, &sz, nullptr, 0) != 0) {
            throw std::logic_error("Could not allocate arena");
        }
        arena = a;

        if (je_mallctl("tcache.create", (void*)&a, &sz, nullptr, 0) != 0) {
            throw std::logic_error("Could not allocate tcache");
        }
        tcache = a;
    }

    void allocate(size_t sz) {
        if (allocateIndex >= allocations.size()) {
            throw std::logic_error("Engine full");
        }
        allocations[allocateIndex].reset(
                reinterpret_cast<char*>(je_mallocx(sz, MALLOCX_ARENA(arena))));
        mem_used += sz;
    }

    void tcache_allocate(size_t sz) {
        if (allocateIndex >= allocations.size()) {
            throw std::logic_error("Engine full");
        }
        allocations[allocateIndex].reset(reinterpret_cast<char*>(
                je_mallocx(sz, MALLOCX_ARENA(arena) | MALLOCX_TCACHE(tcache))));
        mem_used += sz;
    }

    int arena{0};
    short tcache{0};
    int allocateIndex{0};
    size_t mem_used{0};
    std::vector<Ptr> allocations{500};
};

static void allocations(Engine& e, bool tcache) {
    std::array<int, 8> sizes = {{320, 384, 448, 512, 640, 768, 896, 1024}};
    for (auto s : sizes) {
        if (tcache) {
            e.tcache_allocate(s);
        } else {
            e.allocate(s);
        }
    }
}

TEST_F(JimsJeMallocModelTest, default_tcache_allocate) {
    Engine engine1;
    Engine engine2;
    Engine engine3;

    allocations(engine1, false);
    allocations(engine2, false);
    allocations(engine3, false);

    je_malloc_stats_print(NULL, NULL, NULL);
    std::cerr << "engine1.mem_used:" << engine1.mem_used << std::endl;
    std::cerr << "engine2.mem_used:" << engine2.mem_used << std::endl;
    std::cerr << "engine3.mem_used:" << engine3.mem_used << std::endl;
}

TEST_F(JimsJeMallocModelTest, own_tcache_allocate) {
    Engine engine1;
    Engine engine2;
    Engine engine3;

    allocations(engine1, true);
    allocations(engine2, true);
    allocations(engine3, true);

    je_malloc_stats_print(NULL, NULL, NULL);
    std::cerr << "engine1.mem_used:" << engine1.mem_used << std::endl;
    std::cerr << "engine2.mem_used:" << engine2.mem_used << std::endl;
    std::cerr << "engine3.mem_used:" << engine3.mem_used << std::endl;
}