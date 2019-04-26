/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

/**
 * Use this file as an abstraction to the underlying hooks api
 */

#include <stdint.h>
#include <stdlib.h>
#include <vector>

#ifndef __cplusplus
#include <stdbool.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct allocator_ext_stat {
    char key[48];
    size_t value;
} allocator_ext_stat;

typedef struct allocator_stats {
    /* Bytes of memory allocated by the application. Doesn't include allocator
       overhead or fragmentation. */
    size_t allocated_size;

    /* Bytes of memory reserved by the allocator */
    size_t heap_size;

    /* mem occupied by allocator metadata */
    size_t metadata_size;

    /* Memory overhead of the allocator*/
    size_t fragmentation_size;

    /* memory that has not been given back to the OS */
    size_t retained_size;

    /* max bytes in resident pages mapped by the allocator*/
    size_t resident_size;

    /* Vector of additional allocator-specific statistics */
    std::vector<allocator_ext_stat> ext_stats;

} allocator_stats;

/**
 * Engine allocator hooks for memory tracking.
 */
struct ServerAllocatorIface {

    /**
     * Returns the number of extra stats for the current allocator.
     */
    int (*get_extra_stats_size)(void);

    /**
     * Obtains relevant statistics from the allocator. Every allocator
     * is required to return total allocated bytes, total heap bytes,
     * total free bytes, and toal fragmented bytes. An allocator will
     * also provide a varying number of allocator specific stats
     */
    void (*get_allocator_stats)(allocator_stats*);

    /**
     * Returns the total bytes allocated by the allocator. This value
     * may be computed differently based on the allocator in use.
     */
    size_t (*get_allocation_size)(const void*);

    /**
     * Fills a buffer with special detailed allocator stats.
     */
    void (*get_detailed_stats)(char*, int);

    /**
     * Attempts to release free memory back to the OS.
     */
    void (*release_free_memory)(void);

    /**
     * Enables / disables per-thread caching by the allocator
     * __for the calling thread__. Returns if the thread cache was enabled
     * before the call.
     */
    bool (*enable_thread_cache)(bool enable);

    /**
     * Gets a property by name from the allocator.
     * @param name property name
     * @param value destination for numeric value from the allocator
     * @return whether the call was successful
     */
    bool (*get_allocator_property)(const char* name, size_t* value);
};

#ifdef __cplusplus
}
#endif
