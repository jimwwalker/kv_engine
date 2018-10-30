/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Time keeping for memcached.
 */

#include "memcached/types.h"

#ifndef MC_TIME_H
#define MC_TIME_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Initialise this module.
 */
void mc_time_init(struct event_base* ev_base);

/*
 * Init the epoch time tracking variables
 */
void mc_time_init_epoch(void);

/*
 * Return a monotonically increasing value.
 * The value returned represents seconds since memcached started.
 */
rel_time_t mc_time_get_current_time(void);

/*
 * Update a number of time keeping variables and account for system
 * clock changes.
 */
void mc_time_clock_tick(void);

/*
 * Convert a relative time value to an absolute time.
 *
 * Note that the following usage approximates gettimeofday()
 * I.e. seconds since EPOCH without actually calling gettimeofday.
 *
 * time_convert_to_abs_time(time_get_current_time());
 */
time_t mc_time_convert_to_abs_time(const rel_time_t rel_time);

/**
 * Convert a time stamp to an absolute time stamp.
 * @param t a protocol expiry time-stamp
 * @oaram limit an optional limit, see server_api.h 'realtime' description
 */
rel_time_t mc_time_convert_to_real_time(rel_time_t t);

time_t mc_time_limit_abstime(time_t t, std::chrono::seconds limit);

#ifdef __cplusplus
}
#endif
#endif
