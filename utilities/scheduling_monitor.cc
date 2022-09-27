/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "scheduling_monitor.h"

#include <folly/io/async/EventBase.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/platform_time.h>

SchedulingMonitor& SchedulingMonitor::instance(time_unit interval,
                                               time_unit tolerance) {
    static SchedulingMonitor instance{interval, tolerance};
    return instance;
}

SchedulingMonitor::SchedulingMonitor(time_unit interval, time_unit tolerance)
    : interval(interval), warnTolerance(tolerance) {
}

void SchedulingMonitor::beginMonitoring(folly::EventBase& eventBase) {
    continueMonitoring(eventBase);
}

void SchedulingMonitor::continueMonitoring(folly::EventBase& eventBase) {
    if (shutdown) {
        return;
    }

    // record time when we request the sleep
    start = std::chrono::system_clock::now();
    eventBase.schedule([this, &eventBase]() { callback(eventBase); }, interval);
}

void SchedulingMonitor::callback(folly::EventBase& eventBase) {
    // running - calculate our expected runtime and compare with now
    auto expectedNow = start + interval;
    auto now = std::chrono::system_clock::now();

    auto difference = std::chrono::duration_cast<time_unit>(now - expectedNow);

    accumulativeDifference += difference.count(); // negative??
    samples++;

    if (difference > warnTolerance) {
        LOG_WARNING(
                "SchedulingMonitor: callback delayed by {} "
                "accumulativeDifference:{}, samples:{}",
                std::chrono::duration_cast<time_unit>(difference).count(),
                accumulativeDifference,
                samples);
    }

    continueMonitoring(eventBase);
}

nlohmann::json SchedulingMonitor::toJSON() const {
    nlohmann::json json;

    json["interval"] = interval.count();
    json["warn"] = warnTolerance.count();
    json["samples"] = samples;
    json["difference"] = accumulativeDifference;

    return json;
}