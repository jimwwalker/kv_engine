/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json_fwd.hpp>
#include <chrono>

using namespace std::chrono_literals;

namespace folly {
class EventBase;
}

/**
 * The SchedulingMonitor a singleton that captures statistics regarding the
 * performance of the system scheduler.
 */
class SchedulingMonitor {
public:
    static SchedulingMonitor& instance(
            std::chrono::milliseconds interval = 100ms,
            std::chrono::milliseconds tolerance = 100ms);

    SchedulingMonitor(std::chrono::milliseconds interval,
                      std::chrono::milliseconds tolerance);

    /**
     * Starts the SchedulingMonitor, which is a callback that runs every
     * interval milliseconds and accounts for how long it really took to
     * execute the callback (scheduling latency)
     */
    void beginMonitoring(folly::EventBase& eventBase);

    void setShutdown() {
        shutdown = true;
    }

    size_t getSamples() const {
        return samples;
    }

    size_t getDifference() const {
        return accumulativeDifference;
    }

    /**
     * Create a JSON representation of this, includes current stats and config
     */
    nlohmann::json toJSON() const;

private:
    void continueMonitoring(folly::EventBase& eventBase);
    void callback(folly::EventBase& eventBase);

    /// How long between executions of the callback
    const std::chrono::milliseconds interval{100};

    /// A tolerance which when exceeded triggers a log warning message
    const std::chrono::milliseconds warnTolerance{100};

    /// records the time_point of when the schedule request occurs
    std::chrono::system_clock::time_point start;

    /// the difference between calling schedule and when callback runs
    size_t accumulativeDifference{0};
    /// how many updates have been made to accumulativeDifference
    size_t samples{0};

    bool shutdown{false};
};
