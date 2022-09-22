/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "utilities/scheduling_monitor.h"

#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/io/async/EventBase.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <iostream>
#include <memory>

using namespace std::chrono_literals;

class Handler : public folly::AsyncSignalHandler {
public:
    Handler(folly::EventBase* evBase) : folly::AsyncSignalHandler(evBase) {
    }

    void signalReceived(int signum) noexcept override {
        SchedulingMonitor::instance().setShutdown();
        getEventBase()->terminateLoopSoon();
    }
};

int main() {
    cb::logger::createConsoleLogger();

    auto evBase = std::make_unique<folly::EventBase>();
    auto sigHandler = std::make_unique<Handler>(evBase.get());
    sigHandler->registerSignalHandler(2);
    SchedulingMonitor::instance(100ms, 150ms).beginMonitoring(*evBase);

    // SIGINT to stop
    evBase->loopForever();

    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << SchedulingMonitor::instance().toJSON().dump() << std::endl;
    return 0;
}