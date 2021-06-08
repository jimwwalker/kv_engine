/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "pid_controller.h"
#include "bucket_logger.h"

PIDController::PIDController(float target,
                             float kp,
                             float ki,
                             float kd,
                             std::chrono::milliseconds dt)
    : kp{kp},
      ki{ki},
      kd{kd},
      dt{dt},

      target(target),
      lastStep{std::chrono::steady_clock::now()} {
}

float PIDController::stepCore(
        float current, std::chrono::time_point<std::chrono::steady_clock> now) {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - lastStep);
    lastStep = now;
    // try to stabilise the step to a fixed interval
    // link to blog
    if (duration < dt) {
        return output;
    }

    float error = target - current;

    integral += (error * duration.count());

    float derivative = (error - previousError) / duration.count();

    output = (kp * error) + (ki * integral) + (kd * derivative);
    EP_LOG_INFO("PID::Step error:{}, i:{}, d:{}, dt:{}....  {} + {} + {} = {}",
                error,
                integral,
                derivative,
                duration.count(),
                (kp * error),
                (ki * integral),
                (kd * derivative),
                output);

    previousError = error;

    return output;
}

void PIDController::reset() {
    integral = 0;
    previousError = 0;
    output = 0;
}

std::ostream& operator<<(std::ostream& os, const PIDController& pid) {
    os << "kp:" << pid.kp << ", ki:" << pid.ki << ", kd:" << pid.kd
       << ", target:" << pid.target << ", integral:" << pid.integral
       << ", lastStep:" << pid.lastStep.time_since_epoch().count()
       << ", output:" << pid.output;
    return os;
}
