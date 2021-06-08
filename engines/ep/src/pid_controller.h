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

#include <chrono>

class PIDController {
public:
    PIDController(float target,
                  float kp,
                  float ki,
                  float kd,
                  std::chrono::milliseconds dt);

    template <class Clock = std::chrono::steady_clock>
    float step(float current) {
        return stepCore(current, Clock::now());
    }

    void reset();

private:
    float stepCore(float current,
                   std::chrono::time_point<std::chrono::steady_clock> now);

    float kp{0.0};
    float ki{0.0};
    float kd{0.0};
    std::chrono::milliseconds dt{0};
    float integral{0.0};
    float target{0.0};
    float previousError{0.0};
    float output{0.0};
    std::chrono::time_point<std::chrono::steady_clock> lastStep;
    friend std::ostream& operator<<(std::ostream&, const PIDController&);
};

std::ostream& operator<<(std::ostream& os, const PIDController& pid);
