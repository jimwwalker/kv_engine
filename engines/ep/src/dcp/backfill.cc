/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "dcp/active_stream.h"
#include "dcp/backfill.h"

#include <phosphor/phosphor.h>

DCPBackfill::DCPBackfill(Vbid vbid) : vbid(vbid) {
    *state.wlock() = State::Create;
}

backfill_status_t DCPBackfill::run() {
    auto lockedState = state.wlock();
    auto runtimeGuard =
            folly::makeGuard([start = std::chrono::steady_clock::now(), this] {
                runtime += (std::chrono::steady_clock::now() - start);
            });

    TRACE_EVENT2("dcp/backfill",
                 "DCPBackfill::run",
                 "vbid",
                 getVBucketId().get(),
                 "state",
                 uint8_t(*lockedState));

    backfill_status_t status = backfill_finished;
    switch (*lockedState) {
    case State::Create:
        status = create();
        Expects(status == backfill_success || status == backfill_snooze ||
                status == backfill_finished);
        if (status == backfill_success) {
            transitionState(*lockedState, State::Scan);
        }
        break;
    case State::Scan:
        status = scan();
        Expects(status == backfill_success || status == backfill_finished);
        break;
    case State::Done:
        // As soon as we return finished, we change to State::Done, finished
        // signals the caller should not call us again so throw if that occurs
        throw std::logic_error(
                "DCPBackfill::run: run should not be called in State::Done " +
                getVBucketId().to_string());
    }

    if (status == backfill_finished) {
        transitionState(*lockedState, State::Done);
    }

    return status;
}

void DCPBackfill::cancel() {
    if (*state.rlock() != State::Done) {
        EP_LOG_WARN(
                "DCPBackfill::cancel ({}) cancelled before reaching "
                "State::Done",
                getVBucketId());
    }
}

std::string DCPBackfill::to_string(State state) {
    switch (state) {
    case State::Create:
        return "Create";
    case State::Scan:
        return "Scan";
    case State::Done:
        return "Done";
    }
    throw std::logic_error("DCPBackfill::to_string: Invalid State " +
                           std::to_string(int(state)));
}

void DCPBackfill::transitionState(State& currentState, State newState) {
    bool validTransition = false;
    switch (newState) {
    case State::Create:
        // No valid transition to 'create'
        break;
    case State::Scan:
        if (currentState == State::Create) {
            validTransition = true;
        }
        break;
    case State::Done:
        if (currentState == State::Create || currentState == State::Scan) {
            validTransition = true;
        }
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "DCPBackfill::transitionState:"
                " newState (which is " +
                to_string(newState) +
                ") is not valid for currentState (which is " +
                to_string(currentState) + ")");
    }

    currentState = newState;
}
