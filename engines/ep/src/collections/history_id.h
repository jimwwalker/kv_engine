/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#include "collections/manifest_generated.h"

#include <array>
#include <string>

#pragma once

/**
 * Class representing the cluster history-id as seen in collection manifests
 */
namespace Collections {

class HistoryID {
public:
    // @todo remove.
    // Default construction only required until historyID is propagated to all
    // areas, vbuckets new and warmed-up, manifests new and warmed-up.
    HistoryID() = default;

    /**
     * Construct from 'human-readable' string, each byte is a 2 character hex
     * Input is expected to be 32 characters all in the range 0-9, a-f and A-F
     * @throws invalid_argument if string cannot be converted
     */
    HistoryID(std::string_view string);

    /**
     * Construct from the Flatbuffer type
     */
    HistoryID(FlatbufferHistoryID constructId);

    /**
     * @return human-readable string (ascii/hex string)
     */
    std::string to_string() const;

    /**
     * @return flatbuffer view of HistoryID
     */
    FlatbufferHistoryID toFlatbuffer() const {
        return id;
    }

    /// @return true if id is equal
    bool operator==(const HistoryID&) const;

    /// @return true if id is not equal
    bool operator!=(const HistoryID&) const;

private:
    FlatbufferHistoryID id;
};

std::ostream& operator<<(std::ostream& os, const HistoryID& manifest);

} // namespace Collections