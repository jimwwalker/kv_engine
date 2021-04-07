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

#include "collections/history_id.h"

#include <spdlog/fmt/fmt.h>
#include <charconv>

namespace Collections {

HistoryID::HistoryID(std::string_view string) {
    if (string.size() != sizeof(id) * 2) {
        throw std::invalid_argument("Cannot construct HistoryID from size:" +
                                    std::to_string(string.size()) +
                                    " value:" + std::string(string));
    }

    auto sItr = string.begin();
    auto convert = [&sItr, &string]() {
        uint64_t data{0};
        for (size_t index = 0; index < sizeof(uint64_t); index++) {
            uint64_t result = 0;
            auto [p, ec] = std::from_chars(sItr, sItr + 2, result, 16);
            (void)p;
            if (ec == std::errc()) {
                data |= result << index * 8;
            } else {
                throw std::invalid_argument(
                        "Cannot construct HistoryID from_chars failed size:" +
                        std::to_string(string.size()) +
                        " value:" + std::string(string));
            }
            sItr += 2;
        }
        return data;
    };

    auto data0 = convert();
    auto data1 = convert();
    id = FlatbufferHistoryID(data0, data1);
}

HistoryID::HistoryID(FlatbufferHistoryID constructId) : id(constructId) {
}

std::string HistoryID::to_string() const {
    std::string rv;
    rv.reserve(sizeof(id) * 2);
    auto convert = [&rv](uint64_t data) {
        for (size_t index = 0; index < sizeof(data); index++) {
            uint8_t byte = uint8_t((data >> (index * 8)) & 0xff);
            fmt::memory_buffer buf;
            format_to(buf, "{:02x}", byte);
            rv.append(buf.begin(), buf.end());
        }
    };

    convert(id.data0());
    convert(id.data1());
    return rv;
}

bool HistoryID::operator==(const HistoryID& other) const {
    return id.data0() == other.id.data0() && id.data1() == other.id.data1();
}

bool HistoryID::operator!=(const HistoryID& other) const {
    return !(*this == other);
}

std::ostream& operator<<(std::ostream& os, const HistoryID& historyId) {
    os << historyId.to_string();
    return os;
}

} // namespace Collections