/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#pragma once

#include <platform/sized_buffer.h>
#include <array>
#include <gsl/gsl>
#include <optional>
#include <type_traits>

namespace cb {
namespace mcbp {

/**
 * Helper code for encode and decode of LEB128 values.
 * - mcbp encodes collection-ID as an unsigned LEB128
 * - see https://en.wikipedia.org/wiki/LEB128
 */

// Empty, non specialised version of the decoder class
template <class T, class Enable = void>
class unsigned_leb128 {};

/**
 * For encoding a unsigned T leb128, class constructs from a T value and
 * provides a const_byte_buffer for access to the encoded
 */
template <class T>
class unsigned_leb128<
        T,
        typename std::enable_if<std::is_unsigned<T>::value>::type> {
public:
    unsigned_leb128(T in) {
        while (in > 0) {
            auto byte = gsl::narrow_cast<uint8_t>(in & 0x7full);
            in >>= 7;

            // In has more data?
            if (in > 0) {
                byte |= 0x80;
                encodedData[encodedSize - 1] = byte;
                // Increase the size
                encodedSize++;
            } else {
                encodedData[encodedSize - 1] = byte;
            }
        }
    }

    cb::const_byte_buffer get() const {
        return {encodedData.data(), encodedSize};
    }

    const uint8_t* begin() const {
        return encodedData.data();
    }

    const uint8_t* end() const {
        return encodedData.data() + encodedSize;
    }

    const uint8_t* data() const {
        return encodedData.data();
    }

    size_t size() const {
        return encodedSize;
    }

    constexpr static size_t getMaxSize() {
        return maxSize;
    }

    /**
     * decode returns the decoded T and a const_byte_buffer initialised with the
     * data following the leb128 data.
     *
     * @param buf buffer containing a leb128 encoded value (of size T). This can
     *            be a prefix on some other data, the decode will only process
     *            up to the maximum number of bytes permitted for the type T.
     *            E.g. uint32_t use 5 bytes maximum. buf.size must be >= 1
     *
     * @returns A std::pair where first is the decoded value and second is a
     *          buffer initialised with the data following the leb128 data. Note
     *          if the input buf was 100% only a leb128, the returned buffer
     *          will point outside of the input buf, but size will be 0.
     *
     * @throws std::invalid_argument if the input is not a valid leb128, this
     *         means decode processed 'getMaxSize' bytes without a stop byte.
     */
    static std::pair<T, cb::const_byte_buffer> decode(
            cb::const_byte_buffer buf);

    /**
     * decodeCanonical returns the decoded value of type T and a
     * const_byte_buffer initialised with the data following the leb128 data.
     *
     * This version does not throw an exception, but returns failure for two
     * reasons.
     *  - no-stop byte found
     *  - non-canonical encoding was used, e.g. 0x81.00 instead of 0x01
     *
     * The caller will have to inspect the input to determine the error.
     *
     * @param buf buffer containing a leb128 encoded value (of size T). This can
     *            be a prefix on some other data, the decode will only process
     *            up to the maximum number of bytes permitted for the type T.
     *            E.g. uint32_t use 5 bytes maximum. buf.size must be >= 1
     *
     * @returns On error a std::pair where the second buffer has a nullptr and
     *          zero size, first is set to 0. On success a std::pair where first
     *          is the decoded value and second is a buffer initialised with the
     *          data following the leb128 data. Note if the input buf was 100%
     *          only a leb128, the returned buffer will point outside of the
     *          input buf, but size will be 0.
     */
    static std::pair<T, cb::const_byte_buffer> decodeCanonical(
            cb::const_byte_buffer buf);

    /**
     * decodeNoThrow returns the decoded value of type T and a const_byte_buffer
     * initialised with the data following the leb128 data.
     *
     * This version does not throw an exception, but returns failure if no-stop
     * is byte found.
     *
     * @param buf buffer containing a leb128 encoded value (of size T). This can
     *            be a prefix on some other data, the decode will only process
     *            up to the maximum number of bytes permitted for the type T.
     *            E.g. uint32_t use 5 bytes maximum. buf.size must be >= 1
     *
     * @returns On error a std::pair where the second buffer has a nullptr and
     *          zero size, first is set to 0. On success a std::pair where first
     *          is the decoded value and second is a buffer initialised with the
     *          data following the leb128 data. Note if the input buf was 100%
     *          only a leb128, the returned buffer will point outside of the
     *          input buf, but size will be 0.
     */
    static std::pair<T, cb::const_byte_buffer> decodeNoThrow( cb::const_byte_buffer buf);

    // testing chunk decode
    static std::pair<T, cb::const_byte_buffer> decodeCanonical_chunk(
            cb::const_byte_buffer buf);

protected:
    struct NoThrow {};
    /**
     * decode returns the decoded value of type T and a const_byte_buffer
     * initialised with the data following the leb128 data.
     *
     * This is the protected inner method used by the public decode, does not
     * throw for bad input (allows public methods to decide error fate)
     */
    static std::pair<T, cb::const_byte_buffer> decode(cb::const_byte_buffer buf,
                                                      NoThrow);

    static std::pair<T, cb::const_byte_buffer> decode_chunk(
            cb::const_byte_buffer buf, NoThrow);

    template <typename, typename>
    friend class unsigned_leb128;

    /**
     * Test that a decoded value was encoded in the canonical format.
     *
     * The test works by examining the length and comparing against a constant.
     * The constant is the maximum value that can be encoded as leb128 in
     * 'encodedLength - 1' bytes.
     *
     * For example if the encodedLength was 2 and the value was less than or
     * equal to 127, a non-canonical encoding was used, 127 and less can and
     * must be encoded in only 1 byte.
     *
     * So the test when encoded length is 2 is that the value is greater than
     * 127. If the encoded length is 3 the value must be greater than 16383 and
     * so on.
     *
     * @param value The integer that was decoded
     * @param encodedLength How many bytes the leb128 encoding used
     */
    static inline bool is_canonical(uint64_t value, size_t encodedLength);

private:
    // Larger T may need a larger array
    static_assert(sizeof(T) <= 8, "Class is only valid for uint 8/16/64");

    // value is large enough to store ~0 as leb128
    static constexpr size_t maxSize = sizeof(T) + (((sizeof(T) + 1) / 8) + 1);
    std::array<uint8_t, maxSize> encodedData{};
    uint8_t encodedSize{1};
};

// Generate the maximum value that can be encoded in nbytes
#define MAX_LEB128(nbytes) \
    ((0x7full << (nbytes - 1) * 7) | ((1ull << (nbytes - 1) * 7) - 1ull))

template <>
inline bool unsigned_leb128<uint8_t>::is_canonical(uint64_t value,
                                                   size_t encodedLength) {
    return (encodedLength == 2 && value > MAX_LEB128(1)) || encodedLength == 1;
}

template <>
inline bool unsigned_leb128<uint16_t>::is_canonical(uint64_t value,
                                                    size_t encodedLength) {
    if (unsigned_leb128<uint8_t>::is_canonical(value, encodedLength)) {
        return true;
    }
    return encodedLength == 3 && value > MAX_LEB128(2);
}

template <>
inline bool unsigned_leb128<uint32_t>::is_canonical(uint64_t value,
                                                    size_t encodedLength) {
    if (unsigned_leb128<uint16_t>::is_canonical(value, encodedLength)) {
        return true;
    }

    switch (encodedLength) {
    case 4:
        return value > MAX_LEB128(3);
    case 5:
        return value > MAX_LEB128(4);
    }

    return false;
}

template <>
inline bool unsigned_leb128<uint64_t>::is_canonical(uint64_t value,
                                                    size_t encodedLength) {
    // We first have to ask if this is non-canonical for the lower size, e.g.
    // u32. Each size asks the lower size first.
    if (unsigned_leb128<uint32_t>::is_canonical(value, encodedLength)) {
        return true;
    }

    switch (encodedLength) {
    case 6:
        return value > MAX_LEB128(5);
    case 7:
        return value > MAX_LEB128(6);
    case 8:
        return value > MAX_LEB128(7);
    case 9:
        return value > MAX_LEB128(8);
    case 10:
        return value > MAX_LEB128(9);
    }

    return false;
}

template <class T>
std::pair<T, cb::const_byte_buffer>
unsigned_leb128<T, typename std::enable_if<std::is_unsigned<T>::value>::type>::
        decode(cb::const_byte_buffer buf, NoThrow) {
    T rv = buf[0] & 0x7full;
    size_t end = 0;
    // Process up to the end of buf, or the max size for T, this ensures that
    // bad input, e.g. no stop-byte avoids invalid shifts (where shift just
    // keeps getting better). Primarily this gives us much better control over
    // invalid input, e.g. 20 bytes of 0x80 with a stop byte, would of
    // previously decoded to 0, but is really not valid input.
    size_t size =
            std::min(buf.size(), cb::mcbp::unsigned_leb128<T>::getMaxSize());
    if ((buf[0] & 0x80) == 0x80ull) {
        T shift = 7;
        // shift in the remaining data
        for (end = 1; end < size; end++) {
            rv |= (buf[end] & 0x7full) << shift;
            if ((buf[end] & 0x80ull) == 0) {
                break; // no more
            }
            shift += 7;
        }
        // We should of stopped for a stop byte, not the end of the buffer or
        // max encoding
        if (end == size) {
            return {0, cb::const_byte_buffer{}};
        }
    }
    // Return the decoded value and a buffer for any remaining data
    return {rv,
            cb::const_byte_buffer{buf.data() + end + 1,
                                  buf.size() - (end + 1)}};
}

/**
 * decode_unsigned_leb128 returns the decoded T and a const_byte_buffer
 * initialised with the data following the leb128 data. This form of the decode
 * does not throw for invalid input and the caller should always check
 * second.data() for success or error (see returns info).
 *
 * Decode uses a chunk based approach (rather than byte by byte).
 * The input buf is usually the leb128 prefixed data, thus we may often
 * be able to load larger chunks and decode the whole leb128 with one load.
 *
 * The algorithm works by trying to do the largest load it can based on the
 * bytes available, this means we may load the entire leb128 and some of the
 * trailing data. After each load, the leb128 shift/or loop operates on the
 * single register and stops on the stop-byte. If the single load fails to find
 * a stop byte, we loop around and try for the next largest chunk possible.
 *
 * @param buf buffer containing a leb128 encoded value (of size T). This can be
 *            a prefix on some other data, the decode will only process upto the
 *            maximum number of bytes permitted for the type T. E.g. uint32_t
 *            use 5 bytes maximum.
 * @returns On error a std::pair where first is set to 0 and second is nullptr/0
 *          const_byte_buffer. On success a std::pair where first is the decoded
 *          value and second is a buffer initialised with the data following the
 *          leb128 data.
 */
template <class T>
std::pair<T, cb::const_byte_buffer>
unsigned_leb128<T, typename std::enable_if<std::is_unsigned<T>::value>::type>::
        decode_chunk(cb::const_byte_buffer buf, NoThrow) {
    // Take a copy of the buffer size, this is used to determine which chunk
    // can be loaded and is decremented as the leb128 is processed.
    size_t size = buf.size();

    // A second counter is needed so we can count down the valid bytes of leb128
    // and fail for long/bad input
    size_t maxSize = cb::mcbp::unsigned_leb128<T>::getMaxSize();
    // Take a copy of the data pointer, this is were we load from for each
    // chunk.
    const uint8_t* data = buf.data();

    T rv = 0; // return value, the decoded T
    T shift = 0; // the current shift
    do {
        if (size >= sizeof(uint64_t)) {
            // This block is commented with the general approach. Each chunk
            // behaves pretty much the same. Only difference is that the chunks
            // where sizeof(chunk) < 5 have to count down there own size first
            // so they know when the uint16_t etc... has been fully processed.

            // 1) load the maximum size we can given how many bytes are in buf
            uint64_t d = *reinterpret_cast<const uint64_t*>(data);
            size_t chunk = sizeof(uint64_t);
            do {
                // 2) Iterate through the bytes of 'd'
                //    Begin with the LSB, mask off high-bits and shift.
                rv |= T(d & 0x7Full) << shift;

                // 3) A byte has been processed, so reduce size by 1
                size--;

                // 4) Is the current byte the stop byte? I.e. high-bit is 0?
                if ((d & 0x80ull) == 0) {
                    // stop! return current rv and view of the remaining data.
                    return {rv, {buf.data() + (buf.size() - size), size}};
                } else {
                    // 5) No stop-byte so increase shift, and shift out the byte
                    // we just processed.
                    shift += 7;
                    d >>= 8;
                    // 5.1) reduce how many bytes remain in the chunk
                    chunk--;
                    // 5.2) reduce how many bytes of the max leb128 remain
                    maxSize--;
                }
            } while (size && chunk);
            // 6) chunk has been exhausted of any valid leb128-data-bytes, move
            // data pointer by the amount read and loop again for the next chunk
            data += sizeof(uint64_t);
        } else if (size >= sizeof(uint32_t)) {
            uint32_t d = *reinterpret_cast<const uint32_t*>(data);
            size_t chunk = sizeof(uint32_t);
            do {
                rv |= T(d & 0x7Ful) << shift;
                size--;
                if ((d & 0x80ull) == 0) {
                    return {rv, {buf.data() + (buf.size() - size), size}};
                } else {
                    shift += 7;
                    d >>= 8;
                    chunk--;
                    maxSize--;
                }
            } while (size && chunk);
            data += sizeof(uint32_t);
        } else if (size >= sizeof(uint16_t)) {
            uint16_t d = *reinterpret_cast<const uint16_t*>(data);
            size_t chunk = sizeof(uint16_t);
            do {
                rv |= T(d & 0x7Ful) << shift;
                size--;
                if ((d & 0x80ull) == 0) {
                    return {rv, {buf.data() + (buf.size() - size), size}};
                } else {
                    shift += 7;
                    d >>= 8;
                    chunk--;
                    maxSize--;
                }
            } while (size && chunk);
            data += sizeof(uint16_t);
        } else {
            uint8_t d = *data;
            rv |= T(d & 0x7Ful) << shift;
            size--;
            if ((d & 0x80) == 0) {
                return {rv, {buf.data() + (buf.size() - size), size}};
            } else {
                shift += 7;
            }
            data += sizeof(uint8_t);
            maxSize--;
        }
    } while (size);
    return {0, cb::const_byte_buffer{}};
}

/**
 * decode_unsigned_leb128 returns the decoded T and a const_byte_buffer
 * initialised with the data following the leb128 data. This form of the decode
 * throws for invalid input.
 *
 * @param buf buffer containing a leb128 encoded value (of size T)
 * @returns std::pair first is the decoded value and second a buffer for the
 *          remaining data (size will be 0 for no more data)
 * @throws std::invalid_argument if buf[0] does not encode a leb128 value with
 *         a stop byte.
 */
template <class T>
std::pair<T, cb::const_byte_buffer>
unsigned_leb128<T, typename std::enable_if<std::is_unsigned<T>::value>::type>::
        decode(cb::const_byte_buffer buf) {
    if (buf.size() > 0) {
        auto rv = unsigned_leb128<T>::decode(buf, NoThrow{});
        if (rv.second.data()) {
            return rv;
        }
    }
    throw std::invalid_argument(
            "`unsigned_leb128::decode invalid leb128 of size:" +
            std::to_string(buf.size()));
}

template <class T>
std::pair<T, cb::const_byte_buffer>
unsigned_leb128<T, typename std::enable_if<std::is_unsigned<T>::value>::type>::
        decodeCanonical(cb::const_byte_buffer buf) {
    auto rv = unsigned_leb128<T>::decode(buf, NoThrow{});

    if (rv.second.data() &&
        !is_canonical(rv.first, size_t(rv.second.data() - buf.data()))) {
        return {0, cb::const_byte_buffer{}};
    }

    return rv;
}

template <class T>
std::pair<T, cb::const_byte_buffer>
unsigned_leb128<T, typename std::enable_if<std::is_unsigned<T>::value>::type>::
        decodeNoThrow(cb::const_byte_buffer buf) {
    return unsigned_leb128<T>::decode(buf, NoThrow{});
}

template <class T>
std::pair<T, cb::const_byte_buffer>
unsigned_leb128<T, typename std::enable_if<std::is_unsigned<T>::value>::type>::
        decodeCanonical_chunk(cb::const_byte_buffer buf) {
    auto rv = unsigned_leb128<T>::decode_chunk(buf, NoThrow{});

    if (rv.second.data() &&
        !is_canonical(rv.first, size_t(rv.second.data() - buf.data()))) {
        return {0, cb::const_byte_buffer{}};
    }

    return rv;
}

/**
 * @return a buffer to the data after the leb128 prefix
 */
template <class T>
typename std::enable_if<std::is_unsigned<T>::value, cb::const_byte_buffer>::type
skip_unsigned_leb128(cb::const_byte_buffer buf) {
    return unsigned_leb128<T>::decode(buf).second;
}

/// @return the index of the stop byte within buf
static inline std::optional<size_t> unsigned_leb128_get_stop_byte_index(
        cb::const_byte_buffer buf) {
    // If buf does not contain a stop-byte, invalid
    size_t stopByte = 0;
    for (auto c : buf) {
        if ((c & 0x80ull) == 0) {
            return stopByte;
        }
        stopByte++;
    }
    return {};
}

} // namespace mcbp
} // namespace cb
