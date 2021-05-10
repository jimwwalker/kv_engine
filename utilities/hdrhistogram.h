/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <nlohmann/json_fwd.hpp>
#include <chrono>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>

#include "iterator_range.h"

// hdr_histogram.h emits a warning on windows
#ifdef WIN32
#pragma warning(push, 0)
#endif

#include <hdr_histogram.h>

#ifdef WIN32
#pragma warning(pop)
#endif

template <typename HISTOGRAM, uint64_t THRESHOLD_MS>
class GenericBlockTimer;
template <typename T>
class MicrosecondStopwatch;

/**
 * A container for the c hdr_histogram data structure.
 *
 */
class HdrHistogram {
    // Custom deleter for the hdr_histogram struct.
    struct HdrDeleter {
        void operator()(struct hdr_histogram* val);
    };

    using SyncHdrHistogramPtr = folly::Synchronized<
            std::unique_ptr<struct hdr_histogram, HdrDeleter>>;
    using ConstRHistoLockedPtr = SyncHdrHistogramPtr::ConstRLockedPtr;
    using WHistoLockedPtr = SyncHdrHistogramPtr::WLockedPtr;

public:
    /**
     * Class representing a single histogram bucket.
     *
     * This is the type returned when dereferencing an Iterator.
     */
    struct Bucket {
        uint64_t lower_bound = 0;
        uint64_t upper_bound = 0;
        uint64_t count = 0;
        std::optional<double> percentile = {};

        bool operator==(const Bucket& other) const {
            return lower_bound == other.lower_bound &&
                   upper_bound == other.upper_bound && count == other.count &&
                   percentile == other.percentile;
        }
    };
    struct Iterator : public hdr_iter {
        using iterator_category = std::input_iterator_tag;
        using value_type = Bucket;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;
        // tag type, used to test if `itr == end()`. Histogram iterators do not
        // attempt to meet LegacyBidirectionalIterator so do not need to support
        // `--end()`.
        struct EndSentinel {};
        /**
         * Enum to state the method of iteration the iterator is using
         */
        enum class IterMode {
            /**
             * Log enum is a type of iterator that will move
             * though values in logarithmically increasing steps
             */
            Log,
            /**
             * Linear enum is a type of iterator that will move
             * though values in consistent step widths
             */
            Linear,
            /**
             * Recorded enum is a type of iterator that will move
             * though all values by the finest granularity
             * supported by the underlying data structure
             */
            Recorded,
            /**
             * Percentiles enum is a type of iterator that will move
             * though values by percentile levels step widths
             */
            Percentiles
        };

        Iterator(const SyncHdrHistogramPtr& SyncHistoPtr,
                 Iterator::IterMode mode)
            : hdr_iter(), type(mode), histoRLockPtr(SyncHistoPtr.rlock()) {
        }

        Iterator(Iterator&& itr) = default;

        /**
         * Gets the next value and corresponding count from the histogram
         * Returns an optional pair, comprising of:
         * 1) value
         * 2) count associated with the value
         * The pair is optional because iterating past the last value in the
         * histogram will return no result.
         */
        std::optional<std::pair<uint64_t, uint64_t>> getNextValueAndCount();

        /**
         * Gets the next value and corresponding percentile from the histogram
         * Returns an optional pair, comprising of:
         * 1) highest equivalent value
         * 2) next percentile that the iterator moves to
         * The pair is optional because iterating past the last value in the
         * histogram will return no result.
         */
        std::optional<std::pair<uint64_t, double>> getNextValueAndPercentile();

        /**
         * Method used to get buckets from the histogram with the widths defined
         * by the iteration method being used by the iterator. The starting and
         * end values of the bucket is returned as a string in the format
         * low,high e.g. 10,20. The count of this bucket is returned as uint64_t
         * value.
         * @return the bucket data, first part of the pair containing a string
         * of the low and high values of the bucket. The second part of the pair
         * containing the count as a uint64_t for the bucket.
         */
        std::optional<std::tuple<uint64_t, uint64_t, uint64_t>>
        getNextBucketLowHighAndCount();

        /**
         * Dumps the histograms count data to a string
         * @return a string containing the histogram dump
         */
        std::string dumpValues();

        Iterator& operator++();

        const Bucket& operator*() const {
            return bucket;
        }

        const Bucket* operator->() const {
            return &bucket;
        }

        bool operator==(const Iterator& other) const;
        bool operator==(const EndSentinel&) const;

        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }
        bool operator!=(const EndSentinel& other) const {
            return !(*this == other);
        }

        IterMode type;
        uint64_t lastVal = 0;
        uint64_t lastCumulativeCount = 0;

    private:
        // allow HdrHistogram to access histoRLockPtr
        friend class HdrHistogram;
        /**
         * Read lock that's held for the life time of the iterator. To prevent
         * it being resized or reset while we're reading from the underlying
         * data structure.
         */
        ConstRHistoLockedPtr histoRLockPtr;

        // container for the iterated values, accessible through
        // *itr or itr->foo.
        Bucket bucket;

        // the underlying C iterator is exhausted when hdr_iter_next returns
        // a falsy value, this state needs to be tracked here to support
        // `itr == end()` checks.
        bool finished = false;
    };

    /**
     * Constructor for the histogram.
     * @param lowestDiscernibleValue  This is the smallest increment between
     *        distinct values.
     *        e.g., if values are recorded in nanoseconds but only microsecond
     *        level accuracy is needed, this can be set to 1000.
     * @param highestTrackableValue  the largest values that can be held in
     *        the histogram
     * @param sigificantFigures  the level of precision for the histogram.
     *        Note the underlying hdr_histogram requires the value to be
     *        between 1 and 5 (inclusive).
     * @param iterMode sets the default iteration mode that should be used
     *        when iterate though this histogram's data.
     */
    HdrHistogram(uint64_t lowestDiscernibleValue,
                 int64_t highestTrackableValue,
                 int significantFigures,
                 Iterator::IterMode iterMode = Iterator::IterMode::Recorded);

    HdrHistogram() : HdrHistogram(1, 1, 1){};

    /**
     * Copy constructor to define how to do a deep copy of a HdrHistogram
     * @param other other HdrHistogram to copy
     */
    HdrHistogram(const HdrHistogram& other)
        : HdrHistogram(other.getMinDiscernibleValue(),
                       other.getMaxTrackableValue(),
                       other.getSigFigAccuracy()) {
        // take advantage of the code already written in for the addition
        // assigment operator
        *this += other;
    };

    /**
     * Assignment operator to perform a deep copy of one histogram to another
     * @param other HdrHistogram to copy
     * @return returns this HdrHistogram, which is now a copy of the other
     * HdrHistogram
     */
    HdrHistogram& operator=(const HdrHistogram& other);

    /**
     * Addition assigment operator for aggregation of histograms
     * across buckets
     * @param other histogram to add to this one
     * @return returns this histogram with the addition of the values from
     * the other histogram
     */
    HdrHistogram& operator+=(const HdrHistogram& other);

    /**
     * Adds a value to the histogram.
     * @param v value to be added to the histogram and account for by 1 count
     * @return true if it successfully added that value to the histogram
     */
    bool addValue(uint64_t v);

    /**
     * Adds a value and associated count to the histogram.
     * @param v value to be added to the histogram
     * @param count number of counts that should be added to the histogram
     * for this value v.
     * @return true if it successfully added that value to the histogram
     */
    bool addValueAndCount(uint64_t v, uint64_t count);

    /**
     * Returns the number of values added to the histogram.
     */
    uint64_t getValueCount() const;

    /**
     * Returns the min value stored to the histogram
     */
    uint64_t getMinValue() const;

    /**
     * Returns the max value stored to the histogram
     */
    uint64_t getMaxValue() const;

    /**
     * Clears the histogram. Please not that this takes a write lock on the
     * underlying data structure and will wait till all read locks have been
     * released before completing and thus, could result in dead lock
     * situations.
     */
    void reset();

    /**
     * Returns the value held in the histogram at the percentile defined by
     * the input parameter percentage.
     */
    uint64_t getValueAtPercentile(double percentage) const;

    /**
     * Returns a iterator range iterating over linear buckets from the
     * histogram.
     *
     * Convenient to use with a range-based for loop:
     *
     *  for (const auto& bucket: histogram.linearView()) {...}
     *
     * @param valueUnitsPerBucket  the number of values to be grouped into
     *        a single bucket
     */
    auto linearView(int64_t valueUnitsPerBucket) const {
        // make*Iterator returns an iterator pointing to the first bucket
        // for a given iteration mode (here, linear).
        // Comparing the iterator to end() sentinel checks if the underlying
        // (hdr_histogram C code) iterator has reached the end of the recorded
        // values.
        return cb::move_only_iterator_range(
                makeLinearIterator(valueUnitsPerBucket), end());
    }

    /**
     * Returns a iterator range exposing log buckets from the histogram
     * @param firstBucketWidth  the number of values to be grouped into
     *        the first bucket bucket
     * @param log_base base of the logarithm of iterator
     */
    auto logView(int64_t firstBucketWidth, double log_base) const {
        return cb::move_only_iterator_range(
                makeLogIterator(firstBucketWidth, log_base), end());
    }

    /**
     * Returns a iterator range exposing percentile buckets from the histogram
     * @param ticksPerHalfDist The number iteration steps per
     * half-distance to 100%.
     * @return iterator range that moves over the histogram as percentiles
     */
    auto percentileView(uint32_t ticksPerHalfDist) const {
        return cb::move_only_iterator_range(
                makePercentileIterator(ticksPerHalfDist), end());
    }

    /**
     * Returns a iterator range exposing raw recorded buckets from the histogram
     * @return iterator range that moves over the histogram by every value in
     * its recordable range.
     */
    auto recordedView() const {
        return cb::move_only_iterator_range(makeRecordedIterator(), end());
    }

    /**
     * Method to get an iterator range, iterating with the default mode set at
     * histogram construction.
     *
     * @return an iterator range that can be used to iterate over
     * data in this histogram
     */
    auto defaultView(int64_t valueUnitsPerBucket) const {
        return cb::move_only_iterator_range(begin(), end());
    }

    /**
     * Get an Iterator which will traverse this histogram with the mode set in
     * `defaultIterationMode`.
     *
     * @return a HdrHistogram::Iterator that can be used to iterate over
     * data in this histogram
     */
    Iterator begin() const;

    /**
     * Returns a sentinel value that HdrHistogram::Iterator instances may
     * be compared to.
     *
     * When iter == end(), the underlying C style iterator has finished;
     * all recorded values have been seen.
     */
    Iterator::EndSentinel end() const {
        return {};
    }

    /**
     * prints the histogram counts by percentiles to stdout
     */
    void printPercentiles() const;

    /**
     * dumps the histogram to stdout using a logarithmic iterator
     * @param firstBucketWidth range of the first bucket
     * @param log_base base of the logarithmic iterator
     */
    void dumpLogValues(int64_t firstBucketWidth, double log_base) const;

    /**
     * dumps the histogram to stdout using a linear iterator
     * @param bucketWidth size of each bucket in terms of range
     */
    void dumpLinearValues(int64_t bucketWidth) const;

    /**
     * Method to get the histogram as a json object
     * @return a nlohmann::json containing the histograms data iterated
     * over by Percentiles
     */
    nlohmann::json to_json() const;

    /**
     * Dumps the histogram data to json in a string form
     * @return a string of histogram json data
     */
    std::string to_string() const;

    /**
     * Method to get the total amount of memory being used by this histogram
     * @return number of bytes being used by this histogram
     */
    size_t getMemFootPrint() const;

    /**
     * Get the lowest non-zero value this histogram can represent.
     */
    uint64_t getMinDiscernibleValue() const {
        return getMinDiscernibleValue(histogram.rlock());
    }

    /**
     * Method to get the maximum trackable value of this histogram
     * @return maximum trackable value
     */
    int64_t getMaxTrackableValue() const {
        return getMaxTrackableValue(histogram.rlock());
    }

    /**
     * Method to get the number of significant figures being used to value
     * resolution and resolution.
     * @return an int between 0 and 5 of the number of significant
     * figures bing used
     */
    int getSigFigAccuracy() const {
        return getSigFigAccuracy(histogram.rlock());
    }

    /**
     * Method to get hold of the mean of this histogram.
     * @return returned the mean of values added to the histogram as a double.
     */
    double getMean() const;

private:
    void resize(WHistoLockedPtr& histoLockPtr,
                uint64_t lowestDiscernibleValue,
                int64_t highestTrackableValue,
                int significantFigures);

    /**
     * Get the lowest non-zero value this histogram can represent.
     */
    template <class LockType>
    static uint64_t getMinDiscernibleValue(const LockType& histoLockPtr) {
        // NOTE: the current name used by hdr histogram, lowest_trackable_value
        // is misleading, using "discernible" to reduce confusion and to be
        // consistent with the _Java_ documentation,
        return static_cast<uint64_t>(
                histoLockPtr->get()->lowest_trackable_value);
    }

    template <class LockType>
    static int64_t getMaxTrackableValue(const LockType& histoLockPtr) {
        return histoLockPtr->get()->highest_trackable_value;
    }

    template <class LockType>
    static int getSigFigAccuracy(const LockType& histoLockPtr) {
        return histoLockPtr->get()->significant_figures;
    }

    /**
     * Private method used to create an iterator for the specified mode
     * @param mode the mode of the iterator to be created
     * @return an iterator of the specified mode
     */
    Iterator makeIterator(Iterator::IterMode mode) const;

    /**
     * Returns a linear iterator for the histogram
     * @param valueUnitsPerBucket  the number of values to be grouped into
     *        a single bucket
     */
    Iterator makeLinearIterator(int64_t valueUnitsPerBucket) const;

    /**
     * Returns a log iterator for the histogram
     * @param firstBucketWidth  the number of values to be grouped into
     *        the first bucket bucket
     * @param log_base base of the logarithm of iterator
     */
    Iterator makeLogIterator(int64_t firstBucketWidth, double log_base) const;

    /**
     * Returns a percentile iterator for the histogram
     * @param ticksPerHalfDist The number iteration steps per
     * half-distance to 100%.
     * @return iterator that moves over the histogram as percentiles
     */
    Iterator makePercentileIterator(uint32_t ticksPerHalfDist) const;

    /**
     * Returns a recorded iterator fpr the histogram
     * @return iterator that moves over the histogram by every value in its
     * recordable range.
     */
    Iterator makeRecordedIterator() const;

    /**
     * Method to get the default iterator used to iterate over data for
     * this histogram
     * @return a HdrHistogram::Iterator that can be used to iterate over
     * data in this histogram
     */
    Iterator getHistogramsIterator() const;

    /**
     * Variable used to store the default iteration mode of a given histogram.
     */
    Iterator::IterMode defaultIterationMode;

    /**
     * Synchronized unique pointer to a struct hdr_histogram
     */
    SyncHdrHistogramPtr histogram;
};

/** Histogram to store counts for microsecond intervals
 *  Can hold a range of 0us to 60000000us (60 seconds) with a
 *  precision of 1 significant figures
 */
class Hdr1sfMicroSecHistogram : public HdrHistogram {
public:
    Hdr1sfMicroSecHistogram()
        : HdrHistogram(1, 60000000, 1, Iterator::IterMode::Percentiles){};
    bool add(std::chrono::microseconds v, size_t count = 1) {
        return addValueAndCount(static_cast<uint64_t>(v.count()),
                                static_cast<uint64_t>(count));
    }
};

/** Histogram to store counts for microsecond intervals
 *  Can hold a range of 0us to 60000000us (60 seconds) with a
 *  precision of 2 significant figures
 */
class Hdr2sfMicroSecHistogram : public HdrHistogram {
public:
    Hdr2sfMicroSecHistogram()
        : HdrHistogram(1, 60000000, 2, Iterator::IterMode::Percentiles){};
    bool add(std::chrono::microseconds v, size_t count = 1) {
        return addValueAndCount(static_cast<uint64_t>(v.count()),
                                static_cast<uint64_t>(count));
    }
};

using HdrMicroSecBlockTimer = GenericBlockTimer<Hdr1sfMicroSecHistogram, 0>;
using HdrMicroSecStopwatch = MicrosecondStopwatch<Hdr1sfMicroSecHistogram>;

/**
 * Histogram to store counts for values between 0 and 2^32 − 1
 * with a precision of 1 significant figures
 */
class Hdr1sfInt32Histogram : public HdrHistogram {
public:
    Hdr1sfInt32Histogram()
        : HdrHistogram(1,
                       std::numeric_limits<int32_t>::max(),
                       1,
                       Iterator::IterMode::Percentiles){};
    bool add(size_t v, size_t count = 1) {
        return addValueAndCount(v, count);
    }
};

/**
 * Histogram to store values between 0 and 255
 * with a precision of 3 significant figures
 */
class HdrUint8Histogram : public HdrHistogram {
public:
    HdrUint8Histogram()
        : HdrHistogram(1,
                       std::numeric_limits<uint8_t>::max(),
                       3,
                       Iterator::IterMode::Linear){};
    bool add(size_t v, size_t count = 1) {
        return addValueAndCount(v, count);
    }
};
