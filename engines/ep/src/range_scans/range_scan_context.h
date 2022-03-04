/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <memcached/dockey.h>

#include <folly/Synchronized.h>

#include <deque>
#include <memory>

class BackfillManager;
class Item;
class RangeScanResult;
class RangeScanResultKey;
class RangeScanResultValue;

class RangeScanContext {
public:
    RangeScanContext(BackfillManager& bfManager);

    /**
     * @return if the scan is configured for key-only (false means key+value)
     */
    bool isKeyOnly() const {
        return false;
    }

    /**
     * Store the item into the RangeScanContext if the manager says space is
     * available.
     *
     * @param the Item to store
     * @return true if stored, false if not
     */
    bool store(std::unique_ptr<Item> item);

    bool store(DocKey key);

    void storeEndSentinel();

    /**
     * @return the size of the queue (how many items loaded from the scan)
     */
    size_t getSize() const {
        return queue.rlock()->size();
    }
    using resultType = std::unique_ptr<RangeScanResult>;

    /**
     * @return the 'front' and reduce the queue size
     */
    resultType popFront();

private:
    BackfillManager& bfManager; // needed for updating bytes read

    using container = std::deque<resultType>;
    folly::Synchronized<container> queue;
    std::atomic<bool> scanComplete{false};
};