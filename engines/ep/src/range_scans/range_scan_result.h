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

#include "item.h"
#include "storeddockey.h"

#include <memcached/engine_error.h>

class RangeScanResult {
public:
    virtual ~RangeScanResult(){}

    virtual cb::engine_errc getStatus() const {
        return cb::engine_errc::success;
    }

    virtual bool isEnd() const {
        return false;
    }

    virtual DocKey getKey() const;

    /**
     * Intended for test code, compare the key of the result.
     * @return true if the result has a matching key
     */
    virtual bool compare(DocKey key) const {
        return false;
    }

    /**
     * Intended for test code, compare the value of the result (as a string)
     * @return true if the result has a matching value
     */
    virtual bool compare(std::string_view value) const {
        return false;
    }

    // @todo: add a "marshal" type interface that allows the caller to get out
    // whatever the RangeScanResult actually stores, i.e. writes to a send buf
    // the key or key+value or status
};

class RangeScanResultKey : public RangeScanResult {
public:
    RangeScanResultKey(DocKey key) : key(key) {

    }

    DocKey getKey() const override;

    bool compare(DocKey key) const override;

protected:
    StoredDocKey key;
};

class RangeScanResultValue : public RangeScanResult {
public:
    RangeScanResultValue(std::unique_ptr<Item> item) : item(std::move(item)) {
        Expects(this->item);
    }

    DocKey getKey() const override;

    bool compare(DocKey key) const override;

    bool compare(std::string_view value) const override;

protected:

    std::unique_ptr<Item> item;
};

class RangeScanResultEnd : public RangeScanResult {
public:
    RangeScanResultEnd(cb::engine_errc status) : status(status){}

    cb::engine_errc getStatus() const override {
        return status;
    }

    bool isEnd() const override {
        return true;
    }

protected:
    cb::engine_errc status;
};