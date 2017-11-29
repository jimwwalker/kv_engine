/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include "config.h"

#include <memcached/dockey.h>
#include <memcached/engine_error.h>

#include <memory>

class Item;

class CacheLookup {
public:
    CacheLookup(const DocKey& k,
                int64_t s,
                uint16_t vb,
                const std::string& separator)
        : key(k), bySeqno(s), vbid(vb), separator(separator) {
    }

    ~CacheLookup() {}

    DocKey getKey() { return key; }

    int64_t getBySeqno() { return bySeqno; }

    uint16_t getVBucketId() { return vbid; }

    const std::string& getSeparator() const {
        return separator;
    }

private:
    DocKey key;
    int64_t bySeqno;
    uint16_t vbid;
    const std::string& separator;
};

/**
 * Value for callback for GET operations.
 */
class GetValue {
public:
    GetValue();

    explicit GetValue(std::unique_ptr<Item> v,
                      ENGINE_ERROR_CODE s = ENGINE_SUCCESS,
                      uint64_t i = -1,
                      bool incomplete = false,
                      uint8_t _nru = 0xff);

    /// Cannot copy GetValues (cannot copy underlying Item).
    GetValue(const GetValue&) = delete;
    GetValue& operator=(const GetValue&&) = delete;

    /// Can move GetValues
    GetValue(GetValue&& other);
    GetValue& operator=(GetValue&& other);

    ~GetValue();

    /**
     * Engine code describing what happened.
     */
    ENGINE_ERROR_CODE getStatus() const { return status; }

    /**
     * Set the status code
     */
    void setStatus(ENGINE_ERROR_CODE s) { status = s; }

    /**
     * Get the item's underlying ID (if applicable).
     */
    uint64_t getId() { return id; }

    /**
     * Set the item's underlying ID.
     */
    void setId(uint64_t newId) { id = newId; }

    bool isPartial() const { return partial; }

    void setPartial() { partial = true; }

    uint8_t getNRUValue() const { return nru; }

    std::unique_ptr<Item> item;

private:
    uint64_t id;
    ENGINE_ERROR_CODE status;
    bool partial;
    uint8_t nru;
};

/**
 * Interface for callbacks from storage APIs.
 */
template <typename... RV>
class Callback {
public:

    Callback() : myStatus(0) {}

    virtual ~Callback() {}

    /**
     * Method called on callback.
     */
    virtual void callback(RV&... value) = 0;

    virtual void setStatus(int status) {
        myStatus = status;
    }

    virtual int getStatus() {
        return myStatus;
    }

private:

    int myStatus;
};
