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

#include "dcp/backfill.h"
#include "dcp/backfill_disk.h"
#include "kvstore/kvstore.h"

class KVBucket;

/**
 * Class implementing the DCPBackfill API to run a RangeScan
 */
class RangeScanTask : public DCPBackfill, public DCPBackfillDisk {
public:
    /**
     * Construct a RangeScan for the vbid in bucket for the range start/end.
     */
    RangeScanTask(Vbid vbid,
                  KVBucket& bucket,
                  const DocKey& start,
                  const DocKey& end);

    bool shouldCancel() const override;

protected:
    /**
     * Creates a scan context with the KV Store to read items in the range
     */
    backfill_status_t create() override;

    /**
     * Scan the disk (by calling KVStore apis) for the items in the backfill
     * snapshot for collection cid.
     */
    backfill_status_t scan() override;

    ByIdRange range;
};
