/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "callbacks.h"
#include "dcp/backfill_by_seqno.h"
#include "dcp/backfill_disk.h"

class KVBucket;
class ScanContext;
class VBucket;

/**
 * Concrete class that does backfill from the disk and informs the DCP stream
 * of the backfill progress.
 * This class calls asynchronous kvstore apis and manages a state machine to
 * read items in the sequential order from the disk and to call the DCP stream
 * for disk snapshot, backfill items and backfill completion.
 */
class DCPBackfillBySeqnoDisk : public DCPBackfillDisk,
                               public DCPBackfillBySeqno {
public:
    DCPBackfillBySeqnoDisk(KVBucket& bucket,
                           std::shared_ptr<ActiveStream> stream,
                           uint64_t startSeqno,
                           uint64_t endSeqno);

private:
    /**
     * Creates a scan context with the KV Store to read items in the sequential
     * order from the disk. Backfill snapshot range is decided here.
     */
    backfill_status_t create() override;

    /**
     * Scan the disk (by calling KVStore apis) for the items in the backfill
     * snapshot range created in the create scan context. This is an
     * asynchronous operation, KVStore calls the CacheCallback and DiskCallback
     * to populate the items read in the snapshot of scan.
     */
    backfill_status_t scan() override;

    /**
     * Handles the completion of the backfill.
     * Destroys the scan context, indicates the completion to the stream.
     *
     * @param cancelled indicates the if backfill finished fully or was
     *                  cancelled in between; for debug
     */
    backfill_status_t complete(bool cancelled) override;
};