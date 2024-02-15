/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_task.h"

namespace cb {
enum class engine_errc;
}
class CookieIface;

class DownloadSnapshotTask : public EpTask {
public:
    static std::shared_ptr<DownloadSnapshotTask> create(
            CookieIface& cookie,
            EventuallyPersistentEngine& ep,
            std::string_view manifest);

    virtual std::pair<cb::engine_errc, std::string> getResult() const = 0;

protected:
    DownloadSnapshotTask(EventuallyPersistentEngine& ep);
};
