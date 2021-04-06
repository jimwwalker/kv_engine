#pragma once

#include "collections/collections_types.h"
#include "collections/history_id.h"
#include "collections/manifest_generated.h"

class StatCollector;

namespace Collections {

/**
 * Manifest Global Identifier
 *
 * The combination of HistoryID plus manifest revision identify manifests.
 */
class ManifestGID {
public:
    // @todo: look to remove this, it should not be needed once all paths
    // pass around historyID
    ManifestGID() = default;

    /**
     * Construct
     */
    ManifestGID(ManifestUid revision, HistoryID historyId);

    /**
     * Construct from serialised, flatbuffer format
     */
    ManifestGID(FlatbufferManifestGID);

    /**
     * Construct from 'basic' types
     */
    ManifestGID(uint64_t, std::string_view);

    /**
     * Assign, revision cannot go backwards
     */
    ManifestGID& operator=(const ManifestGID&);

    /**
     * Special assign that allows for the revision to go backwards
     */
    void reset(const ManifestGID&);

    ManifestUid getRevision() const {
        return revision;
    }

    HistoryID getHistoryID() const {
        return historyID;
    }

    bool operator==(const ManifestGID&) const;
    bool operator!=(const ManifestGID&) const;

    void addStats(Vbid vbid, const StatCollector& collector) const;

private:
    /**
     * Weakly monotonic revision of the manifest
     */
    ManifestUid revision{0};

    /**
     * HistoryID associated with the manifest
     */
    HistoryID historyID;
};

std::ostream& operator<<(std::ostream&, const ManifestGID&);

} // namespace Collections
