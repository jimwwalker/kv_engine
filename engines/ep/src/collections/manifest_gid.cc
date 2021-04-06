#include "collections/manifest_gid.h"

#include <statistics/cbstat_collector.h>

namespace Collections {

ManifestGID::ManifestGID(ManifestUid revision, HistoryID historyID)
    : revision{revision}, historyID{historyID} {
}

ManifestGID::ManifestGID(FlatbufferManifestGID fbid)
    : revision(fbid.revision()), historyID(*fbid.historyId()) {
}

ManifestGID::ManifestGID(uint64_t revision, std::string_view historyID)
    : revision(revision), historyID(historyID) {
}

ManifestGID& ManifestGID::operator=(const ManifestGID& other) {
    revision = other.revision;
    historyID = other.historyID;
    return *this;
}

void ManifestGID::reset(const ManifestGID& other) {
    revision.reset(other.revision);
    historyID = other.historyID;
}

bool ManifestGID::operator==(const ManifestGID& other) const {
    // @todo: compare historyID
    return revision == other.revision;
}

bool ManifestGID::operator!=(const ManifestGID& other) const {
    return !(*this == other);
}

void ManifestGID::addStats(Vbid vbid, const StatCollector& collector) const {
    fmt::memory_buffer key;

    format_to(key, "vb_{}:manifest:uid", vbid.get());
    collector.addStat(std::string_view(key.data(), key.size()), getRevision());
    key.resize(0);

    format_to(key, "vb_{}:history:uid", vbid.get());
    collector.addStat(std::string_view(key.data(), key.size()),
                      getHistoryID().to_string());
}

std::ostream& operator<<(std::ostream& os, const ManifestGID& id) {
    os << "revision:" << std::hex << id.getRevision()
       << ", historyID:" << id.getHistoryID() << std::dec;
    return os;
}

} // namespace Collections