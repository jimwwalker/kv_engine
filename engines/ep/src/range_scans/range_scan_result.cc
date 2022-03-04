#include "range_scans/range_scan_result.h"
#include <spdlog/fmt/fmt.h>

DocKey RangeScanResult::getKey() const {
    throw std::runtime_error(fmt::format(
            "RangeScanResult::getKey was called isEnd:{}", isEnd()));
}

DocKey RangeScanResultKey::getKey() const {
    return key;
}

DocKey RangeScanResultValue::getKey() const {
    return item->getKey();
}

bool RangeScanResultKey::compare(DocKey key) const {
    return this->key == key;
}

bool RangeScanResultValue::compare(DocKey key) const {
    return item->getKey() == key;
}

bool RangeScanResultValue::compare(std::string_view value) const {
    return item->getValueView() == value;
}