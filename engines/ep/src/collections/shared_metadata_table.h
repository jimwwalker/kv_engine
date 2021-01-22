#pragma once

#include "collections/collections_types.h"

#include <forward_list>
#include <iostream>
#include <string>
#include <unordered_map>

namespace Collections {

/**
 * Class for mapping a Key to a name (std::string). The names are stored using
 * shared pointers so that duplicate names can be shared between users.
 *
 * The class allows for keys to map to multiple values.
 *
 * The class is fairly generic, but is currently collection specific as in the
 * value of the mapping is a std::string, further work could generalise the
 * value allowing either more collection meta-data to be shared or making
 * this object more useful for others. That work is deliberately in the future.
 */
template <class Key, class Value, class ValueView>
class SharedMetaDataTable {
public:
    /**
     * Function returns a SharedName which references the given name. This may
     * reference an existing name or insert it as a new name in the map.
     */
    const Value& createOrReference(Key id, const ValueView& valueView);
    void dereference(Key id, const ValueView& valueView);
    size_t count(Key id) const {
        return smt.count(id);
    }

private:
    template <class K, class V, class VView>
    friend std::ostream& operator<<(
            std::ostream& os, const SharedMetaDataTable<K, V, VView>& table);
    struct MapValue {
        uint32_t refs{0};
        std::unique_ptr<Value> value;
    };
    std::unordered_multimap<Key, MapValue> smt;
};

template <class Key, class Value, class ValueView>
const Value& SharedMetaDataTable<Key, Value, ValueView>::createOrReference(
        Key id, const ValueView& valueView) {
    for (auto [itr, end] = smt.equal_range(id); itr != end; itr++) {
        if (*itr->second.value == valueView) {
            itr->second.refs++;
            return *itr->second.value;
        }
    }

    // Nothing found or (key not mapped or value not mapped)
    auto itr = smt.emplace(id, MapValue{1, std::make_unique<Value>(valueView)});
    return *itr->second.value;
}

template <class Key, class Value, class ValueView>
void SharedMetaDataTable<Key, Value, ValueView>::dereference(
        Key id, const ValueView& valueView) {
    for (auto [itr, end] = smt.equal_range(id); itr != end; itr++) {
        if (*itr->second.value == valueView) {
            itr->second.refs--;
            if (itr->second.refs == 0) {
                smt.erase(itr);
            }
            return;
        }
    }
    throw std::invalid_argument(
            "SharedMetaDataTable<Key>::dereference nothing found for id:" +
            id.to_string()); // + " =>" + std::string(name));
}

template <class Key, class Value, class ValueView>
std::ostream& operator<<(
        std::ostream& os,
        const SharedMetaDataTable<Key, Value, ValueView>& table) {
    os << "SharedMetaDataTable: size:" << table.smt.size() << std::endl;
    for (const auto& [key, value] : table.smt) {
        os << "  id:" << key.to_string() << ", value:" << *value.value
           << ", refs:" << value.refs << std::endl;
    }
    return os;
}

} // namespace Collections