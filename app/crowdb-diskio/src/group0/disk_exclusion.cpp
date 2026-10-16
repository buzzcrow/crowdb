// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "group0/disk_exclusion.h"

#include "diskdb_generated.h"
#include "group0/disk_identity.h"

#include <stdexcept>

namespace crowdb::diskio
{
void DiskExclusion::observe(const folly::dynamic &entries, uint64_t configured_group)
{
    std::unordered_map<DiskId, Entry, DiskIdHash> next;
    std::vector<DiskId>                           order;
    for (const auto &entry : entries) {
        if (entry.at("value").at("status").asInt() != crowdb::diskdb::proto::FBHwStatus_Offline) {
            continue;
        }
        const auto high     = decode_disk_id_word(entry.at("disk_id").at("high"));
        const auto low      = decode_disk_id_word(entry.at("disk_id").at("low"));
        const auto revision = decode_disk_id_word(entry.at("revision"));
        const auto group    = configured_group != 0 ? std::optional<uint64_t>(configured_group)
                                                    : decode_disk_id_word(entry.at("disk_group_id"));
        if (!high || !low || !revision || *revision == 0 || !group) {
            throw std::invalid_argument("disk exclusion requires exact identity and revision");
        }
        const DiskId id{.high = *high, .low = *low};
        const auto   previous  = entries_.find(id);
        const bool   confirmed = previous != entries_.end() && previous->second.revision == *revision &&
                                 previous->second.group == *group && previous->second.confirmed;
        if (!next.emplace(id, Entry{.group = *group, .revision = *revision, .confirmed = confirmed}).second) {
            throw std::invalid_argument("duplicate disk exclusion identity");
        }
        order.push_back(id);
    }
    entries_.swap(next);
    order_.swap(order);
}

folly::dynamic DiskExclusion::pending(const DiskSet &active, const RetiredDisks &retired)
{
    auto result = folly::dynamic::array();
    for (size_t scanned = 0; scanned < order_.size() && result.size() < 16; ++scanned) {
        cursor_ %= order_.size();
        const auto  id    = order_[cursor_++];
        const auto &entry = entries_.at(id);
        if (entry.confirmed || active.find_disk(id) != nullptr || retired.contains(id)) {
            continue;
        }
        result.push_back(folly::dynamic::object("disk_group_id", std::to_string(entry.group))("disk_id_high",
                                                                                              std::to_string(id.high))(
            "disk_id_low", std::to_string(id.low))("disk_revision", std::to_string(entry.revision)));
    }
    return result;
}

void DiskExclusion::confirm(const folly::dynamic &batch)
{
    for (const auto &entry : batch) {
        const auto high     = decode_disk_id_word(entry.at("disk_id_high"));
        const auto low      = decode_disk_id_word(entry.at("disk_id_low"));
        const auto revision = decode_disk_id_word(entry.at("disk_revision"));
        const auto group    = decode_disk_id_word(entry.at("disk_group_id"));
        if (!high || !low || !revision || !group) {
            continue;
        }
        const auto current = entries_.find(DiskId{.high = *high, .low = *low});
        if (current != entries_.end() && current->second.revision == *revision && current->second.group == *group) {
            current->second.confirmed = true;
        }
    }
}
} // namespace crowdb::diskio
