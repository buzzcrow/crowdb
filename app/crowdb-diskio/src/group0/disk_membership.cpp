// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "group0/disk_membership.h"

#include "diskdb_generated.h"
#include "group0/disk_identity.h"

#include <stdexcept>

namespace crowdb::diskio
{

DiskMembership DiskMembership::parse(const folly::dynamic &entries, uint64_t configured_group)
{
    if (!entries.isArray()) {
        throw std::invalid_argument("disk membership is not an array");
    }
    DiskMembership                         result;
    std::unordered_set<DiskId, DiskIdHash> seen;
    for (const auto &entry : entries) {
        const auto &identity = entry.at("disk_id");
        auto        high     = decode_disk_id_word(identity.at("high"));
        auto        low      = decode_disk_id_word(identity.at("low"));
        if (!high || !low) {
            throw std::invalid_argument("disk identity contains an invalid 64-bit word");
        }
        DiskId id{.high = *high, .low = *low};
        if (!seen.insert(id).second) {
            throw std::invalid_argument("duplicate disk identity in membership");
        }
        const auto &status = entry.at("value").at("status");
        if (!status.isInt() || status.asInt() < crowdb::diskdb::proto::FBHwStatus_MIN ||
            status.asInt() > crowdb::diskdb::proto::FBHwStatus_MAX) {
            throw std::invalid_argument("disk status is absent or unsupported");
        }
        uint64_t group = configured_group;
        if (group == 0) {
            const auto &value = entry.at("disk_group_id");
            if (!value.isInt() || value.asInt() < 0) {
                throw std::invalid_argument("disk group identity is invalid");
            }
            group = static_cast<uint64_t>(value.asInt());
        }
        result.groups.insert(group);
        if (status.asInt() != crowdb::diskdb::proto::FBHwStatus_Offline) {
            result.active.insert(id);
        }
    }
    return result;
}

std::vector<DiskId> DiskMembership::exclude(DiskSet &disks, RetiredDisks &retired) const
{
    std::vector<DiskId> excluded;
    for (const auto &id : disks.disk_ids()) {
        if (!active.contains(id)) {
            retired.retire(disks, id);
            excluded.push_back(id);
        }
    }
    return excluded;
}

} // namespace crowdb::diskio
