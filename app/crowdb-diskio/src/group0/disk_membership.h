// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "disk/retired_disks.h"

#include <folly/dynamic.h>

#include <unordered_set>

namespace crowdb::diskio
{

// Administrative membership from a complete successful Group-0 observation.
// Health states other than Offline leave handles available for recovery reads.
struct DiskMembership
{
    std::unordered_set<DiskId, DiskIdHash> active;
    std::unordered_set<uint64_t>           groups;

    static DiskMembership parse(const folly::dynamic &entries, uint64_t configured_group);
    std::vector<DiskId>   exclude(DiskSet &disks, RetiredDisks &retired) const;
};

} // namespace crowdb::diskio
