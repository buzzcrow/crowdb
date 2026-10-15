// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "disk/disk_set.h"
#include "disk/retired_disks.h"

#include <folly/dynamic.h>

#include <unordered_map>
#include <vector>

namespace crowdb::diskio
{
// Single sync-writer state; request readers only access DiskSet.
class DiskExclusion
{
  public:
    void           observe(const folly::dynamic &entries, uint64_t configured_group);
    folly::dynamic pending(const DiskSet &active, const RetiredDisks &retired);
    void           confirm(const folly::dynamic &batch);

  private:
    struct Entry
    {
        uint64_t group;
        uint64_t revision;
        bool     confirmed;
    };

    std::unordered_map<DiskId, Entry, DiskIdHash> entries_;
    std::vector<DiskId>                           order_;
    size_t                                        cursor_ = 0;
};
} // namespace crowdb::diskio
