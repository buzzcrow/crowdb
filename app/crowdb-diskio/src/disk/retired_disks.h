// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "disk/disk_set.h"

#include <memory>
#include <unordered_map>
#include <vector>

namespace crowdb::diskio
{

// Owned by the disk-map writer. Readers only use DiskSet. Keep detached
// disks until both old map snapshots and request callbacks release them.
class RetiredDisks
{
  public:
    void retire(DiskSet &active, DiskId id)
    {
        auto disk = active.find_disk(id);
        if (disk != nullptr) {
            disks_.try_emplace(id, std::move(disk));
            active.remove_disk(id);
        }
    }

    [[nodiscard]] bool contains(DiskId id) const
    {
        return disks_.contains(id);
    }

    // The caller unregisters descriptors before releasing these last handles.
    // No new reader can acquire a reference after detachment from DiskSet.
    std::vector<std::shared_ptr<Disk>> collect_quiescent()
    {
        std::vector<std::shared_ptr<Disk>> ready;
        for (auto it = disks_.begin(); it != disks_.end();) {
            if (it->second.use_count() == 1) {
                ready.push_back(std::move(it->second));
                it = disks_.erase(it);
            }
            else {
                ++it;
            }
        }
        return ready;
    }

  private:
    std::unordered_map<DiskId, std::shared_ptr<Disk>, DiskIdHash> disks_;
};

} // namespace crowdb::diskio
