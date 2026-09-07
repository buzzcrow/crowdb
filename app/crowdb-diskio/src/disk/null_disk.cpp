// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/null_disk.h"

#include "engine/dummy/dummy_engine.h"

#include <fcntl.h>
#include <unistd.h>

namespace crowdb::diskio
{

namespace
{
// /dev/zero accepts reads and writes at arbitrary offsets while retaining no
// data. I/O still traverses the uring/blocking engine without growing tmpfs.
int open_discard_fd()
{
    return ::open("/dev/zero", O_RDWR | O_CLOEXEC);
}
} // namespace

NullDisk::NullDisk(DiskId id, std::shared_ptr<IoEngine> engine, std::vector<Zone> zones,
                   std::optional<DiskProperties> props)
    : id_(id),
      fd_(open_discard_fd())
{
    // Wrap the shared engine with read-content hack + optional fault injection.
    wrapper_ = std::make_shared<DummyDiskEngine>(std::move(engine), true, props);
    zones_   = std::move(zones);
}

NullDisk::~NullDisk()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Zone *NullDisk::find_zone(uint32_t zone_index)
{
    for (auto &z : zones_) {
        if (z.zone_index == zone_index) {
            return &z;
        }
    }
    return nullptr;
}

} // namespace crowdb::diskio
