// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <cstdint>

namespace crowdb::tree
{
// Six budget partitions, CPU/backend queue permits, then host CPU/backend
// workers. Tree samples omit the last two shared resources. Queue permits
// include idle prepaid progress registrations, unlike runnable queue length.
struct ResourceUsage
{
    uint64_t limit       = 0;
    uint64_t used        = 0;
    uint64_t reserved    = 0;
    uint64_t allocated   = 0;
    uint64_t denied_tree = 0;
    uint64_t denied_host = 0;
    uint64_t waits       = 0;
    uint64_t wait_ns     = 0;
    uint64_t waiters     = 0;
};

using ResourceUsageSnapshot = std::array<ResourceUsage, 10>;
} // namespace crowdb::tree
