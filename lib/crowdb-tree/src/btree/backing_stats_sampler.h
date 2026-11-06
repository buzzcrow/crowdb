// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/btree/diagnostics.h"

#include <memory>

namespace crowdb::tree
{
class TreeResourceMetrics;

// A sampling handle never retains pages, a backend, or an executor. Scope IDs
// distinguish independent ledgers; multiple handles of one scope are not summed.
struct BackingStatsSampler
{
    uint64_t                             scope_id = 0;
    std::shared_ptr<TreeResourceMetrics> metrics;
    [[nodiscard]] BackingStatsSnapshot   snapshot() const noexcept;
};
} // namespace crowdb::tree
