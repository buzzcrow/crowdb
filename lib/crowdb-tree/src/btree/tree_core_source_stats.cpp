// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_internal.h"

#include <algorithm>

namespace crowdb::tree
{
SourceMemoryStats TreeCore::source_stats() const
{
    const auto view = capture();
    return view && view->sources ? view->sources->source_stats() : SourceMemoryStats{};
}

std::array<ResourceUsage, 7> TreeCore::admission_resource_usage() const noexcept
{
    std::array<ResourceUsage, 7> result{};
    result[0]          = state_->lifetime->resource_usage();
    result[1]          = state_->apply_lifetime->resource_usage();
    result[2]          = state_->replay_lifetime->resource_usage();
    const auto sources = state_->sources->catalog();
    if (sources && sources->active_budget) {
        const auto usage = sources->active_budget->resource_usage();
        std::copy(usage.begin(), usage.end(), result.begin() + 3);
    }
    return result;
}
} // namespace crowdb::tree
