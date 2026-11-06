// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backing_stats_sampler.h"

#include "tree_metrics.h"

namespace crowdb::tree
{
BackingStatsSnapshot BackingStatsSampler::snapshot() const noexcept
{
    if (!metrics || scope_id == 0) {
        return {};
    }
    const auto value = metrics->snapshot();
    return {.scope_id = scope_id, .allocated_bytes = value.allocated_backing_bytes};
}
} // namespace crowdb::tree
