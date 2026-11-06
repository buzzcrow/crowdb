// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/checkpoint_backend.h"
#include "backend/local/local_backend_state.h"
#include "tree_core_internal.h"

#include <algorithm>
#include <limits>

namespace crowdb::tree
{
BackingStatsSampler TreeCore::backing_sampler() const noexcept
{
    if (state_->chunk) {
        return state_->chunk->backing_sampler();
    }
    if (state_->local) {
        return {.scope_id = state_->local->ledger->limits().backend_id, .metrics = state_->resources};
    }
    return {};
}

BackingStatsSnapshot TreeCore::backing_stats() const noexcept
{
    return backing_sampler().snapshot();
}

TreeStatsSnapshot TreeCore::state_stats() const
{
    auto       result = state_->publisher->stats(*state_->resources);
    const auto budget = state_->binding->account()->snapshot();
    // I/O credit counts operations, not bytes.
    result.resources.allocated_bytes = 0;
    result.resources.reserved_bytes  = 0;
    for (size_t index = 0; index < static_cast<size_t>(detail::BudgetClass::kIo); ++index) {
        result.resources.allocated_bytes += std::min<uint64_t>(
            budget.allocated[index], std::numeric_limits<uint64_t>::max() - result.resources.allocated_bytes);
        result.resources.reserved_bytes += std::min<uint64_t>(
            budget.reserved[index], std::numeric_limits<uint64_t>::max() - result.resources.reserved_bytes);
    }
    result.resources.payload_bytes  = budget.physical[static_cast<size_t>(AllocationKind::kPayload)];
    result.resources.frame_bytes    = budget.physical[static_cast<size_t>(AllocationKind::kFrame)];
    result.resources.metadata_bytes = budget.physical[static_cast<size_t>(AllocationKind::kMetadata)];
    result.resources.io_bytes       = budget.physical[static_cast<size_t>(AllocationKind::kIo)];
    result.resources.scratch_bytes  = budget.physical[static_cast<size_t>(AllocationKind::kScratch)];
    const auto backing              = backing_stats();
    if (backing.scope_id != 0) {
        result.resources.allocated_backing_bytes = backing.allocated_bytes;
    }
    return result;
}
} // namespace crowdb::tree
