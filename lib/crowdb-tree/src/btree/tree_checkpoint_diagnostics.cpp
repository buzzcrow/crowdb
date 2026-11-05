// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_internal.h"

namespace crowdb::tree
{
uint64_t TreeCore::last_snapshot_pages_written() const noexcept
{
    return state_->checkpoint_diagnostics.written_frames.load(std::memory_order_acquire);
}

uint64_t TreeCore::snapshot_pages_total() const noexcept
{
    return state_->checkpoint_diagnostics.total_written_frames.load(std::memory_order_acquire);
}

uint64_t Crowdbtree::last_snapshot_pages_written() const
{
    return core_->last_snapshot_pages_written();
}
} // namespace crowdb::tree
