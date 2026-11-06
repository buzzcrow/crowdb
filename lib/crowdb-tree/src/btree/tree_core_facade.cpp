// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/local_backend_state.h"
#include "btree/tree_core.h"

#include <stdexcept>

namespace crowdb::tree
{
uint64_t Crowdbtree::last_applied_slot() const
{
    return core_->capture()->coverage;
}

uint64_t Crowdbtree::contiguous_slot() const
{
    return core_->contiguous_slot();
}

uint64_t Crowdbtree::version() const
{
    return core_->capture()->commit_seq;
}

uint64_t Crowdbtree::root_page_id() const
{
    return core_->capture()->root_page_id;
}

uint64_t Crowdbtree::leaf_count_atomic() const
{
    return core_->capture()->stats.leaf_pages;
}

uint64_t Crowdbtree::inner_count_atomic() const
{
    return core_->capture()->stats.inner_pages;
}

uint64_t Crowdbtree::durable_snapshot_seq() const
{
    return core_->snapshot_sequence();
}

uint64_t Crowdbtree::durable_snapshot_last_applied_slot() const
{
    return core_->durable_coverage();
}

BackingStatsSnapshot Crowdbtree::backing_stats() const noexcept
{
    return core_->backing_stats();
}

TreeStatsSnapshot Crowdbtree::tree_state_stats() const
{
    return core_->state_stats();
}
} // namespace crowdb::tree
