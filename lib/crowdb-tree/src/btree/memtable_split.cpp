// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"

namespace crowdb::tree
{
Status Crowdbtree::install_split_memtable_overlay(Crowdbtree &source, uint64_t frontier)
{
    return core_->install_split_overlay(*source.core_, frontier);
}

Status Crowdbtree::clear_split_memtable_overlay(Crowdbtree &source)
{
    return core_->clear_split_overlay(*source.core_);
}

Status Crowdbtree::begin_split_memtable_view(uint64_t *generation, uint64_t *frontier)
{
    return core_->begin_split_view(generation, frontier);
}

Status Crowdbtree::release_split_memtable_view(uint64_t generation)
{
    return core_->release_split_view(generation);
}

Status Crowdbtree::publish_split_memtable_view(uint64_t generation, uint64_t frontier, Crowdbtree &destination,
                                               const KeyRange &range)
{
    return core_->publish_split_view(generation, frontier, *destination.core_, range);
}
} // namespace crowdb::tree
