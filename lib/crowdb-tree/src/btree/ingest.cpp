// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"

namespace crowdb::tree
{
Status Crowdbtree::apply(uint64_t slot, const Batch &batch)
{
    return core_->apply(slot, batch);
}

Status Crowdbtree::apply_encoded(uint64_t slot, std::vector<encoded_op> ops)
{
    return core_->apply_encoded(slot, std::move(ops));
}

Status Crowdbtree::apply_external(uint64_t slot, std::vector<external_op> ops)
{
    return core_->apply_external(slot, std::move(ops));
}

void Crowdbtree::force_advance_slot(uint64_t slot)
{
    core_->force_advance_slot(slot);
}
} // namespace crowdb::tree
