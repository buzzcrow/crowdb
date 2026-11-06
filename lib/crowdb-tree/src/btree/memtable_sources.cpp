// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"

namespace crowdb::tree
{
size_t Crowdbtree::memtable_count() const
{
    return core_->memtable_count();
}

size_t Crowdbtree::frozen_table_count() const
{
    return core_->frozen_table_count();
}
} // namespace crowdb::tree
