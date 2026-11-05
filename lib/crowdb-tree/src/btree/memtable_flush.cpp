// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"

namespace crowdb::tree
{
Status Crowdbtree::flush()
{
    return core_->flush();
}

void Crowdbtree::flush_async(std::function<void(Status)> on_done)
{
    core_->flush_async(std::move(on_done));
}
} // namespace crowdb::tree
