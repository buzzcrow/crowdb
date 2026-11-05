// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"

namespace crowdb::tree
{
Status Crowdbtree::install_snapshot_native(std::vector<NativeFrame> frames, uint64_t root, uint64_t slot, uint64_t next)
{
    const auto count = frames.size();
    return core_->install_native(std::move(frames),
                                 {.root_page_id = root, .at_slot = slot, .next_page_id = next, .frame_count = count});
}

Status Crowdbtree::clear()
{
    return core_->clear();
}
} // namespace crowdb::tree
