// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_geometry.h"

#include "btree/page_maintenance.h"
#include "crowdb-protocol/frame/base.h"
#include "crowdb-tree/maptable/frame_page.h"

#include <algorithm>
#include <limits>

namespace crowdb::tree
{
namespace
{
constexpr size_t kOverhead = kFrameHeaderSize + kFrameTrailerSize;
}

size_t leaf_record_capacity(uint32_t frame_bytes) noexcept
{
    const size_t bytes = std::min<size_t>(frame_bytes, crowdb::protocol::kMaxFrameV2PayloadBytes);
    return bytes > kOverhead ? bytes - kOverhead : 0;
}

Status configure_leaf_geometry(const Config &config, PageMaintenanceLimits *pages)
{
    const size_t capacity = leaf_record_capacity(config.frame_bytes);
    if (pages == nullptr || capacity == 0 || config.leaf_split_bytes == 0 ||
        config.leaf_split_bytes > std::numeric_limits<size_t>::max() - kOverhead ||
        config.leaf_merge_bytes > std::numeric_limits<size_t>::max() - kOverhead) {
        return Status::invalid_argument("leaf record thresholds exceed bounded frame geometry");
    }
    const size_t split         = std::min(config.leaf_split_bytes, capacity);
    const size_t merge         = split - std::max<size_t>(1, split / 4);
    pages->primary_page_bytes  = capacity + kOverhead;
    pages->target_page_bytes   = split + kOverhead;
    pages->merge_target_bytes  = merge + kOverhead;
    pages->merge_trigger_bytes = std::min(config.leaf_merge_bytes, merge) + kOverhead;
    return Status::Ok();
}
} // namespace crowdb::tree
