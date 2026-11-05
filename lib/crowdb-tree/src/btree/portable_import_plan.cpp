// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "portable_import_plan.h"

namespace crowdb::tree
{
Status PortableImportPlan::append(size_t key_bytes, size_t value_bytes, bool tombstone)
{
    if (config_.frame_bytes <= kFrameHeaderSize + kFrameTrailerSize || key_bytes > loads_.max_leaf_key_bytes ||
        value_bytes > loads_.max_value_bytes) {
        return Status::resource_exhausted("range input exceeds import geometry");
    }
    const auto cap          = config_.frame_bytes - kFrameHeaderSize - kFrameTrailerSize;
    const auto inline_limit = config_.max_inline_value != 0 ? config_.max_inline_value : config_.frame_bytes / 4;
    auto       inline_bytes =
        detail::AllocationBound(kLeafSlotSize + kCellHeaderSize).add(key_bytes).add(value_bytes).value();
    if (!inline_bytes) {
        return Status::resource_exhausted("range entry bound overflow");
    }
    const bool spill = !tombstone && (value_bytes > inline_limit || *inline_bytes > cap);
    const auto needed =
        spill ? detail::AllocationBound(kLeafSlotSize + kOverflowCellSize).add(key_bytes).value() : inline_bytes;
    if (!needed || *needed > cap) {
        return Status::resource_exhausted("range entry cannot fit an import primary");
    }
    if (leaf_count_ == 0 || leaf_count_ == loads_.max_leaf_entries ||
        key_bytes > loads_.max_leaf_key_bytes - leaf_keys_ || *needed > cap - leaf_used_) {
        if (shape_.leaf_pages == NativeImportLimits{}.max_frames) {
            return Status::resource_exhausted("range import leaf count limit");
        }
        ++shape_.leaf_pages;
        leaf_used_ = leaf_keys_ = leaf_count_ = 0;
    }
    leaf_used_ += *needed;
    leaf_keys_ += key_bytes;
    ++leaf_count_;
    shape_.max_key_bytes   = std::max(shape_.max_key_bytes, key_bytes);
    shape_.max_value_bytes = std::max(shape_.max_value_bytes, value_bytes);
    if (spill) {
        const auto capacity = overflow_chunk_cap(config_.frame_bytes);
        if (capacity == 0) {
            return Status::resource_exhausted("range import has no overflow capacity");
        }
        const auto frames = (value_bytes / capacity) + static_cast<size_t>(value_bytes % capacity != 0);
        const auto total  = detail::AllocationBound(shape_.dependency_steps).add(frames).value();
        if (!total || *total > NativeImportLimits{}.max_dependency_steps) {
            return Status::resource_exhausted("range overflow dependency limit");
        }
        shape_.dependency_steps = *total;
        ++shape_.stored_references;
    }
    return Status::Ok();
}

std::optional<NativeImportShape> PortableImportPlan::finish() const noexcept
{
    auto result = shape_;
    // Every parent consumes at least two children except a final unary tail.
    // Repeated ceil(n/2) bounds the builder without retaining separator keys.
    size_t level         = result.leaf_pages;
    result.primary_pages = level;
    while (level > 1) {
        level            = (level / 2) + (level % 2);
        const auto total = detail::AllocationBound(result.primary_pages).add(level).value();
        if (!total) {
            return {};
        }
        result.primary_pages = *total;
    }
    const auto total = detail::AllocationBound(result.primary_pages).add(result.dependency_steps).value();
    if (!total || *total > NativeImportLimits{}.max_frames) {
        return {};
    }
    result.frames = *total;
    return result;
}
} // namespace crowdb::tree
