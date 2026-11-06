// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_bound.h"
#include "btree/published_state.h"

#include <algorithm>

namespace crowdb::tree
{
// A range source retains its current frame while result cells can keep earlier
// frames alive. A point lookup needs only one physical frame per logical base.
inline std::optional<size_t> snapshot_leaf_window(const PublishedTreeState &view, size_t entries) noexcept
{
    const auto shape = view.mapping.copy_bounds();
    if (entries == 0) {
        return shape.max_leaf_bases;
    }
    const auto available =
        detail::AllocationBound().add_product(shape.leaf_pages, shape.max_leaf_bases).value().value_or(SIZE_MAX);
    const auto window = detail::AllocationBound(shape.max_leaf_bases).add(entries).value();
    return window ? std::optional<size_t>(std::min(*window, available)) : std::nullopt;
}
} // namespace crowdb::tree
