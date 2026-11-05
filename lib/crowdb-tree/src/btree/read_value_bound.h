// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/published_state.h"

#include <algorithm>

namespace crowdb::tree
{
// Inline overlay cells lend their existing owners, even for large values or
// unknown live-key counts. Only encoded overflow cells need an assembly window.
// Bases keep that window unless their exact stored dependencies prove that
// they contain only inline cells, even when their decoded frames are absent.
inline size_t overflow_value_bytes_bound(const PublishedTreeState &view, size_t maximum) noexcept
{
    if (!view.mapping.copy_bounds().may_have_overflow_values) {
        return 0;
    }
    return view.stats.kv_unknown_pages == 0 ? std::min<uint64_t>(maximum, view.stats.kv_known_bytes) : maximum;
}
} // namespace crowdb::tree
