// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "maintenance_publication_internal.h"

#include <algorithm>

namespace crowdb::tree::detail
{
MaintenanceLeafMerge plan_published_leaf_merge(const PublishedPage &left, const PublishedPage &right,
                                               const LeafMergeInput &input, const PageMaintenanceLimits &limits,
                                               PageVersion version, uint64_t first_run_id)
{
    MaintenanceLeafMerge result;
    const auto          &a = left.metrics;
    const auto          &b = right.metrics;
    if (left.leaf != input.left || right.leaf != input.right || limits.merge_target_bytes >= limits.target_page_bytes ||
        limits.target_page_bytes > limits.primary_page_bytes) {
        result.logical.status = Status::invalid_argument("merge metrics must belong to the exact bounded source pages");
        return result;
    }
    if (a.page_size_known && a.kv_known && b.page_size_known && b.kv_known) {
        constexpr size_t overhead = kFrameHeaderSize + kFrameTrailerSize;
        if (a.page_size_bytes < overhead || b.page_size_bytes < overhead) {
            result.logical.status = Status::corruption("exact leaf size is smaller than its frame overhead");
            return result;
        }
        if (std::min(a.page_size_bytes, b.page_size_bytes) >= limits.merge_trigger_bytes ||
            a.page_size_bytes > limits.merge_target_bytes ||
            b.page_size_bytes - overhead > limits.merge_target_bytes - a.page_size_bytes) {
            return result;
        }
        if (b.kv_count > UINT64_MAX - a.kv_count || b.kv_bytes > UINT64_MAX - a.kv_bytes) {
            result.logical.status = Status::resource_exhausted("merged leaf statistics overflow");
            return result;
        }
        result.logical       = join_leaf_ranges(input, limits, version);
        result.primary_bytes = a.page_size_bytes + b.page_size_bytes - overhead;
        result.statistics = LeafLayoutStats{.kv_count = a.kv_count + b.kv_count, .kv_bytes = a.kv_bytes + b.kv_bytes};
        return result;
    }
    result.logical = plan_leaf_merge(input, limits, version, first_run_id);
    if (!result.logical.status.ok() || !result.logical.page) {
        return result;
    }
    auto layout              = plan_leaf_layout(result.logical.page, limits);
    result.logical.status    = std::move(layout.status);
    result.logical.readiness = layout.readiness;
    result.logical.loads     = std::move(layout.loads);
    if (result.logical.status.ok() && layout.readiness == MaintenanceReadiness::kReady) {
        if (layout.primary_bytes.size() != 1) {
            result.logical.status = Status::corruption("accepted merge must have a complete single-page layout");
        }
        else {
            result.primary_bytes = layout.primary_bytes.front();
        }
    }
    return result;
}
} // namespace crowdb::tree::detail
