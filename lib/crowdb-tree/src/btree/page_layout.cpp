// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_maintenance.h"

#include <limits>

namespace crowdb::tree
{
namespace
{
constexpr size_t kPageOverhead = kFrameHeaderSize + kFrameTrailerSize;

Status entry_bytes(Slice key, const LogicalCell &cell, const PageMaintenanceLimits &limits, size_t *bytes,
                   bool *overflow)
{
    *overflow               = !cell.tombstone() && (cell.overflow() || cell.value.size() > limits.inline_value_bytes);
    const size_t cell_bytes = *overflow ? kOverflowCellSize : kCellHeaderSize + cell.value.size();
    const auto   available  = kMaxPhysicalPageBytes - kPageOverhead - kLeafSlotSize;
    if (key.size() > available || cell_bytes > available - key.size()) {
        return Status::invalid_argument("leaf record exceeds the physical page safety bound");
    }
    *bytes = key.size() + cell_bytes + kLeafSlotSize;
    return Status::Ok();
}
} // namespace

LeafLayoutPlan plan_leaf_layout(std::shared_ptr<const PageState> page, const PageMaintenanceLimits &limits)
{
    LeafLayoutPlan result(limits.metadata);
    result.source = std::move(page);
    if (result.source == nullptr || limits.primary_page_bytes <= kPageOverhead + kLeafSlotSize ||
        limits.primary_page_bytes > UINT32_MAX || limits.target_page_bytes <= kPageOverhead ||
        limits.target_page_bytes > limits.primary_page_bytes || limits.max_output_pages == 0) {
        result.status = Status::invalid_argument("invalid leaf layout input or capacity");
        return result;
    }
    LogicalPageCursor cursor(result.source, result.source->range, UINT64_MAX, limits.max_sources, false,
                             limits.metadata);
    if (!cursor.status().ok()) {
        result.status    = cursor.status();
        result.readiness = MaintenanceReadiness::kDeferred;
        return result;
    }
    if (cursor.needs_base().has_value()) {
        result.readiness = MaintenanceReadiness::kNeedsBase;
        result.loads.push_back({.page = result.source, .base_index = *cursor.needs_base()});
        return result;
    }
    size_t current_bytes          = kPageOverhead;
    size_t separator_bytes        = 0;
    bool   split_budget_exhausted = false;
    result.total_primary_bytes    = kPageOverhead;
    while (cursor.valid()) {
        if (result.entry_count >= limits.max_work_entries) {
            result.status = Status::resource_exhausted("leaf layout work budget exhausted");
            break;
        }
        size_t bytes    = 0;
        bool   overflow = false;
        result.status   = entry_bytes(cursor.key(), cursor.cell(), limits, &bytes, &overflow);
        if (!result.status.ok()) {
            break;
        }
        if (current_bytes != kPageOverhead &&
            bytes > limits.target_page_bytes - std::min(current_bytes, limits.target_page_bytes)) {
            if (result.primary_bytes.size() + 1 >= limits.max_output_pages ||
                cursor.key().size() > limits.max_metadata_bytes - separator_bytes) {
                split_budget_exhausted = true;
            }
            if (!split_budget_exhausted) {
                result.primary_bytes.push_back(current_bytes);
                result.separators.emplace_back(cursor.key(), limits.metadata);
                separator_bytes += cursor.key().size();
            }
            current_bytes = kPageOverhead;
        }
        if (bytes > std::numeric_limits<size_t>::max() - result.total_primary_bytes) {
            result.status = Status::resource_exhausted("leaf layout size overflow");
            break;
        }
        current_bytes += bytes;
        result.total_primary_bytes += bytes;
        ++result.entry_count;
        result.overflow_values += static_cast<size_t>(overflow);
        cursor.next();
    }
    if (result.status.ok() && !cursor.status().ok()) {
        result.status = cursor.status();
    }
    const auto pending_base = cursor.needs_base();
    if (result.status.ok() && pending_base) {
        result.readiness = MaintenanceReadiness::kNeedsBase;
        result.loads.push_back({.page = result.source, .base_index = *pending_base});
        result.primary_bytes.clear();
        result.separators.clear();
        return result;
    }
    if (result.status.ok() && split_budget_exhausted && result.total_primary_bytes > limits.primary_page_bytes) {
        result.status = Status::resource_exhausted("leaf split output/metadata budget exhausted");
    }
    if (!result.status.ok()) {
        result.readiness = MaintenanceReadiness::kDeferred;
        result.primary_bytes.clear();
        result.separators.clear();
        return result;
    }
    result.primary_bytes.push_back(current_bytes);
    if (result.total_primary_bytes <= limits.primary_page_bytes) {
        result.separators.clear();
        result.primary_bytes = {result.total_primary_bytes};
    }
    return result;
}
} // namespace crowdb::tree
