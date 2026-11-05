// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "maintenance_split_layout.h"

#include "allocation_bound.h"

#include <algorithm>

namespace crowdb::tree::detail
{
namespace
{
constexpr size_t kPageOverhead = kFrameHeaderSize + kFrameTrailerSize;

Status primary_bytes(Slice key, const LogicalCell &cell, const PageMaintenanceLimits &limits, size_t *bytes)
{
    const bool   overflow  = !cell.tombstone() && (cell.overflow() || cell.value.size() > limits.inline_value_bytes);
    const size_t value     = overflow ? kOverflowCellSize : kCellHeaderSize + cell.value.size();
    const auto   available = limits.primary_page_bytes - kPageOverhead - kLeafSlotSize;
    if (key.size() > available || value > available - key.size()) {
        return Status::invalid_argument("one key/cell cannot fit an uncompressed primary leaf frame");
    }
    *bytes = key.size() + value + kLeafSlotSize;
    return Status::Ok();
}
} // namespace

SplitWindowCursor::SplitWindowCursor(std::shared_ptr<const PageState> page, const PageMaintenanceLimits &limits)
    : page_(std::move(page)),
      limits_(limits),
      layout_(limits.metadata),
      cursor_(page_, page_ ? page_->range : PageKeyRange{}, UINT64_MAX, limits.max_sources, false, limits.metadata)
{
    layout_.source = page_;
}

std::optional<BaseLoadRequest> SplitWindowCursor::needs_base() const
{
    const auto index = cursor_.needs_base();
    if (!index) {
        return std::nullopt;
    }
    return BaseLoadRequest{.page = page_, .base_index = *index};
}

Status SplitWindowCursor::advance()
{
    const auto &limits = limits_;
    auto       *layout = &layout_;
    if (done_) {
        return Status::Ok();
    }
    if (limits.primary_page_bytes <= kPageOverhead + kLeafSlotSize || limits.primary_page_bytes > UINT32_MAX ||
        limits.target_page_bytes <= kPageOverhead || limits.target_page_bytes > limits.primary_page_bytes ||
        limits.max_work_entries == 0 || limits.max_output_pages < 2) {
        return Status::invalid_argument("split window requires bounded work and at least two output ranges");
    }
    if (!cursor_.status().ok()) {
        return cursor_.status();
    }
    if (cursor_.needs_base()) {
        return Status::Ok();
    }
    const auto outputs = limits.max_output_pages;
    while (cursor_.valid()) {
        const bool work_limit = entries_ == limits.max_work_entries;
        size_t     bytes      = 0;
        if (!work_limit) {
            const auto status = primary_bytes(cursor_.key(), cursor_.cell(), limits, &bytes);
            if (!status.ok()) {
                return status;
            }
        }
        if (work_limit || (current_bytes_ != kPageOverhead &&
                           bytes > limits.target_page_bytes - std::min(current_bytes_, limits.target_page_bytes))) {
            if (cursor_.key().size() > limits.max_metadata_bytes - separator_bytes_) {
                return Status::resource_exhausted("split separators exceed metadata budget");
            }
            layout->primary_bytes.push_back(current_bytes_);
            layout->statistics.push_back(current_stats_);
            layout->separators.emplace_back(cursor_.key(), limits.metadata);
            separator_bytes_ += cursor_.key().size();
            if (work_limit || layout->separators.size() + 1 == outputs) {
                done_ = true;
                return Status::Ok();
            }
            current_bytes_ = kPageOverhead;
            current_stats_ = {};
        }
        auto status = accumulate_leaf_stats(cursor_.key(), cursor_.cell(), &current_stats_);
        if (!status.ok()) {
            return status;
        }
        current_bytes_ += bytes;
        ++entries_;
        cursor_.next();
        if (!cursor_.status().ok()) {
            return cursor_.status();
        }
        if (cursor_.needs_base()) {
            return Status::Ok();
        }
    }
    layout->primary_bytes.push_back(current_bytes_);
    layout->statistics.push_back(current_stats_);
    done_ = true;
    return Status::Ok();
}

Status plan_split_window(const std::shared_ptr<const PageState> &page, const PageMaintenanceLimits &limits,
                         SplitWindowLayout *layout, MaintenancePublicationResult *result)
{
    SplitWindowCursor cursor(page, limits);
    auto              status = cursor.advance();
    if (status.ok()) {
        if (auto request = cursor.needs_base()) {
            result->readiness = MaintenanceReadiness::kNeedsBase;
            result->loads.push_back(std::move(*request));
        }
        else {
            *layout = cursor.take_layout();
        }
    }
    return status;
}
} // namespace crowdb::tree::detail
