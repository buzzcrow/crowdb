// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/flush_persistence.h"

#include "backend/checkpoint_backend.h"
#include "btree/allocation_bound.h"
#include "snapshot/primary_backing.h"

#include <algorithm>

namespace crowdb::tree
{
FlushPersistence::FlushPersistence(std::shared_ptr<checkpoint_detail::Preparation> preparation, uint64_t wal_offset)
    : preparation_(std::move(preparation)),
      wal_offset_(wal_offset)
{
}

void FlushPersistence::configure(FlushBatchOptions &options)
{
    initial_   = preparation_->core->publisher().capture();
    inherited_ = initial_->snapshot_baseline ? initial_->snapshot_baseline->pages : nullptr;
    select_inputs();
    options.persistent       = true;
    options.persistence      = shared_from_this();
    options.checkpoint_token = initial_->checkpoint_token + 1;
    report_.checkpoint_token = options.checkpoint_token;
}

void FlushPersistence::select_inputs()
{
    pending_leaves_.reset();
    if (selection_metadata_) {
        selection_metadata_->seal();
    }
    const auto bytes = detail::AllocationBound(detail::shared_allocation_bound<detail::AllocationReservation>())
                           .add_product(initial_->snapshot_changes.size(), sizeof(const SnapshotPageChange *))
                           .value();
    if (!bytes) {
        throw std::bad_alloc();
    }
    auto credit = detail::ResourceBudget::try_reserve(preparation_->core->binding()->account(),
                                                      detail::BudgetClass::kRouting, *bytes);
    if (!credit) {
        throw std::bad_alloc();
    }
    selection_metadata_ = detail::AllocationReservation::create(std::move(*credit));
    pending_leaves_.emplace(detail::ReservationAllocator<const SnapshotPageChange *>(selection_metadata_));
    pending_leaves_->reserve(initial_->snapshot_changes.size());
    uint64_t next = 0;
    while (!initial_->snapshot_changes.visit_from(
        next, 64, &*pending_leaves_, +[](void *raw, const SnapshotPageChange &change) {
            if (change.after.present && change.after.level == 0) {
                static_cast<detail::ReservedVector<const SnapshotPageChange *> *>(raw)->push_back(&change);
            }
        })) {
    }
    std::sort(pending_leaves_->begin(), pending_leaves_->end(),
              [](const auto *a, const auto *b) { return a->after.range.lower < b->after.range.lower; });
    selection_metadata_->seal();
}

bool FlushPersistence::selected(uint64_t pid, const PageKeyRange &range) const
{
    if (preparation_->options.write.frames.exclusion.length != 0) {
        return true;
    }
    if (!inherited_ || !pending_leaves_) {
        return true;
    }
    const auto page = initial_->mapping.get(pid);
    if (page && page->primary && page->primary->frame.summary) {
        const auto oldest = page->primary->frame.summary->oldest_tombstone_slot;
        if (oldest != UINT64_MAX && oldest <= std::min(preparation_->core->requested_gc_floor(), initial_->coverage)) {
            return true;
        }
    }
    if (!page || page->metrics.dirty || !inherited_->frames.contains(pid)) {
        return true;
    }
    const auto found = std::lower_bound(
        pending_leaves_->begin(), pending_leaves_->end(), range.lower, [](const auto *change, const auto &lower) {
            return lower && change->after.range.upper && *change->after.range.upper <= *lower;
        });
    return found != pending_leaves_->end() && (*found)->after.range.overlaps(range);
}

std::optional<CheckpointResult> FlushPersistence::result() const
{
    return finished_ ? std::optional(report_) : std::nullopt;
}
} // namespace crowdb::tree
