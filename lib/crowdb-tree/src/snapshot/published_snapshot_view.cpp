// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_snapshot_view.h"

#include "btree/allocation_bound.h"
#include "btree/tree_core.h"

#include <stdexcept>

namespace crowdb::tree
{
PublishedSnapshotView::PublishedSnapshotView(std::shared_ptr<PublishedSnapshotContext> context, size_t max_bytes)
    : Snapshot(context->view()->coverage, {}),
      context_(std::move(context)),
      max_bytes_(max_bytes)
{
}

PublishedSnapshotView::~PublishedSnapshotView()
{
    if (auto *stored = materialized_.load(std::memory_order_relaxed)) {
        detail::ReservationDeleter<SnapshotMaterialized>{stored->metadata}(stored);
    }
}

Status PublishedSnapshotView::create(std::shared_ptr<PublishedSnapshotContext> context, size_t max_bytes,
                                     std::shared_ptr<Snapshot> *out)
try {
    if (!context || max_bytes == 0 || out == nullptr || *out) {
        return Status::invalid_argument("snapshot compatibility view requires bounded fixed inputs and empty output");
    }
    const auto bound = detail::shared_allocation_bound<PublishedSnapshotView>() +
                       detail::shared_allocation_bound<detail::AllocationReservation>();
    auto       credit =
        detail::ResourceBudget::try_reserve(context->binding()->account(), detail::BudgetClass::kRouting, bound);
    if (!credit) {
        return Status::resource_exhausted("snapshot compatibility control reserve unavailable");
    }
    auto metadata = detail::AllocationReservation::create(std::move(*credit));
    auto result   = detail::reserved_construct<PublishedSnapshotView>(metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) PublishedSnapshotView(std::move(context), max_bytes);
    });
    if (!result->context_->detached()) {
        detail::ReservedUnique<SnapshotMaterialized> values;
        auto status = materialize_snapshot(result->context_, max_bytes, &values);
        if (!status.ok()) {
            return status;
        }
        result->materialized_.store(values.release(), std::memory_order_release);
        result->context_.reset();
    }
    metadata->seal();
    *out = std::move(result);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot compatibility allocation failed");
}

const std::vector<leaf_entry> &PublishedSnapshotView::entries() const
{
    if (const auto *stored = materialized_.load(std::memory_order_acquire)) {
        return stored->entries;
    }
    detail::ReservedUnique<SnapshotMaterialized> values;
    auto                                         status = materialize_snapshot(context_, max_bytes_, &values);
    if (!status.ok()) {
        throw SnapshotReadError(status);
    }
    auto *expected = static_cast<SnapshotMaterialized *>(nullptr);
    if (materialized_.compare_exchange_strong(expected, values.get(), std::memory_order_acq_rel)) {
        return values.release()->entries;
    }
    return expected->entries;
}

bool PublishedSnapshotView::get(Slice key, uint64_t *out_slot, std::string *out_value) const
{
    if (materialized_.load(std::memory_order_acquire) != nullptr) {
        return Snapshot::get(key, out_slot, out_value);
    }
    GetView value;
    auto    status = context_->get(key, &value);
    if (!status.ok()) {
        throw SnapshotReadError(status);
    }
    if (!value.found()) {
        return false;
    }
    if (out_slot != nullptr) {
        *out_slot = value.slot();
    }
    if (out_value != nullptr) {
        *out_value = value.value().to_string();
    }
    return true;
}

Status create_published_snapshot(const std::shared_ptr<TreeCore> &core, std::shared_ptr<Snapshot> *out)
{
    if (!core || out == nullptr || *out) {
        return Status::invalid_argument("snapshot compatibility view requires a Core and empty output");
    }
    std::shared_ptr<PublishedSnapshotContext> context;
    auto                                      status = core->capture_snapshot(&context);
    if (!status.ok()) {
        return status;
    }
    return PublishedSnapshotView::create(std::move(context), core->binding()->options().limits.routing_bytes, out);
}
} // namespace crowdb::tree
