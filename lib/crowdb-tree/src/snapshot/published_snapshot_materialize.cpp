// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_snapshot_materialize.h"

#include "btree/allocation_bound.h"

#include <climits>

namespace crowdb::tree
{
namespace
{
struct MaterializePlan
{
    size_t count = 0;
    size_t bytes = sizeof(SnapshotMaterialized) + detail::shared_allocation_bound<detail::AllocationReservation>();

    Status accept(const PublishedScanEntry &entry, size_t limit)
    {
        // Direct string construction has no old buffer. Reserve conservative
        // growth room, then transfer only actual heap capacity to allocation.
        const auto bound = detail::AllocationBound(bytes)
                               .add((sizeof(leaf_entry) * 2) + sizeof(std::string) + 1 + 9)
                               .add_product(entry.key.size(), 2)
                               .add(entry.cell.value.size())
                               .value();
        if (!bound || *bound > limit || count == INT_MAX) {
            return Status::resource_exhausted("snapshot compatibility result exceeds its capacity limit");
        }
        ++count;
        bytes = *bound;
        return Status::Ok();
    }
};

size_t key_heap_bytes(const std::string &key)
{
    const auto object     = reinterpret_cast<uintptr_t>(&key);
    const auto data       = reinterpret_cast<uintptr_t>(key.data());
    const auto object_end = reinterpret_cast<uintptr_t>(std::addressof(key) + 1);
    return data >= object && data < object_end ? 0 : key.capacity() + 1;
}

Status plan_materialization(const std::shared_ptr<PublishedSnapshotContext> &snapshot, size_t max_bytes,
                            MaterializePlan *plan)
{
    if (plan->bytes > max_bytes) {
        return Status::resource_exhausted("snapshot compatibility result cannot fit its control state");
    }
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    auto                                      status = PublishedSnapshotEntries::create(snapshot, &cursor);
    if (!status.ok()) {
        return status;
    }
    bool done = false;
    while (!done) {
        std::optional<PublishedScanEntry> entry;
        status = cursor->next(&entry, &done);
        if (!status.ok()) {
            return status;
        }
        if (entry) {
            status = plan->accept(*entry, max_bytes);
            if (!status.ok()) {
                return status;
            }
        }
    }
    return Status::Ok();
}

Status copy_materialization(const std::shared_ptr<PublishedSnapshotContext> &snapshot, size_t count,
                            SnapshotMaterialized *result)
{
    result->entries.reserve(count);
    result->charge(result->entries.capacity() * sizeof(leaf_entry));
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    auto                                      status = PublishedSnapshotEntries::create(snapshot, &cursor);
    if (!status.ok()) {
        return status;
    }
    bool done = false;
    while (!done) {
        std::optional<PublishedScanEntry> entry;
        status = cursor->next(&entry, &done);
        if (!status.ok()) {
            return status;
        }
        if (!entry) {
            continue;
        }
        if (result->entries.size() == count) {
            return Status::corruption("snapshot compatibility result grew beyond its fixed preflight");
        }
        leaf_entry copied{.key  = std::string(entry->key.data(), entry->key.size()),
                          .cell = encode_cell_buf(entry->cell.slot,
                                                  entry->cell.tombstone() ? OpKind::kDelete : OpKind::kPut,
                                                  entry->cell.value)};
        result->charge(key_heap_bytes(copied.key) + (copied.cell.inlined() ? 0 : copied.cell.capacity()),
                       AllocationKind::kPayload);
        result->entries.push_back(std::move(copied));
    }
    return result->entries.size() == count
             ? Status::Ok()
             : Status::corruption("snapshot compatibility result shortened after fixed preflight");
}
} // namespace

SnapshotMaterialized::~SnapshotMaterialized()
{
    // Release actual allocations before returning their budget to the host.
    std::vector<leaf_entry>().swap(entries);
    metadata->record_allocation(AllocationKind::kMetadata, metadata_bytes, false);
    metadata->record_allocation(AllocationKind::kPayload, payload_bytes, false);
    metadata->release(metadata_bytes + payload_bytes);
}

void SnapshotMaterialized::charge(size_t bytes, AllocationKind kind)
{
    if (!metadata->claim(bytes)) {
        throw std::bad_alloc();
    }
    metadata->record_allocation(kind, bytes, true);
    if (kind == AllocationKind::kPayload) {
        payload_bytes += bytes;
    }
    else {
        metadata_bytes += bytes;
    }
}

Status materialize_snapshot(const std::shared_ptr<PublishedSnapshotContext> &snapshot, size_t max_bytes,
                            detail::ReservedUnique<SnapshotMaterialized> *out)
try {
    if (!snapshot || out == nullptr || *out) {
        return Status::invalid_argument("snapshot materialization requires a fixed view and empty output");
    }
    MaterializePlan plan;
    auto            status = plan_materialization(snapshot, max_bytes, &plan);
    if (!status.ok()) {
        return status;
    }
    snapshot->lane()->drain();
    auto credit =
        detail::ResourceBudget::try_reserve(snapshot->binding()->account(), detail::BudgetClass::kRouting, plan.bytes);
    if (!credit) {
        return Status::resource_exhausted("snapshot compatibility result reserve unavailable");
    }
    auto metadata = detail::AllocationReservation::create(std::move(*credit));
    auto result   = detail::reserved_unique<SnapshotMaterialized>(metadata, metadata);
    status        = copy_materialization(snapshot, plan.count, result.get());
    if (!status.ok()) {
        return status;
    }
    metadata->seal();
    *out = std::move(result);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot compatibility result allocation failed");
}
} // namespace crowdb::tree
