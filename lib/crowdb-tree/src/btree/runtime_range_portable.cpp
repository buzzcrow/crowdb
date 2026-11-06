// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "portable_import.h"
#include "portable_import_plan.h"
#include "runtime_range_rebuild.h"
#include "snapshot/published_snapshot_entries.h"
#include "snapshot/recovered_state.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
Status reserve_range(const std::shared_ptr<PublishedSnapshotContext> &snapshot, TreeCore &destination,
                     const KeyRange &range, RuntimeRangeImport &staging)
{
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    auto                                      status = PublishedSnapshotEntries::create(snapshot, &cursor, range);
    if (!status.ok()) {
        return status;
    }
    PortableImportPlan plan(destination.config(), destination.load_limits());
    bool               complete = false;
    while (!complete) {
        std::optional<PublishedScanEntry> entry;
        status = cursor->next(&entry, &complete);
        if (!status.ok()) {
            return status;
        }
        if (entry) {
            status = plan.append(entry->key.size(), entry->cell.value.size(), entry->cell.tombstone());
            if (!status.ok()) {
                return status;
            }
        }
    }
    const auto shape = plan.finish();
    if (!shape) {
        return Status::resource_exhausted("range import shape overflow");
    }
    return reserve_range_shape(destination, *shape, true, staging);
}
} // namespace

Status copy_portable_range(TreeCore &source, TreeCore &destination, const KeyRange &range, RuntimeRangeImport &staging)
{
    const auto                                frontier = source.contiguous_slot();
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    auto                                      status = source.capture_snapshot(&snapshot);
    if (!status.ok()) {
        return status;
    }
    if (snapshot->view()->coverage < frontier) {
        snapshot.reset();
        status = source.flush();
        if (!status.ok()) {
            return status;
        }
        status = source.capture_snapshot(&snapshot);
        if (!status.ok()) {
            return status;
        }
    }
    return copy_snapshot_range(snapshot, destination, range, staging);
}

Status copy_snapshot_range(const std::shared_ptr<PublishedSnapshotContext> &snapshot, TreeCore &destination,
                           const KeyRange &range, RuntimeRangeImport &staging)
{
    auto status = reserve_range(snapshot, destination, range, staging);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    status = PublishedSnapshotEntries::create(snapshot, &cursor, range);
    if (!status.ok()) {
        return status;
    }
    PortableImportBuilder builder(destination.config(), destination.load_limits(), snapshot->view()->coverage,
                                  staging.credit);
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> cell{
        detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(staging.credit)};
    bool complete = false;
    while (!complete) {
        std::optional<PublishedScanEntry> entry;
        status = cursor->next(&entry, &complete);
        if (!status.ok()) {
            return status;
        }
        if (!entry) {
            continue;
        }
        cell.resize(kCellHeaderSize + entry->cell.value.size());
        frame_put_u64(cell.data(), 0, entry->cell.slot);
        cell[sizeof(uint64_t)] = entry->cell.tombstone() ? kFlagTombstone : 0;
        if (!entry->cell.value.empty()) {
            std::copy_n(reinterpret_cast<const uint8_t *>(entry->cell.value.data()), entry->cell.value.size(),
                        cell.data() + kCellHeaderSize);
        }
        status = builder.append(Slice(entry->key), Slice(cell.data(), cell.size()));
        if (!status.ok()) {
            return status;
        }
        ++staging.stats.entries_examined;
        ++staging.stats.entries_emitted;
    }
    staging.stats.subtrees_skipped = cursor->skipped_subtrees();
    status                         = builder.finish(&staging.catalog);
    if (status.ok()) {
        staging.stats.pages_rebuilt = staging.catalog->frames.size();
    }
    return status;
}
} // namespace crowdb::tree
