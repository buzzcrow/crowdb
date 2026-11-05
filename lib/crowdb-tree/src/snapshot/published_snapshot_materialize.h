// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/published_snapshot_entries.h"

namespace crowdb::tree
{
// The legacy Snapshot API returns std::vector<leaf_entry>. Its allocator type
// cannot change, so these leases account actual retained vector/key/cell
// capacities while the fixed reservation bounds construction.
struct SnapshotMaterialized
{
    explicit SnapshotMaterialized(std::shared_ptr<detail::AllocationReservation> budget) : metadata(std::move(budget))
    {
    }

    ~SnapshotMaterialized();
    void charge(size_t bytes, AllocationKind kind = AllocationKind::kMetadata);

    std::shared_ptr<detail::AllocationReservation> metadata;
    size_t                                         metadata_bytes = 0;
    size_t                                         payload_bytes  = 0;
    std::vector<leaf_entry>                        entries;
};

Status materialize_snapshot(const std::shared_ptr<PublishedSnapshotContext> &snapshot, size_t max_bytes,
                            detail::ReservedUnique<SnapshotMaterialized> *out);
} // namespace crowdb::tree
