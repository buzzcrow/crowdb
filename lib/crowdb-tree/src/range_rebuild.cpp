// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/range_rebuild.h"

#include "crowdb-tree/cell.h"

#include <algorithm>
#include <utility>

namespace crowdb::tree
{

Status rebuild_range(Crowdbtree &source, const KeyRange &range, Options destination_options,
                     std::unique_ptr<Crowdbtree> *out, RangeRebuildStats *stats)
{
    if (out == nullptr || destination_options.page_store == nullptr) {
        return Status::invalid_argument("range rebuild requires an output and durable destination store");
    }
    Status range_status = range.validate();
    if (!range_status.ok()) {
        return range_status;
    }

    destination_options.key_range = range;
    std::unique_ptr<Crowdbtree> destination;
    Status                      open_status = Crowdbtree::open(destination_options, &destination);
    if (!open_status.ok()) {
        return open_status;
    }

    RangeRebuildStats local;
    auto              snapshot = source.snapshot_view();
    for (const leaf_entry &entry : snapshot->entries()) {
        ++local.entries_examined;
        if (!range.contains(Slice(entry.key))) {
            ++local.entries_filtered;
            continue;
        }
        Batch    batch;
        CellView cell{entry.cell.slice()};
        batch.ops.push_back({.key   = entry.key,
                             .kind  = cell.is_tombstone() ? OpKind::kDelete : OpKind::kPut,
                             .value = cell.is_tombstone() ? std::string() : cell.value().to_string()});
        Status apply_status = destination->apply(cell.slot(), batch);
        if (!apply_status.ok()) {
            return apply_status;
        }
        ++local.entries_emitted;
    }
    destination->force_advance_slot(source.last_applied_slot());
    Status flush_status = destination->flush();
    if (!flush_status.ok()) {
        return flush_status;
    }
    Status snapshot_status = destination->snapshot();
    if (!snapshot_status.ok()) {
        return snapshot_status;
    }
    local.pages_rebuilt = destination->leaf_count_atomic() + destination->inner_count_atomic();
    if (stats != nullptr) {
        *stats = local;
    }
    *out = std::move(destination);
    return Status::Ok();
}

} // namespace crowdb::tree
