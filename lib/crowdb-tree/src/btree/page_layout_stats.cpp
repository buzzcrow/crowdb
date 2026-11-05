// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_maintenance.h"

namespace crowdb::tree
{
Status accumulate_leaf_stats(Slice key, const LogicalCell &cell, LeafLayoutStats *stats)
{
    if (cell.tombstone()) {
        return Status::Ok();
    }
    if (cell.overflow() && cell.encoded.size() < kOverflowCellSize) {
        return Status::corruption("leaf layout found an invalid overflow cell");
    }
    const uint64_t bytes = cell.overflow() ? CellView(cell.encoded).overflow_len() : cell.value.size();
    if (bytes > UINT64_MAX - key.size() || bytes + key.size() > UINT64_MAX - stats->kv_bytes ||
        stats->kv_count == UINT64_MAX) {
        return Status::resource_exhausted("leaf layout statistics overflow");
    }
    ++stats->kv_count;
    stats->kv_bytes += bytes + key.size();
    return Status::Ok();
}
} // namespace crowdb::tree
