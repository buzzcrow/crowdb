// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/snapshot/snapshot_io.h"
#include "snapshot/published_byte_export.h"

namespace crowdb::tree
{
SnapshotExport::SnapshotExport(std::shared_ptr<PublishedByteExport> runtime)
    : chunk_bytes_(runtime->info().chunk_bytes),
      at_slot_(runtime->info().slot),
      total_bytes_(runtime->info().total_bytes),
      final_crc32c_(runtime->info().final_crc),
      runtime_(std::move(runtime)),
      facade_credit_(runtime_->metadata(), sizeof(SnapshotExport), AllocationKind::kMetadata)
{
    runtime_->metadata()->seal();
}

Status SnapshotExport::next_owned(SnapshotChunk *out, bool *done)
try {
    if (out == nullptr || done == nullptr) {
        return Status::invalid_argument("snapshot chunk requires output and completion");
    }
    *out = {};
    if (runtime_) {
        auto status = runtime_->next(out, done);
        pos_        = runtime_->info().offset;
        return status;
    }
    auto bytes  = std::make_shared<std::string>();
    auto status = next_chunk(bytes.get(), done);
    if (status.ok()) {
        *out = {
            .bytes             = {reinterpret_cast<const uint8_t *>(bytes->data()), bytes->size()},
            .owner             = std::move(bytes),
            .allocation_budget = {}
        };
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot chunk allocation failed");
}
} // namespace crowdb::tree
