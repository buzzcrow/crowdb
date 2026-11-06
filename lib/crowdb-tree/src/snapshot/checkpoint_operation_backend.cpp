// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/checkpoint_operation_internal.h"

namespace crowdb::tree
{
bool CheckpointOperation::State::backend_needs_recovery() const noexcept
{
    return chunk ? chunk->needs_recovery.load(std::memory_order_acquire) : backend->checkpoints->needs_recovery();
}

uint64_t CheckpointOperation::State::backend_identity() const noexcept
{
    return chunk ? chunk->backend_identity() : backend->ledger->limits().backend_id;
}

std::shared_ptr<const CapturedDurablePages> CheckpointOperation::State::inherited_pages() const
{
    return backend ? backend->inherited : nullptr;
}

void CheckpointOperation::State::begin_chunk_checkpoint()
{
    auto status = chunk->prepare_checkpoint({.pages                = written_pages,
                                             .view                 = *view,
                                             .wal_offset           = chunk_wal_offset,
                                             .max_dependency_steps = options.write.max_dependency_steps,
                                             .store                = store,
                                             .runtime              = runtime,
                                             .lane                 = lane,
                                             .budget               = budget,
                                             .owner                = owner},
                                            &chunk_checkpoint);
    if (!status.ok()) {
        reconcile(std::move(status));
        return;
    }
    write.reset();
    begin_child(chunk_checkpoint->sender(), Stage::Checkpoint);
}
} // namespace crowdb::tree
