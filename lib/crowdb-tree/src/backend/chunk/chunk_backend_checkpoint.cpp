// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_backend_state.h"
#include "chunk_protocol_operation.h"
#include "snapshot/chunk_root_commit.h"
#include "snapshot/snapshot_baseline.h"

namespace crowdb::tree
{
namespace
{
class ChunkCheckpoint final : public BackendCheckpoint
{
  public:
    ChunkBackendState                      *backend = nullptr;
    std::shared_ptr<ChunkRootCommit>        commit;
    std::shared_ptr<ChunkProtocolOperation> operation;

    detail::CallbackSender sender() noexcept override
    {
        return operation->sender();
    }

    void request_stop() noexcept override
    {
        operation->request_stop();
    }

    void completed() noexcept override
    {
        backend->completed(*commit);
    }

    [[nodiscard]] bool committed() const noexcept override
    {
        return commit->committed();
    }

    [[nodiscard]] Status status() const override
    {
        return commit->status();
    }
};
} // namespace

bool ChunkBackendState::supported() const noexcept
{
    return domain && domain->supported() && ids && facade != nullptr;
}

std::optional<size_t> ChunkBackendState::checkpoint_allocation_bound(size_t frames, size_t heads) const noexcept
{
    return detail::AllocationBound()
        .add(ChunkRootCommit::allocation_bound(frames,
                                               [&] {
                                                   const auto root = committed_root.load(std::memory_order_acquire);
                                                   return root ? root->indexes.size() : 0;
                                               }()))
        .add(ChunkRootCommit::window_allocation_bound(heads))
        .add(ChunkProtocolOperation::allocation_bound())
        .add(detail::shared_allocation_bound<ChunkCheckpoint>())
        .value();
}

size_t ChunkBackendState::reservation_allocation_bound() const noexcept
{
    return chunk_backing_allocation_bound();
}

Status ChunkBackendState::prepare_checkpoint(BackendCheckpointInput input, std::shared_ptr<BackendCheckpoint> *out)
{
    auto identity              = next_identity(input.view);
    identity.next_page_id      = input.pages->next_page_id;
    identity.wal_replay_offset = input.wal_offset;
    auto result                = detail::reserved_shared<ChunkCheckpoint>(input.budget.bytes);
    result->backend            = this;
    auto status = ChunkRootCommit::create(input.pages, domain, ids, identity, input.budget.bytes, input.budget.bytes,
                                          &result->commit, input.max_dependency_steps,
                                          committed_root.load(std::memory_order_acquire),
                                          input.view.snapshot_baseline ? input.view.snapshot_baseline->pages : nullptr);
    if (!status.ok()) {
        return status;
    }
    status = ChunkProtocolOperation::create(
        input.store, input.runtime, input.lane, input.budget, input.owner,
        [operation = result->commit](bool stopped) { return operation->step(stopped); },
        [operation = result->commit] { return operation->done(); }, &result->operation);
    if (status.ok()) {
        *out = std::move(result);
    }
    return status;
}
} // namespace crowdb::tree
