// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_backend_state.h"

#include "backend/chunk/chunk_frame.h"
#include "backend/chunk/chunk_physical_registry.h"
#include "snapshot/chunk_root_commit.h"

namespace crowdb::tree
{
ChunkBackendState::~ChunkBackendState()
{
    close_writer();
    if (owns_writer_domain) {
        facade->release_runtime_domain();
    }
}

BackingStatsSampler ChunkBackendState::backing_sampler() const noexcept
{
    if (!domain || !domain->physical_registry) {
        return {};
    }
    const auto &registry = domain->physical_registry;
    return {.scope_id = registry->backing_stats().scope_id, .metrics = registry->backing_metrics()};
}

void ChunkBackendState::close_writer() noexcept
{
    writer_session.reset();
}

void ChunkBackendState::recovered(const ChunkRootIdentity &identity) noexcept
{
    durable_coverage.store(identity.coverage, std::memory_order_release);
    snapshot_sequence.store(identity.snapshot_sequence, std::memory_order_release);
    root_generation.store(identity.root_generation, std::memory_order_release);
    facade->committed(identity.root_generation, identity.wal_replay_offset);
}

void ChunkBackendState::completed(const ChunkRootCommit &operation) noexcept
{
    if (operation.committed()) {
        recovered(operation.identity());
    }
    if (operation.needs_recovery() || !operation.done()) {
        needs_recovery.store(true, std::memory_order_release);
    }
}

ChunkRootIdentity ChunkBackendState::next_identity(const PublishedTreeState &view) const noexcept
{
    return {.tree              = domain->tree,
            .root_generation   = root_generation.load(std::memory_order_acquire) + 1,
            .owner_epoch       = domain->epoch,
            .snapshot_sequence = snapshot_sequence.load(std::memory_order_acquire) + 1,
            .page_generation   = view.generation,
            .coverage          = view.coverage,
            .gc_floor          = view.gc_floor,
            .root_page_id      = view.root_page_id,
            .next_page_id      = view.next_page_id,
            .wal_replay_offset = facade->requested_wal_offset()};
}

PageWriteOperation::ReserveExtent
ChunkBackendState::reserve(const std::shared_ptr<detail::AllocationReservation> &metadata) const
{
    return [domain = domain, ids = ids, metadata,
            capacity = facade->options().max_chunk_bytes](uint64_t bytes, PageWriteOperation::ExtentComplete complete) {
        if (bytes == 0 || bytes > UINT32_MAX || detail::chunk_framed_length(bytes) > capacity) {
            complete(Status::resource_exhausted(), {});
            return;
        }
        detail::ChunkStageId              stage;
        detail::ChunkId                   chunk;
        auto                              status = ids->next(PagePurpose::kBtreePage, &stage, &chunk);
        std::shared_ptr<ChunkPackBacking> backing;
        if (status.ok()) {
            status = prepare_chunk_backing(domain, stage, chunk, static_cast<uint32_t>(bytes), PagePurpose::kBtreePage,
                                           metadata, &backing);
        }
        if (!status.ok()) {
            complete(std::move(status), {});
            return;
        }
        complete(Status::Ok(), {.backend_id       = domain->backend_id,
                                .address          = 0,
                                .length           = bytes,
                                .content_id       = stage.operation_low,
                                .generation       = domain->epoch,
                                .owner            = backing,
                                .physical_metrics = {},
                                .chunk            = backing});
    };
}
} // namespace crowdb::tree
