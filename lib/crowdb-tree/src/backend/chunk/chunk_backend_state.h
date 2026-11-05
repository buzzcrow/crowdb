// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/checkpoint_backend.h"
#include "backend/chunk/chunk_collector.h"
#include "backend/chunk/chunk_pin_release.h"
#include "backend/chunk/chunk_runtime_ids.h"
#include "backend/chunk/chunk_runtime_page_store.h"
#include "snapshot/chunk_root_codec.h"

namespace crowdb::tree
{
class ChunkRootCommit;

// Stable authority owner. Root identity is independent of the live mapping
// generation and its staged pack references. No whole committed map is kept.
struct ChunkBackendState final : CheckpointBackend
{
    ~ChunkBackendState() override;
    std::shared_ptr<detail::ChunkPinOwner>    writer_session;
    bool                                      owns_writer_domain = false;
    std::shared_ptr<const ChunkBackingDomain> domain;
    std::shared_ptr<detail::ChunkRuntimeIds>  ids;
    std::shared_ptr<const void>               backend_owner;
    detail::ChunkRuntimePageStore            *facade = nullptr;
    // Reserved for the catalog's actual root-retention owner, including old
    // committed roots until authority GC retires their reference windows.
    std::shared_ptr<const void>                           checkpoint_roles;
    std::atomic<std::shared_ptr<const ChunkRootManifest>> committed_root;
    std::atomic<uint64_t>                                 root_generation{0};
    std::atomic<bool>                                     collection_active{false};
    detail::ChunkCollector                                collector;

    [[nodiscard]] bool                supported() const noexcept override;
    [[nodiscard]] BackingStatsSampler backing_sampler() const noexcept override;

    [[nodiscard]] uint64_t backend_identity() const noexcept override
    {
        return domain->backend_id;
    }

    [[nodiscard]] uint64_t incarnation() const noexcept override
    {
        return domain->epoch;
    }

    [[nodiscard]] uint64_t requested_wal_offset() const noexcept override
    {
        return facade->requested_wal_offset();
    }

    [[nodiscard]] std::optional<size_t> checkpoint_allocation_bound(size_t frames,
                                                                    size_t heads) const noexcept override;
    [[nodiscard]] size_t                reservation_allocation_bound() const noexcept override;
    Status prepare_checkpoint(BackendCheckpointInput input, std::shared_ptr<BackendCheckpoint> *out) override;
    void   close_writer() noexcept override;
    void   recovered(const ChunkRootIdentity &identity, std::shared_ptr<const ChunkRootManifest> root = {}) noexcept;
    void   completed(const ChunkRootCommit &operation) noexcept;
    [[nodiscard]] ChunkRootIdentity next_identity(const PublishedTreeState &view) const noexcept;
    [[nodiscard]] PageWriteOperation::ReserveExtent
    reserve(const std::shared_ptr<detail::AllocationReservation> &metadata) const override;
};
} // namespace crowdb::tree
