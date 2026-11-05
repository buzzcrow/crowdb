// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/backing_stats_sampler.h"
#include "btree/page_writeback.h"
#include "crowdb-tree/btree/diagnostics.h"

namespace crowdb::tree
{
struct PublishedTreeState;
struct CapturedDurablePages;

class BackendCheckpoint
{
  public:
    virtual ~BackendCheckpoint()                           = default;
    virtual detail::CallbackSender sender() noexcept       = 0;
    virtual void                   request_stop() noexcept = 0;
    // Reconciles durable authority even when the transport returned an error.
    virtual void                 completed() noexcept       = 0;
    [[nodiscard]] virtual bool   committed() const noexcept = 0;
    [[nodiscard]] virtual Status status() const             = 0;
};

struct BackendCheckpointInput
{
    std::shared_ptr<const CapturedDurablePages> pages;
    const PublishedTreeState                   &view;
    uint64_t                                    wal_offset;
    size_t                                      max_dependency_steps;
    std::shared_ptr<AsyncPageStore>             store;
    std::shared_ptr<detail::FlushRuntime>       runtime;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    PageLoadBudget                              budget;
    std::shared_ptr<const void>                 owner;
};

// An injected immutable-storage authority. Generic Tree orchestration owns no
// concrete catalog, protocol, or remote transport implementation.
class CheckpointBackend
{
  public:
    virtual ~CheckpointBackend() = default;

    std::shared_ptr<AsyncPageStore> store;
    uint32_t                        iu_size         = 0;
    size_t                          max_frame_bytes = 0;
    size_t                          max_blob_bytes  = 0;
    std::atomic<uint64_t>           durable_coverage{0};
    std::atomic<uint64_t>           snapshot_sequence{0};
    std::atomic<bool>               needs_recovery{false};

    [[nodiscard]] virtual bool                  supported() const noexcept                               = 0;
    virtual void                                close_writer() noexcept                                  = 0;
    [[nodiscard]] virtual uint64_t              backend_identity() const noexcept                        = 0;
    [[nodiscard]] virtual uint64_t              incarnation() const noexcept                             = 0;
    [[nodiscard]] virtual uint64_t              requested_wal_offset() const noexcept                    = 0;
    [[nodiscard]] virtual std::optional<size_t> checkpoint_allocation_bound(size_t frames,
                                                                            size_t heads) const noexcept = 0;
    [[nodiscard]] virtual size_t                reservation_allocation_bound() const noexcept            = 0;
    [[nodiscard]] virtual PageWriteOperation::ReserveExtent
                   reserve(const std::shared_ptr<detail::AllocationReservation> &metadata) const             = 0;
    virtual Status prepare_checkpoint(BackendCheckpointInput input, std::shared_ptr<BackendCheckpoint> *out) = 0;

    [[nodiscard]] virtual BackingStatsSampler backing_sampler() const noexcept
    {
        return {};
    }
};
} // namespace crowdb::tree
