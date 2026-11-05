// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/captured_write.h"

namespace crowdb::tree
{
struct CapturedRebaseResult
{
    Status   status;
    uint64_t installed = 0;
    uint64_t deferred  = 0;
    // Exact captured primary receipts require no representation replacement.
    uint64_t reused = 0;
};

// Installs protected reloadable receipts after staged writeback or durable
// Commit. A conflict leaves the live page untouched; failure only defers
// reclamation and never advances or rolls back durable coverage.
class CapturedRebaseOperation
{
    struct State;

  public:
    static Status
    create(PublicationCoordinator &publisher, std::shared_ptr<const PublishedTreeState> captured,
           std::shared_ptr<const CapturedDurablePages> pages, std::shared_ptr<detail::FlushRuntime> runtime,
           std::shared_ptr<detail::FlushRuntime::Lane> lane, std::shared_ptr<detail::AllocationReservation> metadata,
           std::shared_ptr<CapturedRebaseOperation> *out, size_t max_sources = 128,
           std::shared_ptr<WritebackMetrics> writeback_metrics = {}, std::shared_ptr<ApplyDispatcher> cpu_progress = {},
           std::shared_ptr<PublicationLane> publication = {}, TreeWorkCause cause = TreeWorkCause::Checkpoint);
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t primary_frames, size_t max_sources = 128,
                                                                bool   track_residency = true,
                                                                size_t rebase_batches  = SIZE_MAX) noexcept;
    [[nodiscard]] detail::CallbackSender       sender() noexcept;
    [[nodiscard]] const CapturedRebaseResult  &result() const noexcept;
    void                                       request_stop() noexcept;

  private:
    explicit CapturedRebaseOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    static Status          initialize(std::shared_ptr<State> state, std::shared_ptr<detail::FlushRuntime> runtime,
                                      std::shared_ptr<detail::FlushRuntime::Lane>    lane,
                                      std::shared_ptr<detail::AllocationReservation> metadata,
                                      std::shared_ptr<CapturedRebaseOperation> *out, size_t max_sources,
                                      std::shared_ptr<WritebackMetrics> writeback_metrics,
                                      std::shared_ptr<ApplyDispatcher>  cpu_progress,
                                      std::shared_ptr<PublicationLane> publication, TreeWorkCause cause);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
