// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "flush_report_sender.h"
#include "flush_reservation.h"
#include "source_admission.h"

namespace crowdb::tree
{
class ApplyProgress;
class SourcePause;

struct FrozenSourceView
{
    std::shared_ptr<const SourceCatalog> catalog;
    // Complete L1 paired with catalog.floor, fixed before capture terminal.
    std::shared_ptr<const PublishedTreeState> base     = nullptr;
    uint64_t                                  frontier = 0;
    uint64_t                                  batch_id = 0;
};

struct FlushReservePlan
{
    std::shared_ptr<const PublishedTreeState> view    = nullptr;
    std::shared_ptr<const SourceCatalog>      sources = nullptr;
    detail::FlushReserveBounds                bounds{};
    std::shared_ptr<SourcePause>              pause = nullptr;
};

class FlushOperation
{
    struct State;

  public:
    ~FlushOperation();
    FlushOperation(const FlushOperation &)                                 = delete;
    FlushOperation                      &operator=(const FlushOperation &) = delete;
    [[nodiscard]] detail::CallbackSender sender() noexcept;
    [[nodiscard]] FlushReportSender      report_sender() noexcept;
    // Close, publish the successor, and drain this exact finite source set.
    // The normal sender resumes its handoff; capture does not credit coverage.
    // Like the normal sender, only one connection may run at a time.
    [[nodiscard]] FlushReportSender capture_sender() noexcept;
    // Read after a successful capture terminal. These immutable owners remain
    // valid while the operation resumes and after the source Tree closes.
    [[nodiscard]] FrozenSourceView frozen_sources() const noexcept;
    // Host-only: wait for an existing connection, then run this finite operation.
    [[nodiscard]] Status wait();
    // Host capture joins a current connection and preserves its finite source
    // identity; asynchronous sender connections still reject concurrent use.
    [[nodiscard]] Status wait_capture(FlushResult *out);
    void                 request_stop() noexcept;
    // Read only after terminal. A retry preserves batch/capture identities.
    [[nodiscard]] const FlushResult &result() const noexcept;

  private:
    friend class FlushController;

    explicit FlushOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    [[nodiscard]] Status   wait_report(bool capture_only, FlushResult *out);
    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    static void            submit_report(void *source, FlushReportComplete complete, void *context);
    static void            submit_capture(void *source, FlushReportComplete complete, void *context);
    std::shared_ptr<State> state_;
};

// One controller per Tree owns repair state even if a caller drops its handle.
// Before enabling writes, the host must publish SourceAdmission::catalog()'s
// complete initial membership with the initial Tree mapping. Runtime, source
// admission, publisher and contiguous tracker outlive all operation callbacks.
class FlushController
{
  public:
#ifdef CROWDB_TREE_TEST_UTIL
    void set_handoff_wait_for_tests(std::function<bool(uint64_t, std::function<void()>)> callback);
#endif
    FlushController(PublicationCoordinator &publisher, detail::FlushRuntime &runtime, SourceAdmission &sources,
                    const std::atomic<uint64_t> &contiguous, std::shared_ptr<detail::FlushRuntime::Lane> lane,
                    FlushBatchOptions options = {}, FlushBatch::Maintenance maintenance = {},
                    std::shared_ptr<detail::AllocationReservation> lifecycle = {},
                    std::shared_ptr<ApplyProgress>                 progress  = {});
    [[nodiscard]] static size_t allocation_bound() noexcept;
    [[nodiscard]] size_t        source_window() const noexcept;
    [[nodiscard]] Status check_rotation(const FlushPreparationOptions &preparation, const PublishedTreeState &view,
                                        const SourceCatalog &catalog, WriteFootprint sealed) const;
    ~FlushController();
    FlushController(const FlushController &)            = delete;
    FlushController &operator=(const FlushController &) = delete;

    // The caller has already acquired whole-batch metadata credit against the
    // named immutable state. A stale sequence fails before source closure.
    // Omitting the sequence captures it here for standalone callers. Existing
    // repair work is returned unchanged; only a completed operation permits a
    // new capture. The supplied maintenance callback owns its reclaim admission.
    [[nodiscard]] Status prepare(std::shared_ptr<detail::AllocationReservation> metadata,
                                 std::shared_ptr<FlushOperation>               *out,
                                 std::optional<uint64_t>                        expected_commit_seq = {});
    // Derive the complete handoff bound from source admission caps and the
    // immutable mapping, acquire all progress partitions, then prepare their
    // loader/maintenance consumers. Rejection leaves sources open.
    [[nodiscard]] Status prepare(const FlushPreparationOptions &preparation, std::shared_ptr<FlushOperation> *out);
    [[nodiscard]] Status prepare(const FlushPreparationOptions &preparation, std::shared_ptr<FlushOperation> *out,
                                 bool *blocked);
    // Return existing repair work, or a complete immutable-state reserve plan.
    // The caller may await each named partition without holding this gate.
    [[nodiscard]] Status prepare_or_plan(const FlushPreparationOptions   &preparation,
                                         std::shared_ptr<FlushOperation> *existing, FlushReservePlan *plan);
    [[nodiscard]] Status prepare_prepaid(const FlushPreparationOptions &preparation, const FlushReservePlan &plan,
                                         detail::FlushCredits credits, std::shared_ptr<FlushOperation> *out);
    // Reject new prepare calls without cancelling accepted work. The host
    // awaits operation terminals and releases writers before draining and
    // destroying the shared runtime, source admission or publisher.
    void close() noexcept;
    // Host teardown after prepare callers have joined. Running/late callbacks
    // retain their own state; the controller must not retain backend tickets
    // while the host waits for those tickets to drain.
    void release_pending() noexcept;

  private:
    struct State;
    Status prepare_impl(std::shared_ptr<detail::AllocationReservation> metadata, std::shared_ptr<FlushOperation> *out,
                        std::optional<uint64_t> expected_commit_seq, const FlushPreparationOptions *preparation,
                        const FlushReservePlan *plan = nullptr, detail::FlushCredits *credits = nullptr,
                        bool *blocked = nullptr);
    detail::ReservedUnique<State> state_;
};
} // namespace crowdb::tree
