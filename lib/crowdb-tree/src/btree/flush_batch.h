// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "flush_runtime.h"
#include "logical_page.h"
#include "maintenance_publication.h"
#include "page_encode.h"
#include "published_state.h"

namespace crowdb::tree
{
class PageLoader;
class FlushPersistence;
class ApplyDispatcher;
class PublicationLane;
enum class FlushPhase : uint8_t { kCaptured, kHandoff, kRepairRequired, kCovered, kMaintenance, kComplete };

struct FlushResult
{
    uint64_t   batch_id           = 0;
    uint64_t   floor              = 0;
    uint64_t   frontier           = 0;
    uint64_t   published_coverage = 0;
    Status     handoff_status;
    Status     maintenance_status;
    uint64_t   deferred_pages = 0;
    FlushPhase phase          = FlushPhase::kCaptured;
    // True only for the attempt that first advances the covered frontier.
    bool handoff_advanced = false;
    // A newer publication invalidated preparation before admission/capture.
    // Only this failure permits automatic replanning of the soft request.
    bool admission_stale = false;
};

struct FlushBatchOptions
{
    using PersistComplete = detail::InlineFunction<void(Status, std::shared_ptr<const PublishedPage>)>;
    using ReserveOverflow = detail::InlineFunction<Status(uint64_t, ReservedOverflowIds *)>;
    // Fixed for this flush. The parent submits only pages that survive its
    // sibling decisions; the ordinary memory flow has no persistence sink.
    bool                                         persistent       = false;
    size_t                                       persistent_pages = 0;
    std::shared_ptr<FlushPersistence>            persistence      = nullptr;
    uint64_t                                     checkpoint_token = 0;
    RunLimits                                    run_limits;
    std::optional<MaintenancePublicationOptions> consolidation;
    size_t                                       max_sources            = 64;
    size_t                                       parallelism            = 4;
    size_t                                       inline_entries         = 32;
    size_t                                       inline_bytes           = 64 * 1024;
    size_t                                       max_run_bytes          = 1024 * 1024;
    size_t                                       inline_ranges_per_step = 8;
    // Install the first bounded run window early; later windows double so
    // immutable prefix-array copies remain linear in the batch input.
    size_t                           install_run_window      = 64;
    size_t                           routing_leaves_per_step = 64;
    size_t                           max_route_depth         = 64;
    std::shared_ptr<PageLoader>      loader                  = nullptr;
    std::shared_ptr<PageLoader>      consolidation_loader    = nullptr;
    PageKeyRange                     selection;
    std::shared_ptr<ApplyDispatcher> cpu_progress = nullptr;
    std::shared_ptr<PublicationLane> publication  = nullptr;
    // A controller measures the complete attempt including source capture.
    bool measure_batch = true;
#ifdef CROWDB_TREE_TEST_UTIL
    // Runs after publication admission and before source closure.
    std::function<void()> before_capture_for_tests = nullptr;
    // Called after a bounded run is owned and all source guards are released.
    std::function<void(size_t, size_t)>     after_run_for_tests      = nullptr;
    std::function<void(uint64_t)>           before_handoff_for_tests = nullptr;
    std::function<void(uint64_t, uint32_t)> after_page_for_tests     = nullptr;
#endif
};

// The source admission layer supplies an already closed/drained catalog and
// a fixed frontier. It retains this batch after any partial failure and retries
// it before accepting another batch or changing routing. No Tree worker waits.
// This core has its own mapping owner; legacy mutable mapping cannot be mixed
// with it. Backend/capture adapters must finish migration before default use.
// Host runtime/publisher ownership must outlive all CPU steps, including the
// return from terminal callbacks. Runtime destruction is a host-side operation.
class FlushBatch
{
    struct State;

  public:
    using MaintenanceComplete = detail::InlineFunction<void(Status, uint64_t)>;
    using Maintenance = detail::InlineFunction<void(std::shared_ptr<const PublishedTreeState>, MaintenanceComplete)>;
    // Bound the batch's metadata before closing sources. Counts include
    // admitted writers, not only currently materialized table entries. Source
    // capture/controller and cold routing byte/I/O credit are added separately.
    [[nodiscard]] static std::optional<size_t> metadata_reserve_bound(const MappingView &mapping, size_t sources,
                                                                      size_t entries, size_t key_bytes, size_t workers,
                                                                      const FlushBatchOptions &options,
                                                                      bool                     track_residency = false);
    static Status prepare(PublicationCoordinator &publisher, detail::FlushRuntime &runtime,
                          std::shared_ptr<detail::FlushRuntime::Lane> lane, uint64_t batch_id, uint64_t frontier,
                          FlushBatchOptions options, Maintenance maintenance, std::shared_ptr<FlushBatch> *out,
                          std::shared_ptr<const PublishedSources>        inputs                = {},
                          std::shared_ptr<detail::AllocationReservation> metadata              = {},
                          uint64_t                                       flush_admission_owner = 0);
    ~FlushBatch();
    FlushBatch(const FlushBatch &)            = delete;
    FlushBatch &operator=(const FlushBatch &) = delete;

    // One connected operation at a time. Reconnecting after terminal retries
    // only missing records. Once covered, retries never repeat the handoff.
    [[nodiscard]] detail::CallbackSender sender() noexcept;
    void                                 request_stop() noexcept;
    // Read only after sender terminal (which returns a progress-bearing value).
    [[nodiscard]] const FlushResult &result() const noexcept;

  private:
    explicit FlushBatch(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
