// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "apply_dispatcher.h"
#include "maintenance_operation.h"
#include "resource_observation.h"

namespace crowdb::tree::detail
{

struct MaintenanceFactory
{
    MaintenanceFactory(PublicationCoordinator &publish, FlushRuntime &cpu, std::shared_ptr<FlushRuntime::Lane> queue,
                       PageMaintenanceOperationOptions limits, std::shared_ptr<PageLoader> loads = {},
                       std::shared_ptr<ApplyDispatcher> progress = {})
        : publisher(publish),
          runtime(cpu),
          lane(std::move(queue)),
          options(std::move(limits)),
          loader(std::move(loads)),
          retry_loader(loader),
          cpu_progress(std::move(progress))
    {
    }

    PublicationCoordinator             &publisher;
    FlushRuntime                       &runtime;
    std::shared_ptr<FlushRuntime::Lane> lane;
    PageMaintenanceOperationOptions     options;
    std::shared_ptr<PageLoader>         loader;
    std::weak_ptr<PageLoader>           retry_loader;
    std::shared_ptr<ApplyDispatcher>    cpu_progress;
    std::atomic_flag                    active = ATOMIC_FLAG_INIT;
};

struct MaintenanceOperation : std::enable_shared_from_this<MaintenanceOperation>
{
    enum class Phase : uint8_t { Collect, InnerMerge, Analyze, Coalesce, ClassifyMerge, Merge };

    struct Candidate
    {
        uint64_t pid;
        size_t   stale_replans = 0;
    };

    MaintenanceOperation(std::shared_ptr<MaintenanceFactory> owner, uint64_t generation, uint64_t dirty,
                         FlushBatch::MaintenanceComplete        completion,
                         std::shared_ptr<AllocationReservation> reservation);
    ~MaintenanceOperation();
    void        schedule() noexcept;
    static void run(void *context) noexcept;
    void        step();
    void        initialize();
    void        collect();
    void        process();
    bool        begin_cold(std::shared_ptr<const PublishedTreeState> view, std::shared_ptr<const PublishedPage> page);
    void        cold_step();
    bool        cold_route();
    bool        cold_siblings();
    bool        load_cold_inner(uint64_t pid);
    [[nodiscard]] std::shared_ptr<const PublishedPage>      cold_page(uint64_t pid) const;
    void                                                    cold_plan();
    void                                                    advance();
    void                                                    submit(MaintenancePublicationResult result);
    void                                                    published(PublicationResult result);
    void                                                    finish(Status status) noexcept;
    [[nodiscard]] std::shared_ptr<const PublishedTreeState> capture_l1();
    [[nodiscard]] MaintenancePublicationResult              plan(const std::shared_ptr<const PublishedTreeState> &view,
                                                                 const std::shared_ptr<const PublishedPage>      &page);
    [[nodiscard]] Status merge_pair(const std::shared_ptr<const PublishedTreeState> &view,
                                    const std::shared_ptr<const PublishedPage> &left, bool *eligible) const;
    [[nodiscard]] Status classify_merge(const std::shared_ptr<const PublishedTreeState> &view,
                                        const std::shared_ptr<const PublishedPage>      &left,
                                        MergePairPublication                            *classification) const;

    struct Cold
    {
        explicit Cold(const std::shared_ptr<AllocationReservation> &metadata)
            : replacements(ReservationAllocator<PageReplacement>(metadata)),
              siblings(ReservationAllocator<uint64_t>(metadata))
        {
        }

        ReservedVector<PageReplacement>           replacements;
        ReservedVector<uint64_t>                  siblings;
        size_t                                    next_sibling = 0;
        uint64_t                                  loading_pid  = kInvalidPageId;
        uint64_t                                  route_pid    = kInvalidPageId;
        size_t                                    route_steps  = 0;
        std::shared_ptr<const InnerPageState>     loaded_inner;
        bool                                      loading_inner = false;
        std::shared_ptr<const PublishedTreeState> view;
        std::shared_ptr<const PublishedPage>      expected;
        std::shared_ptr<PageState>                leaf;
        std::shared_ptr<const LeafBase>           loaded;
        Status                                    status;
        size_t                                    next_base    = 0;
        size_t                                    pending_base = 0;
        bool                                      waiting      = false;
        std::atomic<bool>                         ready{false};
    };

    // Fixed-target split mode never scans or merges unrelated pages.
    std::shared_ptr<const PublishedTreeState> target_view;
    uint64_t                                  target_pid        = kInvalidPageId;
    uint64_t                                  target_commit_seq = 0;
    uint64_t                                  target_l1_seq     = 0;
    TreeWorkCause                             cause             = TreeWorkCause::Flush;
    std::atomic<bool>                         target_stopped{false};
    std::atomic<bool>                         target_started{false};

    ApplyDispatcher::Node                 cpu_node{&MaintenanceOperation::run, this};
    std::atomic<bool>                     scheduled{false};
    std::atomic<bool>                     wake_requested{false};
    std::shared_ptr<Cold>                 cold;
    bool                                  cold_started       = false;
    bool                                  cold_inner_started = false;
    size_t                                inner_merge_pairs  = 0;
    std::shared_ptr<MaintenanceFactory>   factory;
    std::shared_ptr<PageLoader>           loader;
    std::shared_ptr<MaintenanceOperation> lifetime;
    FlushBatch::MaintenanceComplete       done;
    MaintenancePublicationOptions         publication;
    using Candidates = std::vector<Candidate, ReservationAllocator<Candidate>>;
    std::shared_ptr<Candidates>         candidates;
    uint64_t                            generation;
    uint64_t                            invocation;
    uint64_t                            sequence = 0;
    uint64_t                            dirty;
    uint64_t                            scan_pid    = 0;
    uint64_t                            scan_limit  = 0;
    size_t                              visits      = 0;
    size_t                              steps       = 0;
    size_t                              index       = 0;
    Phase                               phase       = Phase::Collect;
    bool                                initialized = false;
    std::atomic<bool>                   finished{false};
    ResourceObservation                 queued_resources;
    ResourceObservation                 active_resources;
    bool                                metrics_started         = false;
    size_t                              activity_slot           = PublicationCoordinator::kMaintenanceSlots;
    uint64_t                            coalesce_input_runs     = 0;
    uint64_t                            coalesce_output_runs    = 0;
    uint64_t                            coalesce_input_entries  = 0;
    uint64_t                            coalesce_output_entries = 0;
    ReservedVector<uint64_t>            outputs;
    std::optional<TreeMetricsOperation> action_metrics;
};

[[nodiscard]] bool maintenance_needs_coalesce(const PageState &page, size_t max_sources);
} // namespace crowdb::tree::detail
