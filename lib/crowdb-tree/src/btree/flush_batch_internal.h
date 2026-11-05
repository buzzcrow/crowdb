// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "apply_dispatcher.h"
#include "flush_batch.h"
#include "published_route.h"

namespace crowdb::tree
{
struct FlushBatch::State : std::enable_shared_from_this<State>
{
    struct RangeComplete
    {
        void *context;
        void (*complete)(void *, Status) noexcept;

        void operator()(Status status) const noexcept
        {
            complete(context, std::move(status));
        }
    };

    struct Subtree;
    void                     start_subtree();
    Status                   collect_subtree(PublicationTransaction &transaction);
    void                     start_load() noexcept;
    void                     start_leaves() noexcept;
    std::atomic<Subtree *>   leaf_waiters{nullptr};
    std::atomic<size_t>      active_leaves{0};
    std::atomic<Subtree *>   load_waiters{nullptr};
    std::atomic<bool>        load_active{false};
    std::shared_ptr<Subtree> subtree;
    std::atomic<uint64_t>    next_pid{0};
    std::atomic<size_t>      next_range{0};

    struct Range
    {
        size_t                                                 index           = 0;
        std::optional<detail::BoundedRangeJoin::Completion>    join_completion = std::nullopt;
        uint64_t                                               page_id;
        PageKeyRange                                           bounds;
        std::shared_ptr<const PublishedPage>                   expected;
        std::shared_ptr<InstallRecord>                         record;
        OverlayStorage                                         runs;
        detail::ReservedVector<std::shared_ptr<InstallRecord>> installs;
        std::optional<OwnedKey>                                next_key;
        std::shared_ptr<InstallRecord>                         prepared_record   = nullptr;
        std::shared_ptr<const PublishedPage>                   prepared_page     = nullptr;
        bool                                                   window_prepared   = false;
        bool                                                   probed            = false;
        size_t                                                 installed_runs    = 0;
        size_t                                                 next_install_at   = 0;
        uint64_t                                               installed_entries = 0;
        uint64_t                                               installed_bytes   = 0;
        uint64_t                                               entries           = 0;
        uint64_t                                               bytes             = 0;
        bool                                                   ready             = false;
        std::shared_ptr<TreeMetricsOperation>                  metrics           = nullptr;
    };

    struct DispatchScope
    {
        explicit DispatchScope(State &state);
        ~DispatchScope();
        static bool                        take(State &state) noexcept;
        State                             &owner;
        size_t                             remaining;
        DispatchScope                     *previous;
        static thread_local DispatchScope *active;
    };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<State> owner;
        void                   set_value() noexcept;
        void                   set_error(Status error) noexcept;
        void                   set_stopped() noexcept;
    };

    using JoinOperation = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<State> owner)
            : connected(stdexec::connect(sender, Receiver{std::move(owner)}))
        {
        }

        JoinOperation connected;
    };

    struct Work
    {
        Work(std::shared_ptr<State> state, Range &range, RangeComplete completion)
            : owner(std::move(state)),
              range(range),
              done(std::move(completion)),
              node(&Work::execute, this)
        {
        }

        std::shared_ptr<State> owner;
        Range                 &range;
        RangeComplete          done;
        ApplyDispatcher::Node  node;
        static void            execute(void *context) noexcept;
    };

    struct PlanWork
    {
        PlanWork(std::shared_ptr<State> state, Status result, std::shared_ptr<const InnerPageState> inner, bool resume)
            : owner(std::move(state)),
              status(std::move(result)),
              loaded(std::move(inner)),
              resume(resume),
              node(&PlanWork::execute, this)
        {
        }

        std::shared_ptr<State>                owner;
        Status                                status;
        std::shared_ptr<const InnerPageState> loaded;
        bool                                  resume = false;
        ApplyDispatcher::Node                 node;
        static void                           execute(void *context) noexcept;
    };

    State(PublicationCoordinator &publish, detail::FlushRuntime &cpu,
          std::shared_ptr<detail::AllocationReservation> credit)
        : publisher(publish),
          runtime(cpu),
          metadata(std::move(credit)),
          ranges(detail::ReservationAllocator<Range>(metadata))
    {
    }

    bool retire_step() noexcept
    {
        for (size_t step = 0; step < 64 && !ranges.empty(); ++step) {
            auto &range = ranges.back();
            if (!range.runs.empty()) {
                range.runs.pop_back();
            }
            else if (!range.installs.empty()) {
                range.installs.pop_back();
            }
            else {
                ranges.pop_back();
            }
        }
        return ranges.empty();
    }

    void schedule_plan(Status status = Status::Ok(), std::shared_ptr<const InnerPageState> loaded = {},
                       bool resume = false);
    void plan_step(std::shared_ptr<const InnerPageState> loaded, bool resume);
    std::optional<OwnedKey> first_input(std::optional<Slice> lower);
    void                    start_handoff();
    void                    acquire_route();
    void                    start(detail::CallbackComplete callback, void *argument);
    void                    start_range(size_t index, detail::BoundedRangeJoin::Completion done);
    void                    start_range(Range &range, RangeComplete done);
    Status                  prepare_run(Range &range, bool inline_probe);
    void                    finish_range(Range &range, RangeComplete done, Status status = Status::Ok()) noexcept;
    void                    execute_range(Range &range, RangeComplete done, bool inline_probe) noexcept;
    void                    install(Range &range, RangeComplete done, bool inline_probe);
    void                    joined(Status status) noexcept;
    void                    cover();
    void                    maintain() noexcept;
    void                    do_maintain();
    void                    finish() noexcept;

    PublicationCoordinator                                 &publisher;
    detail::FlushRuntime                                   &runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>             lane;
    std::shared_ptr<const PublishedTreeState>               captured;
    std::shared_ptr<const PublishedSources>                 inputs;
    std::shared_ptr<detail::AllocationReservation>          metadata;
    uint64_t                                                admission_owner = 0;
    std::vector<Range, detail::ReservationAllocator<Range>> ranges;
    detail::ReservedUnique<PublishedRouteCursor>            route;
    std::optional<OwnedKey>                                 plan_key;
    bool                                                    planned = false;
    std::shared_ptr<InstallRecord>                          coverage_record;
    std::shared_ptr<InstallRecord>                          routing_record;
    FlushBatchOptions                                       options;
    Maintenance                                             maintenance;
    FlushResult                                             report;
    static constexpr uint8_t                                kRunning = 1;
    static constexpr uint8_t                                kStopped = 2;
    static constexpr uint8_t                                kStarted = 4;
    std::atomic<uint8_t>                                    lifecycle{0};
    std::atomic<bool>                                       terminal{false};
    std::atomic<uint64_t>                                   maintenance_generation{0};
    std::optional<TreeMetricsOperation>                     batch_metrics;
    std::optional<TreeMetricsOperation>                     handoff_metrics;
    std::optional<TreeMetricsOperation>                     maintenance_metrics;
    detail::ReservedUnique<detail::BoundedRangeJoin>        join;
    detail::ReservedUnique<Attempt>                         operation;
    detail::CallbackComplete                                complete = nullptr;
    void                                                   *context  = nullptr;
};
} // namespace crowdb::tree
