// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "apply_dispatcher.h"
#include "flush_operation.h"

namespace crowdb::tree
{
struct FlushOperation::State : std::enable_shared_from_this<State>
{
    enum class Stage : uint8_t { kDrain, kBatch };
    enum class CpuStage : uint8_t { kBegin, kDrain, kBatch };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<State> owner;
        Stage                  stage;
        void                   set_value() noexcept;
        void                   set_error(Status status) noexcept;
        void                   set_stopped() noexcept;
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<State> owner, Stage stage)
            : connected(stdexec::connect(sender, Receiver{.owner = std::move(owner), .stage = stage}))
        {
        }

        Connected connected;
    };

    State(PublicationCoordinator &publish, detail::FlushRuntime &cpu, SourceAdmission &admission,
          const std::atomic<uint64_t> &frontier, std::shared_ptr<detail::FlushRuntime::Lane> tree_lane,
          FlushBatchOptions bounds, FlushBatch::Maintenance maintain)
        : publisher(publish),
          runtime(cpu),
          sources(admission),
          contiguous(frontier),
          lane(std::move(tree_lane)),
          options(std::move(bounds)),
          maintenance(std::move(maintain))
    {
    }

    bool        start(detail::CallbackComplete callback, void *argument, FlushReportComplete report_callback = nullptr,
                      bool only_capture = false, bool reject_running = true) noexcept;
    void        dispatch(CpuStage stage) noexcept;
    static void run_cpu(void *context) noexcept;
    void        begin();
    void        acquire_admission();
    void        release_admission(Status status);
    void        fail_terminal(Status status) noexcept;
    void        publish_successor();
    void        drain_sources();
    void        start_batch();
    void        stage_done(Stage stage, Status status) noexcept;
    void        maintain(std::shared_ptr<const PublishedTreeState> view, FlushBatch::MaintenanceComplete done);
    Status      acknowledge(const PublishedTreeState &view);
    void        fail(Status status) noexcept;
    void        finish() noexcept;
    [[nodiscard]] bool stopped() const noexcept;

    PublicationCoordinator                        &publisher;
    detail::FlushRuntime                          &runtime;
    SourceAdmission                               &sources;
    const std::atomic<uint64_t>                   &contiguous;
    std::shared_ptr<detail::FlushRuntime::Lane>    lane;
    std::shared_ptr<ApplyProgress>                 progress;
    FlushBatchOptions                              options;
    FlushBatch::Maintenance                        maintenance;
    std::shared_ptr<detail::AllocationReservation> metadata;
    detail::FlushReservations                      reservations;
    std::shared_ptr<const SourceCatalog>           expected_catalog;
    std::shared_ptr<SourcePause>                   source_pause;
    uint64_t                                       admission_owner     = 0;
    uint64_t                                       expected_commit_seq = 0;
    bool                                           admitted            = false;
    std::shared_ptr<detail::AllocationReservation> release_metadata;
    bool                                           admission_released = false;
    std::shared_ptr<InstallRecord>                 admission_record;
    std::shared_ptr<PublicationRequest>            release_request;
    std::atomic<bool>                              replaceable{false};
    std::shared_ptr<SourceCapture>                 capture;
    FrozenSourceView                               frozen;
    bool                                           capture_only = false;
    std::shared_ptr<FlushBatch>                    batch;
    std::shared_ptr<InstallRecord>                 source_record;
    std::shared_ptr<const PublishedSources>        expected_sources;
    std::shared_ptr<const PublishedSources>        published_sources;
    std::optional<Attempt>                         drain_attempt;
    std::optional<Attempt>                         batch_attempt;
    std::shared_ptr<State>                         queued_owner;
    ApplyDispatcher::Node                          cpu_node{&State::run_cpu, this};
    CpuStage                                       queued_stage = CpuStage::kBegin;
    std::atomic<FlushBatch *>                      cancel_batch{nullptr};
    std::atomic<bool>                              complete_success{false};
    bool                                           source_acknowledged = false;
    static constexpr uint8_t                       kRunning            = 1;
    static constexpr uint8_t                       kStopped            = 2;
    std::atomic<uint8_t>                           lifecycle{0};
    detail::CallbackComplete                       complete         = nullptr;
    void                                          *context          = nullptr;
    FlushReportComplete                            report_complete  = nullptr;
    bool                                           covered_at_start = false;
    FlushResult                                    report;
    std::optional<TreeMetricsOperation>            batch_metrics;
    std::optional<TreeMetricsOperation>            capture_metrics;
    std::optional<TreeMetricsOperation>            drain_metrics;
    std::optional<TreeMetricsOperation>            repair_metrics;
    uint64_t                                       captured_at_ns = 0;
};
} // namespace crowdb::tree
