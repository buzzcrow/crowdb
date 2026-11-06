// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "flush_operation.h"
#include "source_pause.h"

namespace crowdb::tree
{
struct FlushController::State
{
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

    Status                      pause_source(const SourceCatalog &catalog);
    [[nodiscard]] bool          try_enter() noexcept;
    void                        leave() noexcept;
    [[nodiscard]] bool          has_pending() const noexcept;
    [[nodiscard]] static size_t control_allocation_bound() noexcept;

    Status prepare_covered(const FlushPreparationOptions                   &preparation,
                           const std::shared_ptr<const PublishedTreeState> &view,
                           const std::shared_ptr<const SourceCatalog> &catalog, std::shared_ptr<FlushOperation> *out);

    PublicationCoordinator                        &publisher;
    detail::FlushRuntime                          &runtime;
    SourceAdmission                               &sources;
    const std::atomic<uint64_t>                   &contiguous;
    std::shared_ptr<detail::FlushRuntime::Lane>    lane;
    std::shared_ptr<ApplyProgress>                 progress;
    FlushBatchOptions                              options;
    FlushBatch::Maintenance                        maintenance;
    std::atomic<bool>                              closed{false};
    std::atomic_flag                               preparing = ATOMIC_FLAG_INIT;
    std::atomic<bool>                              preparing_waiter{false};
    std::shared_ptr<FlushOperation>                pending;
    std::shared_ptr<detail::AllocationReservation> lifecycle;
    std::shared_ptr<SourcePause>                   pause;
};

} // namespace crowdb::tree
