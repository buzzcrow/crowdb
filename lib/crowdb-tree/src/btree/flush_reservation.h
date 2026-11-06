// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/local/block_account.h"
#include "maintenance_operation.h"
#include "page_load.h"
#include "source_admission.h"

namespace crowdb::tree
{
class SharedPageCache;

struct FlushPreparationOptions
{
    bool                                  persistent  = false;
    std::shared_ptr<FlushPersistence>     persistence = nullptr;
    std::shared_ptr<detail::FlushRuntime> runtime;
    std::shared_ptr<AsyncPageStore>       store;
    PageLoadLimits                        loads;
    PageMaintenanceOperationOptions       maintenance;
    std::shared_ptr<SharedPageCache>      cache        = nullptr;
    std::shared_ptr<ApplyDispatcher>      cpu_progress = nullptr;
    std::shared_ptr<PublicationLane>      publication  = nullptr;
};

namespace detail
{
struct FlushReserveBounds
{
    static constexpr std::array categories{BudgetClass::kHandoff, BudgetClass::kRouting, BudgetClass::kReclaim};
    std::array<size_t, categories.size()> amounts{};
    size_t                                backend_bytes            = 0;
    size_t                                snapshot_bytes           = 0;
    size_t                                maintenance_output_pages = 0;
};

using FlushCredits = std::array<ResourceBudget::Credit, FlushReserveBounds::categories.size()>;

struct FlushReservations
{
    std::shared_ptr<AllocationBudget>      backend;
    std::shared_ptr<SharedPageCache>       cache;
    std::shared_ptr<AllocationReservation> handoff;
    std::shared_ptr<AllocationReservation> routing;
    std::shared_ptr<AllocationReservation> reclaim;
    std::shared_ptr<AllocationReservation> snapshot;

    void seal() const noexcept
    {
        seal_backend_budget(backend);
        for (const auto &credit : {handoff, routing, reclaim, snapshot}) {
            if (credit) {
                credit->seal();
            }
        }
    }
};

// Derive the complete fixed-state bound without acquiring any resources.
Status flush_reserve_bounds(PublicationCoordinator &publisher, const PublishedTreeState &view,
                            const SourceCatalog &sources, const FlushPreparationOptions &preparation,
                            size_t control_bytes, const FlushBatchOptions &batch, FlushReserveBounds *out,
                            std::optional<WriteFootprint> stable_active = {}, bool add_successor = false);
// The caller transfers complete grants directly from asynchronous admission;
// loader, backend and maintenance construction never acquire the same credit again.
Status bind_flush_reservations(PublicationCoordinator &publisher, const FlushPreparationOptions &preparation,
                               const std::shared_ptr<ResourceBudget::Account> &account,
                               std::shared_ptr<FlushRuntime::Lane> lane, const FlushReserveBounds &bounds,
                               FlushCredits credits, FlushBatchOptions *batch, FlushBatch::Maintenance *maintenance,
                               FlushReservations *out);

// All-or-rollback admission: no source is closed and no publication gate is
// taken while these independent partitions are being acquired/prepared.
Status prepare_flush_reservations(PublicationCoordinator &publisher, const PublishedTreeState &view,
                                  const SourceCatalog &sources, const FlushPreparationOptions &preparation,
                                  size_t control_bytes, std::shared_ptr<FlushRuntime::Lane> lane,
                                  FlushBatchOptions *batch, FlushBatch::Maintenance *maintenance,
                                  FlushReservations *out, std::optional<WriteFootprint> stable_active = {});
} // namespace detail
} // namespace crowdb::tree
