// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "flush_batch.h"
#include "maintenance_publication.h"

namespace crowdb::tree
{
struct PageMaintenanceOperationOptions
{
    using AcquireReservation = detail::InlineFunction<Status(std::shared_ptr<detail::AllocationReservation> *)>;
    using AcquireWindow      = detail::InlineFunction<Status(const MappingView &, MaintenancePublicationOptions *,
                                                             std::shared_ptr<detail::AllocationReservation> *)>;

    MaintenancePublicationOptions publication;
    size_t                        visits_per_step   = 64;
    size_t                        max_visits        = 4096;
    size_t                        max_candidates    = 256;
    size_t                        max_steps         = 1024;
    size_t                        max_stale_replans = 2;
    // Optional credit acquired before source closure, consumed by the first
    // invocation. Later idle/retry calls use acquire for a fresh reservation.
    std::shared_ptr<detail::AllocationReservation> prepaid;
    // Nonblocking acquisition of a fresh reclaim reservation when needed.
    // publication.metadata must be empty. Handoff credit is never reused.
    AcquireReservation acquire;
    // Acquired before Reclaim. The persistent journal must not retain the
    // working partition needed to release another accepted replay payload.
    AcquireReservation acquire_snapshot;
    // Later invocations may select a larger or smaller window as pressure
    // changes. Acquire both partitions for the supplied fixed mapping before
    // changing its output count; publication metadata remains independently owned.
    AcquireWindow acquire_window;
};

// Control, traversal, one complete logical maintenance transaction and its
// publication. Later bounded steps may stop when this reservation is spent.
// New handoff runs are included before they appear in the current mapping.
[[nodiscard]] std::optional<size_t> page_maintenance_reserve_bound(const MappingView &mapping, size_t new_runs,
                                                                   const PageMaintenanceOperationOptions &options,
                                                                   bool track_residency          = false,
                                                                   bool separate_snapshot_credit = false) noexcept;
[[nodiscard]] std::optional<size_t>
page_maintenance_snapshot_reserve_bound(const MappingView                     &mapping,
                                        const PageMaintenanceOperationOptions &options) noexcept;

// Choose the largest output window whose complete prepaid bounds fit. Keep
// the minimum two-range window when pressure requires admission to wait.
void limit_page_maintenance_window(const MappingView &mapping, size_t new_runs, size_t reclaim_bytes,
                                   size_t snapshot_bytes, bool track_residency,
                                   PageMaintenanceOperationOptions *options) noexcept;

// One invocation per factory may run at a time. Busy is reported explicitly.
// The callback also supports idle maintenance without a new flush. Runtime and
// publisher outlive all callbacks. This stage does no I/O or writeback: its
// deferred count is the live dirty-page count, even after structural progress.
[[nodiscard]] FlushBatch::Maintenance make_page_maintenance(PublicationCoordinator                     &publisher,
                                                            detail::FlushRuntime                       &runtime,
                                                            std::shared_ptr<detail::FlushRuntime::Lane> lane,
                                                            PageMaintenanceOperationOptions             options);
// Automatic handoff can lend its prepaid asynchronous load window until this
// maintenance callback completes. At most one cold leaf is resolved per call.
[[nodiscard]] FlushBatch::Maintenance
make_page_maintenance(PublicationCoordinator &publisher, detail::FlushRuntime &runtime,
                      std::shared_ptr<detail::FlushRuntime::Lane> lane, PageMaintenanceOperationOptions options,
                      std::shared_ptr<PageLoader> loader, std::shared_ptr<ApplyDispatcher> progress);
} // namespace crowdb::tree
