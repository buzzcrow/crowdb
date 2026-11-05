// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_maintenance.h"
#include "btree/published_state.h"

namespace crowdb::tree
{
struct MaintenancePublicationOptions
{
    PageMaintenanceLimits                          pages;
    size_t                                         max_inner_children = 257;
    size_t                                         max_inner_bytes    = 64 * 1024;
    size_t                                         inner_merge_keys   = 64;
    size_t                                         max_path_pages     = 64;
    size_t                                         max_changed_pages  = 256;
    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<detail::AllocationReservation> snapshot_metadata = nullptr;
};

struct MaintenancePublicationResult
{
    MaintenancePublicationResult() = default;

    explicit MaintenancePublicationResult(const std::shared_ptr<detail::AllocationReservation> &metadata)
        : loads(detail::ReservationAllocator<BaseLoadRequest>(metadata)),
          transaction{.pages = PublicationTransaction::Pages(detail::ReservationAllocator<PageReplacement>(metadata)),
                      .metadata = metadata},
          output_leaf_pids(detail::ReservationAllocator<uint64_t>(metadata))
    {
    }

    Status                                  status;
    MaintenanceReadiness                    readiness = MaintenanceReadiness::kReady;
    detail::ReservedVector<BaseLoadRequest> loads;
    PublicationTransaction                  transaction;
    detail::ReservedVector<uint64_t>        output_leaf_pids;
    size_t                                  merged_inner_pairs = 0;

    // A ready result may be a no-op (one page already fits or merge hysteresis).
    [[nodiscard]] bool changed() const noexcept
    {
        return transaction.record != nullptr;
    }
};

// Preparation only. All expected pages, ancestor route guards and the root
// precondition publish as one transaction. A stale result must be replanned;
// it cannot be split into child transactions. Cold data is returned as loads.
MaintenancePublicationResult build_coalesce_publication(std::shared_ptr<const PublishedTreeState> view,
                                                        uint64_t leaf_pid, std::shared_ptr<InstallRecord> record,
                                                        uint64_t                             first_run_id,
                                                        const MaintenancePublicationOptions &options = {});
MaintenancePublicationResult build_split_publication(std::shared_ptr<const PublishedTreeState> view, uint64_t leaf_pid,
                                                     std::shared_ptr<InstallRecord>       record,
                                                     const MaintenancePublicationOptions &options = {});

// A bounded window becomes legal primary-page ranges in one transaction. An
// unvisited suffix keeps unknown size and remains a split candidate. Planning
// shares payloads and does not encode a frame or collect the complete large leaf.
MaintenancePublicationResult build_split_step_publication(std::shared_ptr<const PublishedTreeState> view,
                                                          uint64_t leaf_pid, std::shared_ptr<InstallRecord> record,
                                                          const MaintenancePublicationOptions &options = {});
MaintenancePublicationResult build_merge_publication(std::shared_ptr<const PublishedTreeState> view, uint64_t left_pid,
                                                     uint64_t right_pid, std::shared_ptr<InstallRecord> record,
                                                     uint64_t                             first_run_id,
                                                     const MaintenancePublicationOptions &options = {});

// Repair underfull routing pages on one bounded leaf route. Adjacent siblings,
// ancestor updates and root contraction are guarded by the same publication.
MaintenancePublicationResult build_inner_merge_publication(std::shared_ptr<const PublishedTreeState> view,
                                                           uint64_t leaf_pid, std::shared_ptr<InstallRecord> record,
                                                           const MaintenancePublicationOptions &options = {});
} // namespace crowdb::tree
