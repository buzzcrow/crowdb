// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/logical_page.h"

namespace crowdb::tree
{
struct PageMaintenanceLimits
{
    RunLimits                                      run;
    size_t                                         max_sources         = 64;
    size_t                                         max_base_views      = 64;
    size_t                                         max_output_pages    = 64;
    size_t                                         max_work_entries    = 65536;
    size_t                                         max_metadata_bytes  = 16 * 1024 * 1024;
    size_t                                         primary_page_bytes  = 64 * 1024;
    size_t                                         target_page_bytes   = 48 * 1024;
    size_t                                         merge_trigger_bytes = 16 * 1024;
    size_t                                         merge_target_bytes  = 40 * 1024;
    size_t                                         inline_value_bytes  = 8 * 1024;
    uint64_t                                       gc_floor            = 0;
    std::shared_ptr<detail::AllocationReservation> metadata            = nullptr;
    // Persistent range slices use Routing credit, independently of scratch.
};

struct LeafLayoutStats
{
    uint64_t kv_count = 0;
    uint64_t kv_bytes = 0;
};

enum class MaintenanceReadiness : uint8_t { kReady, kNeedsBase, kDeferred };

struct BaseLoadRequest
{
    std::shared_ptr<const PageState> page;
    size_t                           base_index = 0;
};

struct PageMaintenanceResult
{
    PageMaintenanceResult() = default;

    explicit PageMaintenanceResult(const std::shared_ptr<detail::AllocationReservation> &metadata)
        : loads(detail::ReservationAllocator<BaseLoadRequest>(metadata))
    {
    }

    Status                                  status;
    MaintenanceReadiness                    readiness = MaintenanceReadiness::kReady;
    detail::ReservedVector<BaseLoadRequest> loads;
    std::shared_ptr<const PageState>        page;
    // Winning tombstones removed by a complete, base-free coalesce.
    size_t removed_primary_bytes = 0;
};

// This is an uncompressed primary-frame size plan. It does not allocate
// overflow chains, encode frames, persist data, or assert checkpoint readiness.
struct LeafLayoutPlan
{
    LeafLayoutPlan() = default;

    explicit LeafLayoutPlan(const std::shared_ptr<detail::AllocationReservation> &metadata)
        : loads(detail::ReservationAllocator<BaseLoadRequest>(metadata)),
          separators(detail::ReservationAllocator<OwnedKey>(metadata)),
          primary_bytes(detail::ReservationAllocator<size_t>(metadata))
    {
    }

    Status                                  status;
    MaintenanceReadiness                    readiness = MaintenanceReadiness::kReady;
    std::shared_ptr<const PageState>        source;
    detail::ReservedVector<BaseLoadRequest> loads;
    detail::ReservedVector<OwnedKey>        separators;
    detail::ReservedVector<size_t>          primary_bytes;
    size_t                                  entry_count         = 0;
    size_t                                  total_primary_bytes = 0;
    size_t                                  overflow_values     = 0;
};

struct PageVersion
{
    uint64_t route_generation  = 0;
    uint64_t content_id        = 0;
    uint64_t representation_id = 0;
};

struct SplitTarget
{
    uint64_t    page_id = kInvalidPageId;
    PageVersion version;
};

struct LeafSplitResult
{
    LeafSplitResult() = default;

    explicit LeafSplitResult(const std::shared_ptr<detail::AllocationReservation> &metadata)
        : pages(detail::ReservationAllocator<std::shared_ptr<const PageState>>(metadata))
    {
    }

    Status                                                   status;
    MaintenanceReadiness                                     readiness = MaintenanceReadiness::kReady;
    detail::ReservedVector<std::shared_ptr<const PageState>> pages;
};

struct LeafMergeInput
{
    std::shared_ptr<const PageState> left;
    std::shared_ptr<const PageState> right;
    uint64_t                         left_page_id  = kInvalidPageId;
    uint64_t                         right_page_id = kInvalidPageId;
    uint64_t                         left_parent   = kInvalidPageId;
    uint64_t                         right_parent  = kInvalidPageId;
};

// All operations prepare immutable replacements. Publication and the complete
// parent/sibling/root transaction remain the caller's responsibility.
// Coalesce/merge consume at most max_sources consecutive run identities from
// first_run_id; the caller reserves that interval before dispatching workers.
PageMaintenanceResult coalesce_overlays(std::shared_ptr<const PageState> page, const PageMaintenanceLimits &limits,
                                        uint64_t representation_id, uint64_t first_run_id);
LeafLayoutPlan        plan_leaf_layout(std::shared_ptr<const PageState> page, const PageMaintenanceLimits &limits);
LeafSplitResult       split_leaf_ranges(const LeafLayoutPlan &layout, std::span<const SplitTarget> targets,
                                        const PageMaintenanceLimits &limits);
PageMaintenanceResult plan_leaf_merge(const LeafMergeInput &input, const PageMaintenanceLimits &limits,
                                      PageVersion version, uint64_t first_run_id);
// Joins already validated disjoint ranges without loading or materializing.
// The caller proves merge size/hysteresis from exact captured page metrics.
PageMaintenanceResult join_leaf_ranges(const LeafMergeInput &input, const PageMaintenanceLimits &limits,
                                       PageVersion version);
Status                accumulate_leaf_stats(Slice key, const LogicalCell &cell, LeafLayoutStats *stats);
} // namespace crowdb::tree
