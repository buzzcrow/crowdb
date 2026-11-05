// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_reservation.h"
#include "btree/maintenance_publication.h"

#include <map>

namespace crowdb::tree::detail
{
struct MaintenancePathEntry
{
    uint64_t                             pid   = 0;
    size_t                               child = 0;
    std::shared_ptr<const PublishedPage> page;
};

using MaintenancePath = ReservedVector<MaintenancePathEntry>;

struct MaintenanceChildren
{
    explicit MaintenanceChildren(const std::shared_ptr<AllocationReservation> &metadata)
        : pids(ReservationAllocator<uint64_t>(metadata)),
          separators(ReservationAllocator<OwnedKey>(metadata))
    {
    }

    ReservedVector<uint64_t> pids;
    ReservedVector<OwnedKey> separators;
};

class MaintenanceBuilder
{
  public:
    MaintenanceBuilder(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<InstallRecord> record,
                       const MaintenancePublicationOptions &limits, InlineFunction<Status(uint64_t *)> allocate = {});
    [[nodiscard]] Status validate() const;
    Status               path_to(uint64_t pid, MaintenancePath *path) const;
    Status               guard_path(const MaintenancePath &path);
    Status               allocate_pid(uint64_t *pid);
    Status               replace(uint64_t pid, std::shared_ptr<const PublishedPage> replacement);
    Status               propagate(const MaintenancePath &path, size_t removed_children, MaintenanceChildren children,
                                   uint32_t child_level);
    Status               shrink_root();
    Status               merge_underfull(const MaintenancePath &path, bool *changed);
    Status make_leaf(uint64_t pid, const std::shared_ptr<const PublishedPage> &old,
                     std::shared_ptr<const PageState> logical, std::optional<size_t> exact_bytes,
                     std::shared_ptr<const void> second_owner = {}, const LeafLayoutStats *stats = nullptr,
                     size_t removed_primary_bytes = 0);
    Status finish(MaintenancePublicationResult *result);
    [[nodiscard]] std::shared_ptr<const PublishedPage> lookup(uint64_t pid) const;

    const std::shared_ptr<const PublishedTreeState> view;
    MaintenancePublicationOptions                   options;

    Status partition_inner(uint64_t pid, uint32_t level, const InnerPageState &inner, MaintenanceChildren *output);
    Status grow_root(MaintenanceChildren children, uint32_t child_level);

    [[nodiscard]] uint64_t root_page_id() const noexcept
    {
        return root_pid_;
    }

  private:
    Status merge_inner_pair(const MaintenancePathEntry &ancestor, uint64_t target, bool *changed);
    InlineFunction<Status(uint64_t *)> allocate_;
    std::shared_ptr<InstallRecord>     record_;
    using Change = std::pair<const uint64_t, PageReplacement>;
    std::map<uint64_t, PageReplacement, std::less<>, ReservationAllocator<Change>> changes_;
    uint64_t                                                                       next_pid_;
    uint64_t                                                                       root_pid_;
    size_t                                                                         inner_merges_ = 0;
};

Status build_flush_leaf_outputs(MaintenanceBuilder &builder, uint64_t pid,
                                const std::shared_ptr<const PublishedPage> &old, const struct SplitWindowLayout &layout,
                                MaintenanceChildren *children);

Status next_page_version(const PageState &left, const PageState *right, PageVersion *version);
Status summarize_leaf(const std::shared_ptr<const PageState> &page, const PageMaintenanceLimits &limits,
                      PageMetricsContribution *metrics);

struct MaintenanceLeafMerge
{
    PageMaintenanceResult          logical;
    size_t                         primary_bytes = 0;
    std::optional<LeafLayoutStats> statistics;
};

MaintenanceLeafMerge plan_published_leaf_merge(const PublishedPage &left, const PublishedPage &right,
                                               const LeafMergeInput &input, const PageMaintenanceLimits &limits,
                                               PageVersion version, uint64_t first_run_id);
} // namespace crowdb::tree::detail
