// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_publication_internal.h"

#include <new>

namespace crowdb::tree
{
namespace
{
using detail::MaintenanceBuilder;
using detail::MaintenanceChildren;
using detail::MaintenancePath;
using detail::MaintenancePathEntry;

template <class Prepare>
MaintenancePublicationResult prepare(const MaintenancePublicationOptions &options, Prepare &&operation)
{
    MaintenancePublicationResult result(options.metadata);
    try {
        result.status = operation(&result);
    }
    catch (const std::bad_alloc &) {
        result.status    = Status::resource_exhausted();
        result.readiness = MaintenanceReadiness::kDeferred;
    }
    if (!result.status.ok()) {
        result.transaction = {};
        result.output_leaf_pids.clear();
        if (result.status.code() == Code::kResourceExhausted) {
            result.readiness = MaintenanceReadiness::kDeferred;
        }
    }
    return result;
}

Status coalesce(MaintenanceBuilder &builder, uint64_t pid, uint64_t first_run_id, MaintenancePublicationResult *result)
{
    MaintenancePath path{detail::ReservationAllocator<MaintenancePathEntry>(builder.options.metadata)};
    auto            status = builder.path_to(pid, &path);
    if (!status.ok()) {
        return status;
    }
    const auto old = builder.view->mapping.get(pid);
    if (old->leaf->representation_id == UINT64_MAX) {
        return Status::resource_exhausted("coalesce representation identity exhausted");
    }
    auto limits       = builder.options.pages;
    limits.gc_floor   = builder.view->gc_floor;
    auto compacted    = coalesce_overlays(old->leaf, limits, old->leaf->representation_id + 1, first_run_id);
    result->readiness = compacted.readiness;
    result->loads     = std::move(compacted.loads);
    if (!compacted.status.ok() || !compacted.page) {
        return compacted.status;
    }
    status = builder.make_leaf(pid, old, std::move(compacted.page), std::nullopt, {}, nullptr,
                               compacted.removed_primary_bytes);
    if (status.ok()) {
        status = builder.guard_path(path);
    }
    if (status.ok()) {
        result->output_leaf_pids = {pid};
        status                   = builder.finish(result);
    }
    return status;
}

Status split(MaintenanceBuilder &builder, uint64_t pid, MaintenancePublicationResult *result)
{
    MaintenancePath path{detail::ReservationAllocator<MaintenancePathEntry>(builder.options.metadata)};
    auto            status = builder.path_to(pid, &path);
    if (!status.ok()) {
        return status;
    }
    const auto old    = builder.view->mapping.get(pid);
    auto       layout = plan_leaf_layout(old->leaf, builder.options.pages);
    result->readiness = layout.readiness;
    result->loads     = layout.loads;
    if (!layout.status.ok() || layout.readiness != MaintenanceReadiness::kReady) {
        return layout.status;
    }
    if (layout.primary_bytes.size() == 1) {
        return Status::Ok();
    }
    PageVersion version;
    status = detail::next_page_version(*old->leaf, nullptr, &version);
    if (!status.ok()) {
        return status;
    }
    detail::ReservedVector<SplitTarget> targets{detail::ReservationAllocator<SplitTarget>(builder.options.metadata)};
    targets.reserve(layout.primary_bytes.size());
    for (size_t i = 0; i < layout.primary_bytes.size(); ++i) {
        uint64_t output = pid;
        if (i != 0) {
            status = builder.allocate_pid(&output);
            if (!status.ok()) {
                return status;
            }
        }
        targets.push_back({.page_id = output, .version = version});
    }
    auto leaves       = split_leaf_ranges(layout, targets, builder.options.pages);
    result->readiness = leaves.readiness;
    if (!leaves.status.ok()) {
        return leaves.status;
    }
    MaintenanceChildren children(builder.options.metadata);
    children.separators.assign(layout.separators.begin(), layout.separators.end());
    for (size_t i = 0; i < targets.size(); ++i) {
        status = builder.make_leaf(targets[i].page_id, old, std::move(leaves.pages[i]), layout.primary_bytes[i]);
        if (!status.ok()) {
            return status;
        }
        children.pids.push_back(targets[i].page_id);
    }
    result->output_leaf_pids = children.pids;
    status                   = builder.propagate(path, 1, std::move(children), 0);
    if (status.ok()) {
        status = builder.guard_path(path);
    }
    return status.ok() ? builder.finish(result) : status;
}

Status merge(MaintenanceBuilder &builder, uint64_t left_pid, uint64_t right_pid, uint64_t first_run_id,
             MaintenancePublicationResult *result)
{
    MaintenancePath path{detail::ReservationAllocator<MaintenancePathEntry>(builder.options.metadata)};
    auto            status = builder.path_to(left_pid, &path);
    if (!status.ok()) {
        return status;
    }
    const auto left  = builder.view->mapping.get(left_pid);
    const auto right = builder.view->mapping.get(right_pid);
    if (path.empty() || !right || !right->leaf) {
        return Status::invalid_argument("merge requires two leaves with a common parent");
    }
    const auto &parent = path.back();
    if (parent.child + 1 >= parent.page->inner->children.size() ||
        parent.page->inner->children[parent.child + 1] != right_pid) {
        return Status::invalid_argument("merge leaves must be adjacent children of one parent");
    }
    MaintenancePath right_path{detail::ReservationAllocator<MaintenancePathEntry>(builder.options.metadata)};
    status = builder.path_to(right_pid, &right_path);
    if (!status.ok()) {
        return status;
    }
    if (right_path.empty() || right_path.back().pid != parent.pid || right_path.back().child != parent.child + 1) {
        return Status::invalid_argument("merge right leaf has a different route");
    }
    PageVersion version;
    status = detail::next_page_version(*left->leaf, right->leaf.get(), &version);
    if (!status.ok()) {
        return status;
    }
    auto merged       = detail::plan_published_leaf_merge(*left, *right,
                                                          {.left          = left->leaf,
                                                           .right         = right->leaf,
                                                           .left_page_id  = left_pid,
                                                           .right_page_id = right_pid,
                                                           .left_parent   = parent.pid,
                                                           .right_parent  = parent.pid},
                                                          builder.options.pages, version, first_run_id);
    result->readiness = merged.logical.readiness;
    result->loads     = std::move(merged.logical.loads);
    if (!merged.logical.status.ok() || !merged.logical.page || result->readiness != MaintenanceReadiness::kReady) {
        return merged.logical.status;
    }
    status = builder.make_leaf(left_pid, left, std::move(merged.logical.page), merged.primary_bytes,
                               right->allocation_owner, merged.statistics ? &*merged.statistics : nullptr);
    if (status.ok()) {
        status = builder.replace(right_pid, {});
    }
    if (status.ok()) {
        MaintenanceChildren children(builder.options.metadata);
        children.pids.push_back(left_pid);
        status = builder.propagate(path, 2, std::move(children), 0);
    }
    if (status.ok()) {
        bool changed = false;
        status       = builder.merge_underfull(path, &changed);
    }
    if (status.ok()) {
        status = builder.shrink_root();
    }
    if (status.ok()) {
        status = builder.guard_path(path);
    }
    if (status.ok()) {
        result->output_leaf_pids = {left_pid};
        status                   = builder.finish(result);
    }
    return status;
}
} // namespace

MaintenancePublicationResult build_coalesce_publication(std::shared_ptr<const PublishedTreeState> view,
                                                        uint64_t leaf_pid, std::shared_ptr<InstallRecord> record,
                                                        uint64_t                             first_run_id,
                                                        const MaintenancePublicationOptions &options)
{
    return prepare(options, [&](auto *result) {
        MaintenanceBuilder builder(std::move(view), std::move(record), options);
        auto               status = builder.validate();
        return status.ok() ? coalesce(builder, leaf_pid, first_run_id, result) : status;
    });
}

MaintenancePublicationResult build_split_publication(std::shared_ptr<const PublishedTreeState> view, uint64_t leaf_pid,
                                                     std::shared_ptr<InstallRecord>       record,
                                                     const MaintenancePublicationOptions &options)
{
    return prepare(options, [&](auto *result) {
        MaintenanceBuilder builder(std::move(view), std::move(record), options);
        auto               status = builder.validate();
        return status.ok() ? split(builder, leaf_pid, result) : status;
    });
}

MaintenancePublicationResult build_merge_publication(std::shared_ptr<const PublishedTreeState> view, uint64_t left_pid,
                                                     uint64_t right_pid, std::shared_ptr<InstallRecord> record,
                                                     uint64_t                             first_run_id,
                                                     const MaintenancePublicationOptions &options)
{
    return prepare(options, [&](auto *result) {
        MaintenanceBuilder builder(std::move(view), std::move(record), options);
        auto               status = builder.validate();
        return status.ok() ? merge(builder, left_pid, right_pid, first_run_id, result) : status;
    });
}

MaintenancePublicationResult build_inner_merge_publication(std::shared_ptr<const PublishedTreeState> view,
                                                           uint64_t leaf_pid, std::shared_ptr<InstallRecord> record,
                                                           const MaintenancePublicationOptions &options)
{
    return prepare(options, [&](auto *result) {
        MaintenanceBuilder builder(std::move(view), std::move(record), options);
        auto               status = builder.validate();
        MaintenancePath    path{detail::ReservationAllocator<MaintenancePathEntry>(options.metadata)};
        if (status.ok()) {
            status = builder.path_to(leaf_pid, &path);
        }
        bool changed = false;
        if (status.ok()) {
            status = builder.merge_underfull(path, &changed);
        }
        if (!status.ok() || !changed) {
            return status;
        }
        status = builder.shrink_root();
        if (status.ok()) {
            status = builder.guard_path(path);
        }
        return status.ok() ? builder.finish(result) : status;
    });
}
} // namespace crowdb::tree
