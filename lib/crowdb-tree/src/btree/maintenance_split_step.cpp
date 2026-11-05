// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_publication_internal.h"
#include "maintenance_split_layout.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
void clip_bases(PageState &page)
{
    for (auto &base : page.bases) {
        if (page.range.lower && (!base.range.lower || *page.range.lower > *base.range.lower)) {
            base.range.lower = page.range.lower;
        }
        if (page.range.upper && (!base.range.upper || *page.range.upper < *base.range.upper)) {
            base.range.upper = page.range.upper;
        }
    }
    std::erase_if(page.bases, [](const auto &base) { return base.range.empty(); });
    std::erase_if(page.overlays, [&](const auto &run) {
        const size_t first = page.range.lower ? run->lower_bound(*page.range.lower) : 0;
        const size_t last  = page.range.upper ? run->lower_bound(*page.range.upper) : run->entries().size();
        return first == last;
    });
}

void count_runs(PublishedPage &page)
{
    page.metrics.overlay_runs    = page.leaf->overlays.size();
    page.metrics.overlay_entries = 0;
    for (const auto &run : page.leaf->overlays) {
        const auto  &range = page.leaf->range;
        const size_t first = range.lower ? run->lower_bound(*range.lower) : 0;
        const size_t last  = range.upper ? run->lower_bound(*range.upper) : run->entries().size();
        page.metrics.overlay_entries += last - first;
    }
}

Status prepare_outputs(detail::MaintenanceBuilder &builder, const std::shared_ptr<const PublishedPage> &old,
                       const detail::MaintenanceChildren &children, const detail::SplitWindowLayout &layout,
                       PageVersion version)
{
    size_t prefix_bytes = 0;
    Status status;
    for (size_t i = 0; i < children.pids.size(); ++i) {
        auto logical =
            detail::reserved_shared<PageState>(builder.options.metadata, *old->leaf, builder.options.metadata);
        logical->range.lower = i == 0 ? old->leaf->range.lower : std::optional(children.separators[i - 1]);
        logical->range.upper =
            i == children.separators.size() ? old->leaf->range.upper : std::optional(children.separators[i]);
        logical->right_sibling     = i + 1 == children.pids.size() ? old->leaf->right_sibling : children.pids[i + 1];
        logical->route_generation  = version.route_generation;
        logical->content_id        = version.content_id;
        logical->representation_id = version.representation_id;
        clip_bases(*logical);
        if (i < layout.primary_bytes.size()) {
            status = builder.make_leaf(children.pids[i], old, std::move(logical), layout.primary_bytes[i], {},
                                       &layout.statistics[i]);
            prefix_bytes += layout.primary_bytes[i];
        }
        else {
            // This suffix was not traversed; its size/count remain unknown.
            auto tail      = detail::reserved_shared<PublishedPage>(builder.options.metadata, *old);
            tail->revision = 1;
            tail->leaf     = std::move(logical);
            tail->primary.reset();

            tail->metrics.page_size_known          = false;
            tail->metrics.kv_known                 = false;
            tail->metrics.split_candidate          = true;
            tail->metrics.split_ready              = false;
            tail->metrics.dirty                    = true;
            tail->metrics.writeback_candidate      = true;
            tail->metrics.page_size_estimate_bytes = old->metrics.page_size_estimate_bytes > prefix_bytes
                                                       ? old->metrics.page_size_estimate_bytes - prefix_bytes
                                                       : 0;
            count_runs(*tail);
            status = builder.replace(children.pids[i], std::move(tail));
        }
        if (!status.ok()) {
            return status;
        }
    }
    return Status::Ok();
}

Status prepare_step(detail::MaintenanceBuilder &builder, uint64_t pid, MaintenancePublicationResult *result,
                    const detail::SplitWindowLayout *prepared = nullptr)
{
    auto                    status = builder.validate();
    detail::MaintenancePath path{detail::ReservationAllocator<detail::MaintenancePathEntry>(builder.options.metadata)};
    if (status.ok()) {
        status = builder.path_to(pid, &path);
    }
    if (!status.ok()) {
        return status;
    }
    const auto                old = builder.view->mapping.get(pid);
    detail::SplitWindowLayout layout(builder.options.metadata);
    if (prepared != nullptr) {
        if (prepared->source != old->leaf || prepared->primary_bytes.empty() ||
            prepared->statistics.size() != prepared->primary_bytes.size() ||
            prepared->separators.size() > prepared->primary_bytes.size() ||
            prepared->primary_bytes.size() > prepared->separators.size() + 1) {
            return Status::invalid_argument("prepared split layout does not match its exact source");
        }
        layout = *prepared;
    }
    else {
        status = detail::plan_split_window(old->leaf, builder.options.pages, &layout, result);
    }
    if (!status.ok() || result->readiness != MaintenanceReadiness::kReady) {
        return status;
    }
    if (layout.separators.empty()) {
        if (old->metrics.page_size_known && old->metrics.kv_known && !old->metrics.split_candidate) {
            return Status::Ok();
        }
        status = builder.make_leaf(pid, old, old->leaf, layout.primary_bytes.front(), {}, &layout.statistics.front());
        if (status.ok()) {
            status = builder.guard_path(path);
        }
        if (status.ok()) {
            result->output_leaf_pids = {pid};
            status                   = builder.finish(result);
        }
        return status;
    }
    PageVersion version;
    status = detail::next_page_version(*old->leaf, nullptr, &version);
    if (!status.ok()) {
        return status;
    }
    detail::MaintenanceChildren children(builder.options.metadata);
    children.pids.push_back(pid);
    children.separators = std::move(layout.separators);
    for (size_t i = 0; i < children.separators.size(); ++i) {
        uint64_t output = 0;
        status          = builder.allocate_pid(&output);
        if (!status.ok()) {
            return status;
        }
        children.pids.push_back(output);
    }
    status = prepare_outputs(builder, old, children, layout, version);
    if (!status.ok()) {
        return status;
    }
    result->output_leaf_pids = children.pids;
    status                   = builder.propagate(path, 1, std::move(children), 0);
    if (status.ok()) {
        status = builder.guard_path(path);
    }
    if (status.ok()) {
        status = builder.finish(result);
    }
    return status;
}
} // namespace

MaintenancePublicationResult build_split_step_publication(std::shared_ptr<const PublishedTreeState> view,
                                                          uint64_t leaf_pid, std::shared_ptr<InstallRecord> record,
                                                          const MaintenancePublicationOptions &options)
{
    MaintenancePublicationResult result(options.metadata);
    try {
        detail::MaintenanceBuilder builder(std::move(view), std::move(record), options);
        result.status = prepare_step(builder, leaf_pid, &result);
    }
    catch (const std::bad_alloc &) {
        result.status = Status::resource_exhausted();
    }
    if (!result.status.ok()) {
        result.transaction = {};
        result.output_leaf_pids.clear();
        result.readiness = MaintenanceReadiness::kDeferred;
    }
    return result;
}
} // namespace crowdb::tree

namespace crowdb::tree::detail
{
MaintenancePublicationResult build_split_layout_publication(std::shared_ptr<const PublishedTreeState> view,
                                                            uint64_t leaf_pid, std::shared_ptr<InstallRecord> record,
                                                            const MaintenancePublicationOptions &options,
                                                            const SplitWindowLayout             &layout)
{
    MaintenancePublicationResult result(options.metadata);
    try {
        MaintenanceBuilder builder(std::move(view), std::move(record), options);
        result.status = prepare_step(builder, leaf_pid, &result, &layout);
    }
    catch (const std::bad_alloc &) {
        result.status = Status::resource_exhausted("prepared split publication credit unavailable");
    }
    if (!result.status.ok()) {
        result.transaction = {};
        result.output_leaf_pids.clear();
        result.readiness = MaintenanceReadiness::kDeferred;
    }
    return result;
}
} // namespace crowdb::tree::detail
