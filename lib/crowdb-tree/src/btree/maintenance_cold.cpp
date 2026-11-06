// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "maintenance_operation_internal.h"
#include "maintenance_split_layout.h"
#include "page_load.h"

#include <algorithm>

namespace crowdb::tree::detail
{
namespace
{
size_t route_child(const InnerPageState &inner, const PageState &leaf)
{
    return leaf.range.lower.has_value()
             ? static_cast<size_t>(
                   std::upper_bound(inner.separators.begin(), inner.separators.end(), *leaf.range.lower) -
                   inner.separators.begin())
             : 0;
}

bool needs_load(const PublishedTreeState &view, uint64_t target, const PageState &leaf, size_t max_path,
                bool inner_repair)
{
    if (!inner_repair && std::ranges::any_of(leaf.bases, [](const auto &base) { return !base.resident; })) {
        return true;
    }
    auto pid = view.root_page_id;
    for (size_t depth = 0; pid != target && depth < max_path; ++depth) {
        const auto page = view.mapping.get(pid);
        if (!page || !page->inner) {
            return page && page->inner_backing && page->inner_backing->readable();
        }
        const auto child = route_child(*page->inner, leaf);
        if (child >= page->inner->children.size()) {
            return false;
        }
        if (inner_repair && page->level > 1) {
            for (const auto at :
                 {child == 0 ? child : child - 1, std::min(child + 1, page->inner->children.size() - 1)}) {
                const auto sibling = view.mapping.get(page->inner->children[at]);
                if (sibling && !sibling->inner && sibling->inner_backing) {
                    return true;
                }
            }
        }
        pid = page->inner->children[child];
    }
    return false;
}
} // namespace

bool MaintenanceOperation::begin_cold(std::shared_ptr<const PublishedTreeState> view,
                                      std::shared_ptr<const PublishedPage>      page)
{
    auto &started = phase == Phase::InnerMerge ? cold_inner_started : cold_started;
    if (started || !loader ||
        !needs_load(*view, (*candidates)[index].pid, *page->leaf, publication.max_path_pages,
                    phase == Phase::InnerMerge)) {
        return false;
    }
    auto pending = reserved_shared<Cold>(publication.metadata, publication.metadata);
    pending->replacements.reserve((3 * publication.max_path_pages) + 1);
    pending->siblings.reserve(2 * publication.max_path_pages);
    pending->leaf      = reserved_shared<PageState>(publication.metadata, *page->leaf, publication.metadata);
    pending->route_pid = view->root_page_id;
    pending->view      = std::move(view);
    pending->expected  = std::move(page);
    started            = true;
    cold               = std::move(pending);
    schedule();
    return true;
}

bool MaintenanceOperation::cold_route()
{
    auto      &pending = *cold;
    const auto target  = (*candidates)[index].pid;
    while (pending.route_pid != target && pending.route_steps < publication.max_path_pages) {
        const auto original = cold_page(pending.route_pid);
        if (!original || original->level == 0) {
            finish(Status::corruption("maintenance cold route has no inner page"));
            return false;
        }
        auto inner = original->inner;
        if (!inner) {
            return load_cold_inner(pending.route_pid);
        }
        const auto child = route_child(*inner, *pending.leaf);
        if (inner->children.size() != inner->separators.size() + 1 || child >= inner->children.size()) {
            finish(Status::corruption("maintenance cold route has invalid child bounds"));
            return false;
        }
        if (phase == Phase::InnerMerge && original->level > 1) {
            if (child != 0) {
                pending.siblings.push_back(inner->children[child - 1]);
            }
            if (child + 1 < inner->children.size()) {
                pending.siblings.push_back(inner->children[child + 1]);
            }
        }
        pending.route_pid = inner->children[child];
        ++pending.route_steps;
    }
    if (pending.route_pid != target) {
        finish(Status::resource_exhausted("maintenance cold route exceeds its depth bound"));
        return false;
    }
    return true;
}

void MaintenanceOperation::cold_step()
{
    auto &pending = *cold;
    if (pending.waiting) {
        if (!pending.ready.load(std::memory_order_acquire)) {
            return;
        }
        if (!pending.status.ok() || (pending.loading_inner ? !pending.loaded_inner : !pending.loaded)) {
            finish(pending.status.ok() ? Status::corruption("maintenance loader returned no page")
                                       : std::move(pending.status));
            return;
        }
        if (pending.loading_inner) {
            const auto original = pending.view->mapping.get(pending.loading_pid);
            auto       page     = reserved_shared<PublishedPage>(publication.metadata, *original);
            page->inner         = std::move(pending.loaded_inner);
            pending.replacements.push_back(
                {.page_id = pending.loading_pid, .expected = original, .replacement = std::move(page)});
        }
        else {
            pending.leaf->bases[pending.pending_base].resident = std::move(pending.loaded);
        }
        pending.loading_inner = false;
        pending.waiting       = false;
    }
    if (!cold_route()) {
        return;
    }
    if (phase == Phase::InnerMerge) {
        if (cold_siblings()) {
            cold_plan();
        }
        return;
    }
    while (pending.next_base < pending.leaf->bases.size()) {
        const auto  index = pending.next_base++;
        const auto &base  = pending.leaf->bases[index];
        if (base.resident) {
            continue;
        }
        if (!base.backing || !base.backing->readable()) {
            finish(Status::corruption("maintenance cold base has no readable owner"));
            return;
        }
        pending.pending_base = index;
        pending.waiting      = true;
        pending.ready.store(false, std::memory_order_release);
        auto self = shared_from_this();
        loader->load_leaf(base, [self, pending = cold](Status status, auto leaf) {
            pending->status = std::move(status);
            pending->loaded = std::move(leaf);
            pending->ready.store(true, std::memory_order_release);
            self->schedule();
        });
        return;
    }
    cold_plan();
}

void MaintenanceOperation::cold_plan()
{
    const auto pid  = (*candidates)[index].pid;
    auto       page = reserved_shared<PublishedPage>(publication.metadata, *cold->expected);
    if (phase != Phase::InnerMerge) {
        page->leaf = std::move(cold->leaf);
        cold->replacements.push_back({.page_id = pid, .expected = cold->expected, .replacement = page});
    }
    auto                   resolved = reserved_shared<PublishedTreeState>(publication.metadata, *cold->view);
    MappingView::CopyStats copied;
    std::sort(cold->replacements.begin(), cold->replacements.end(),
              [](const auto &left, const auto &right) { return left.page_id < right.page_id; });
    resolved->mapping = cold->view->mapping.replace(cold->replacements, copied, publication.metadata);
    auto result       = plan(resolved, page);
    // Loaded descriptors are private scratch. Compare every original page so a
    // concurrent route or writer change invalidates the complete transaction.
    for (auto &change : result.transaction.pages) {
        for (const auto &loaded : cold->replacements) {
            if (change.page_id == loaded.page_id) {
                change.expected = loaded.expected;
                if (change.replacement == loaded.replacement) {
                    change.replacement = loaded.expected;
                }
                break;
            }
        }
    }
    cold.reset();
    if (!result.status.ok()) {
        finish(std::move(result.status));
        return;
    }
    if (result.readiness != MaintenanceReadiness::kReady || !result.changed()) {
        advance();
        return;
    }
    if (phase == Phase::InnerMerge) {
        ++steps;
    }
    submit(std::move(result));
}
} // namespace crowdb::tree::detail
