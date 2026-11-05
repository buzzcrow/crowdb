// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/graph_summary.h"
#include "snapshot/primary_backing.h"
#include "snapshot/recovered_state_internal.h"

namespace crowdb::tree
{
void RecoveredStateOperation::State::push(uint64_t pid, PageKeyRange range)
{
    if (path.size() == limits.max_depth || report.pages == limits.max_pages) {
        finish(Status::resource_exhausted());
        return;
    }
    const auto found = captured->frames.find(pid);
    if (pid >= captured->next_page_id || found == captured->frames.end() || found->second.page_id != pid ||
        (found->second.type != page_type::kInnerBase && found->second.type != page_type::kLeafBase) ||
        !found->second.backing ||
        (!found->second.backing->readable() || (require_durable && !found->second.backing->reloadable())) ||
        found->second.backing->backend_id != captured->backend_id || !reached.insert(pid).second) {
        finish(Status::corruption());
        return;
    }
    Path next;
    next.summary.live_counts_known = true;
    next.pid                       = pid;
    next.descriptor                = &found->second;
    next.range                     = std::move(range);
    path.push_back(std::move(next));
    ++report.pages;
    wake();
}

void RecoveredStateOperation::State::inner_step()
{
    auto       &top  = path.back();
    const auto &keys = top.inner->separators;
    size_t      work = 0;
    while (top.index < keys.size() && work++ < limits.max_step_entries) {
        const auto &key = keys[top.index];
        if (key.size() > limits.max_key_bytes - top.key_bytes) {
            finish(Status::resource_exhausted());
            return;
        }
        top.key_bytes += key.size();
        if ((top.index != 0 && keys[top.index - 1] >= key) || (top.range.lower && key < *top.range.lower) ||
            (top.range.upper && key > *top.range.upper)) {
            finish(Status::corruption());
            return;
        }
        ++top.index;
    }
    if (top.index == keys.size()) {
        const auto &stored = top.inner->frame_proof;
        if (!stored.has_value()) {
            finish(Status::corruption());
            return;
        }
        const auto &proof = *stored;
        if (proof.has_fences && !proof.page_id_fences &&
            (proof.lower.size() > limits.max_key_bytes - top.key_bytes ||
             proof.upper.size() > limits.max_key_bytes - top.key_bytes - proof.lower.size())) {
            finish(Status::resource_exhausted());
            return;
        }
        top.index = 0;
        top.phase = Phase::Children;
    }
    wake();
}

void RecoveredStateOperation::State::child_step()
{
    auto &top = path.back();
    if (top.index == top.inner->children.size()) {
        top.phase = Phase::Finish;
        wake();
        return;
    }
    const auto index = top.index++;
    auto       range = top.range;
    if (index != 0) {
        range.lower = top.inner->separators[index - 1];
    }
    if (index < top.inner->separators.size()) {
        range.upper = top.inner->separators[index];
    }
    push(top.inner->children[index], std::move(range));
}

void RecoveredStateOperation::State::complete_page()
{
    auto    &top   = path.back();
    uint32_t level = 0;
    if (top.inner) {
        if (!top.child_level || *top.child_level >= limits.max_depth) {
            finish(Status::corruption());
            return;
        }
        level              = *top.child_level + 1;
        const auto &stored = top.inner->frame_proof;
        if (!stored.has_value()) {
            finish(Status::corruption());
            return;
        }
        const auto &proof = *stored;
        // Old native frames may omit fences; the traversal reconstructs them.
        if (proof.has_fences &&
            (!top.bounds.lower || !top.bounds.upper ||
             (proof.page_id_fences &&
              (proof.lower_leaf != top.bounds.lower_leaf || proof.upper_leaf != top.bounds.upper_leaf)) ||
             (!proof.page_id_fences && (proof.lower != *top.bounds.lower || proof.upper != *top.bounds.upper)))) {
            finish(Status::corruption());
            return;
        }
    }
    if (path.size() > 1) {
        auto &parent = path[path.size() - 2];
        if (parent.child_level && *parent.child_level != level) {
            finish(Status::corruption());
            return;
        }
        parent.child_level = level;
    }
    if (!install_page(level)) {
        return;
    }
    if (path.size() > 1) {
        auto &parent = path[path.size() - 2].bounds;
        if (!parent.lower && top.bounds.lower) {
            parent.lower      = std::move(top.bounds.lower);
            parent.lower_leaf = top.bounds.lower_leaf;
        }
        if (top.bounds.upper) {
            parent.upper      = std::move(top.bounds.upper);
            parent.upper_leaf = top.bounds.upper_leaf;
        }
    }
    if (path.size() > 1) {
        auto &parent                 = path[path.size() - 2].summary;
        parent.oldest_tombstone_slot = std::min(parent.oldest_tombstone_slot, top.summary.oldest_tombstone_slot);
        parent.live_keys += top.summary.live_keys;
        parent.live_key_bytes += top.summary.live_key_bytes;
        parent.live_value_bytes += top.summary.live_value_bytes;
    }
    path.pop_back();
    wake();
}

bool RecoveredStateOperation::State::install_page(uint32_t level)
{
    const auto &top  = path.back();
    auto        page = detail::reserved_shared<PublishedPage>(metadata);
    page->revision   = 1;
    page->level      = level;
    page->metrics    = top.metrics;
    if (top.inner) {
        page->inner_backing = top.descriptor->backing;
        auto descriptor     = *top.descriptor;
        descriptor.summary.emplace();
        descriptor.summary                     = top.summary;
        descriptor.summary->encoded_entries    = top.inner->separators.size();
        descriptor.summary->primary_used_bytes = top.metrics.page_size_bytes;
        descriptor.summary->inner_fences = {.lower_leaf = top.bounds.lower_leaf, .upper_leaf = top.bounds.upper_leaf};
        page->primary = make_inner_primary_backing(descriptor, *page, captured->coverage, building->gc_floor, metadata);
    }
    else {
        auto leaf               = detail::reserved_shared<PageState>(metadata, metadata);
        leaf->range             = top.range;
        leaf->route_generation  = 1;
        leaf->content_id        = 1;
        leaf->representation_id = 1;
        leaf->right_sibling     = top.leaf->right_sibling();
        leaf->bases.push_back({.identity   = top.pid + 1,
                               .generation = captured->generation,
                               .range      = top.range,
                               .max_slot   = captured->coverage,
                               .backing    = top.descriptor->backing});
        auto descriptor    = *top.descriptor;
        descriptor.summary = top.summary;
        page->primary      = make_primary_backing(descriptor, *leaf, captured->coverage, building->gc_floor, metadata);
        page->leaf         = std::move(leaf);
    }
    if (!building->summary ||
        !add_graph_summary(*building->summary, *top.descriptor, top.inner ? nullptr : &top.summary)) {
        finish(Status::corruption("recovered summary overflow or invalid frame"));
        return false;
    }
    MappingView::CopyStats copied;
    const PageReplacement  replacement{.page_id = top.pid, .expected = {}, .replacement = page};
    building->mapping = building->mapping.replace({&replacement, 1}, copied, metadata);
    if (top.pid == captured->root_page_id) {
        building->stats.height = static_cast<uint64_t>(level) + 1;
    }
    if (!apply_page_metrics(building->stats, nullptr, &page->metrics)) {
        finish(Status::corruption());
        return false;
    }
    return true;
}

void RecoveredStateOperation::State::verify_reachable()
{
    size_t work = 0;
    while (position != captured->frames.end() && work++ < limits.max_step_entries) {
        if (reached.erase(position->first) == 0) {
            finish(Status::corruption());
            return;
        }
        if (!building->summary || (position->second.type == page_type::kOverflowFrame &&
                                   !add_graph_summary(*building->summary, position->second))) {
            finish(Status::corruption("recovered overflow summary is invalid"));
            return;
        }
        ++position;
    }
    if (position != captured->frames.end()) {
        wake();
        return;
    }
    if (have_leaf && previous_sibling != kInvalidPageId) {
        finish(Status::corruption());
        return;
    }
    finish(Status::Ok());
}
} // namespace crowdb::tree
