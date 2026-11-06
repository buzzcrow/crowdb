// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_subtree.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/primary_backing.h"

#include <algorithm>

namespace crowdb::tree
{
void FlushBatch::State::Subtree::finish_pages()
{
    if (this == owner.subtree.get()) {
        auto result = prepare_root();
        if (!result.ok()) {
            complete(std::move(result));
            return;
        }
        if (owner.options.persistent) {
            MaintenancePublicationResult changes(owner.metadata);
            result = builder->finish(&changes);
            if (!result.ok()) {
                complete(std::move(result));
                return;
            }
            for (const auto &change : changes.transaction.pages) {
                if (change.replacement) {
                    final_pages.push_back({.task = this, .pid = change.page_id});
                }
            }
            // An unchanged root may still lack a durable representation.
            auto *root_owner = page_owner(final_root);
            final_pages.push_back({.task = root_owner != nullptr ? root_owner : this, .pid = final_root});
            std::sort(final_pages.begin(), final_pages.end(),
                      [](const auto &a, const auto &b) { return a.pid < b.pid; });
            final_pages.erase(std::unique(final_pages.begin(), final_pages.end(),
                                          [](const auto &a, const auto &b) { return a.pid == b.pid; }),
                              final_pages.end());
        }
    }
    joined = true;
    persist_next();
}

FlushBatch::State::Subtree *FlushBatch::State::Subtree::page_owner(uint64_t page_id)
{
    if (builder && builder->lookup(page_id) != builder->view->mapping.get(page_id)) {
        return this;
    }
    if (pid == page_id) {
        return this;
    }
    for (auto &child : children) {
        if (child) {
            if (auto *found = child->page_owner(page_id)) {
                return found;
            }
        }
    }
    return nullptr;
}

void FlushBatch::State::Subtree::persist_next()
{
    if (!owner.options.persistent) {
        complete(Status::Ok());
        return;
    }
    if (!owner.options.persistence) {
        complete(Status::invalid_argument("persistent flush requires its page sink"));
        return;
    }
    while (persist_at < final_pages.size()) {
        const auto target = final_pages[persist_at];
        const auto page   = target.task->lookup(target.pid);
        if (!page) {
            ++persist_at;
            continue;
        }
        writing = true;
        request_load();
        return;
    }
    complete(Status::Ok());
}

std::shared_ptr<const PublishedPage> FlushBatch::State::Subtree::find_page(uint64_t page_id) const
{
    if (builder) {
        auto page = builder->lookup(page_id);
        if (page != builder->view->mapping.get(page_id)) {
            return page;
        }
    }
    auto captured = owner.captured->mapping.get(page_id);
    // Descendants cannot replace an existing peer/ancestor PID. An existing
    // leaf also keeps its original task owner through sibling merge/split.
    // Prune unrelated branches instead of searching every leaf for each
    // index child's summary and fence.
    if (captured && expected &&
        (captured->level >= expected->level || (captured->leaf && !bounds.overlaps(captured->leaf->range)))) {
        return captured;
    }
    for (const auto &child : children) {
        if (child) {
            auto page = child->find_page(page_id);
            if (page != owner.captured->mapping.get(page_id)) {
                return page;
            }
        }
    }
    return captured;
}

FinalPageSummary FlushBatch::State::Subtree::child_summary(uint64_t page_id) const
{
    FinalPageSummary result;
    const auto       page = find_page(page_id);
    if (!page || !page->inner) {
        return result;
    }
    for (auto child : page->inner->children) {
        const auto output = find_page(child);
        if (!output || !output->primary || !output->primary->frame.summary ||
            !output->primary->frame.summary->live_counts_known) {
            return {};
        }
        const auto &part = *output->primary->frame.summary;
        if (part.live_keys > UINT64_MAX - result.live_keys ||
            part.live_key_bytes > UINT64_MAX - result.live_key_bytes ||
            part.live_value_bytes > UINT64_MAX - result.live_value_bytes) {
            return {};
        }
        result.oldest_tombstone_slot = std::min(result.oldest_tombstone_slot, part.oldest_tombstone_slot);
        result.live_keys += part.live_keys;
        result.live_key_bytes += part.live_key_bytes;
        result.live_value_bytes += part.live_value_bytes;
    }
    result.live_counts_known = true;
    return result;
}

InnerEncodeFences FlushBatch::State::Subtree::fences(uint64_t page_id) const
{
    auto edge = [&](bool upper) {
        auto next = page_id;
        for (size_t depth = 0; depth < owner.options.max_route_depth; ++depth) {
            auto page = find_page(next);
            if (!page) {
                return kInvalidPageId;
            }
            if (page->level == 0) {
                return next;
            }
            if (!page->inner) {
                if (page->primary && page->primary->frame.summary && page->primary->frame.summary->inner_fences) {
                    return upper ? page->primary->frame.summary->inner_fences->upper_leaf
                                 : page->primary->frame.summary->inner_fences->lower_leaf;
                }
                return kInvalidPageId;
            }
            next = upper ? page->inner->children.back() : page->inner->children.front();
        }
        return kInvalidPageId;
    };
    return {.lower_leaf = edge(false), .upper_leaf = edge(true)};
}
} // namespace crowdb::tree
