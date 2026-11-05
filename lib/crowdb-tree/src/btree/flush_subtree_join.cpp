// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_subtree.h"
#include "page_load.h"
#include "snapshot/flush_persistence.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace crowdb::tree
{
void FlushBatch::State::Subtree::join()
{
    prepare_builder();

    struct Child
    {
        Subtree                *task;
        uint64_t                pid;
        std::optional<OwnedKey> lower;
        bool                    touched;
    };

    detail::ReservedVector<Child> ordered{detail::ReservationAllocator<Child>(owner.metadata)};
    for (size_t i = 0; i < children.size(); ++i) {
        auto lower = i != 0 ? std::optional(inner->separators[i - 1]) : bounds.lower;
        if (!children[i]) {
            ordered.push_back({.task = this, .pid = inner->children[i], .lower = std::move(lower), .touched = false});
            continue;
        }
        auto &child = *children[i];
        for (size_t j = 0; j < child.output.pids.size(); ++j) {
            if (j != 0) {
                lower = child.output.separators[j - 1];
            }
            ordered.push_back({.task = &child, .pid = child.output.pids[j], .lower = lower, .touched = true});
        }
    }
    for (size_t i = 0; i + 1 < ordered.size();) {
        auto &a      = ordered[i];
        auto &b      = ordered[i + 1];
        auto  left   = a.task->lookup(a.pid);
        auto  right  = b.task->lookup(b.pid);
        bool  merged = false;
        if (a.touched || b.touched) {
            a.task->prepare_builder();
            b.task->prepare_builder();
            if (left->leaf && right->leaf && left->metrics.page_size_known && right->metrics.page_size_known &&
                left->metrics.kv_known && right->metrics.kv_known) {
                PageVersion version;
                auto        result = detail::next_page_version(*left->leaf, right->leaf.get(), &version);
                if (!result.ok()) {
                    complete(std::move(result));
                    return;
                }
                auto plan = detail::plan_published_leaf_merge(*left, *right,
                                                              {.left          = left->leaf,
                                                               .right         = right->leaf,
                                                               .left_page_id  = a.pid,
                                                               .right_page_id = b.pid,
                                                               .left_parent   = pid,
                                                               .right_parent  = pid},
                                                              builder->options.pages, version, 1);
                if (!plan.logical.status.ok()) {
                    complete(std::move(plan.logical.status));
                    return;
                }
                if (plan.logical.page) {
                    result = a.task->builder->make_leaf(a.pid, left, std::move(plan.logical.page), plan.primary_bytes,
                                                        right->allocation_owner,
                                                        plan.statistics ? &plan.statistics.value() : nullptr);
                    if (!result.ok()) {
                        complete(std::move(result));
                        return;
                    }
                    merged = true;
                }
            }
            else if (left->inner && right->inner && b.lower &&
                     std::min(left->inner->separators.size(), right->inner->separators.size()) <
                         builder->options.inner_merge_keys &&
                     left->inner->children.size() + right->inner->children.size() <=
                         builder->options.max_inner_children) {
                InnerPageState combined(*left->inner, builder->options.metadata);
                combined.children.insert(combined.children.end(), right->inner->children.begin(),
                                         right->inner->children.end());
                combined.separators.push_back(*b.lower);
                combined.separators.insert(combined.separators.end(), right->inner->separators.begin(),
                                           right->inner->separators.end());
                size_t bytes = kFrameHeaderSize + kFrameTrailerSize + (combined.children.size() * sizeof(uint64_t)) +
                               (combined.separators.size() * kInnerSlotSize);
                for (const auto &key : combined.separators) {
                    bytes += key.size();
                }
                if (bytes <= builder->options.max_inner_bytes) {
                    detail::MaintenanceChildren outputs(owner.metadata);
                    auto result = a.task->builder->partition_inner(a.pid, left->level, combined, &outputs);
                    if (!result.ok()) {
                        complete(std::move(result));
                        return;
                    }
                    if (outputs.pids.size() != 1) {
                        complete(Status::internal_error("accepted index merge split again"));
                        return;
                    }
                    merged = true;
                }
            }
        }
        if (merged) {
            record_work(owner.publisher.work_metrics(), TreeWorkCounter::MergeOperations, 1, TreeWorkCause::Flush);
            record_work(owner.publisher.work_metrics(), TreeWorkCounter::MergeInputPages, 2, TreeWorkCause::Flush);
            record_work(owner.publisher.work_metrics(), TreeWorkCounter::MergeOutputPages, 1, TreeWorkCause::Flush);
            auto result = b.task->builder->replace(b.pid, {});
            if (!result.ok()) {
                complete(std::move(result));
                return;
            }
            a.touched = true;
            ordered.erase(ordered.begin() + static_cast<ptrdiff_t>(i + 1));
        }
        else {
            ++i;
        }
    }
    InnerPageState combined(builder->options.metadata);
    for (size_t i = 0; i < ordered.size(); ++i) {
        if (owner.options.persistent && ordered[i].touched) {
            final_pages.push_back({.task = ordered[i].task, .pid = ordered[i].pid});
        }
        combined.children.push_back(ordered[i].pid);
        if (i != 0) {
            combined.separators.push_back(ordered[i].lower.value());
        }
    }
    output.pids.clear();
    output.separators.clear();
    if (combined.children == inner->children && combined.separators == inner->separators) {
        output.pids.push_back(pid);
    }
    else {
        auto result = builder->partition_inner(pid, expected->level, combined, &output);
        if (!result.ok()) {
            complete(std::move(result));
            return;
        }
    }
#ifdef CROWDB_TREE_TEST_UTIL
    if (owner.options.after_page_for_tests) {
        owner.options.after_page_for_tests(pid, expected->level);
    }
#endif
    finish_pages();
}

std::shared_ptr<const PublishedPage> FlushBatch::State::Subtree::lookup(uint64_t page_id) const
{
    return builder ? builder->lookup(page_id) : owner.captured->mapping.get(page_id);
}

Status FlushBatch::State::Subtree::collect(PublicationTransaction &transaction)
{
    if (range) {
        transaction.required_installs.push_back(range->record);
    }
    for (auto &child : children) {
        if (child) {
            auto status = child->collect(transaction);
            if (!status.ok()) {
                return status;
            }
        }
    }
    if (!builder) {
        return Status::Ok();
    }
    MaintenancePublicationResult result(builder->options.metadata);
    auto                         status = builder->finish(&result);
    if (!status.ok()) {
        return status;
    }
    for (auto &page : result.transaction.pages) {
        transaction.pages.push_back(std::move(page));
    }
    return Status::Ok();
}

void FlushBatch::State::start_subtree()
{
    if (!subtree) {
        next_pid.store(captured->next_page_id, std::memory_order_relaxed);
        const auto first = first_input({});
        const bool selected =
            first.has_value() || (options.persistent && options.persistence &&
                                  options.persistence->selected(captured->root_page_id, options.selection));
        subtree =
            detail::reserved_shared<Subtree>(metadata, *this, captured->root_page_id, options.selection, selected);
    }
    else {
        subtree->reset_work();
    }
    next_pid.store(captured->next_page_id, std::memory_order_relaxed);
    operation = detail::reserved_unique<Attempt>(metadata, subtree->sender(), shared_from_this());
    stdexec::start(operation->connected);
}

Status FlushBatch::State::Subtree::prepare_root()
{
    prepare_builder();
    auto    &outputs = output;
    uint64_t root    = outputs.pids.front();
    if (outputs.pids.size() > 1) {
        const auto status = builder->grow_root(std::move(outputs), expected->level);
        if (!status.ok()) {
            return status;
        }
        root = builder->root_page_id();
    }
    // Only the root may contract its level. Descendant joins retain their
    // level until their parent has made its own sibling decision.
    auto find = [&](auto &&self, Subtree &task, uint64_t pid) -> std::shared_ptr<const PublishedPage> {
        if (task.builder) {
            auto page = task.builder->lookup(pid);
            if (page != task.builder->view->mapping.get(pid)) {
                return page;
            }
        }
        for (auto &child : task.children) {
            if (!child) {
                continue;
            }
            auto page = self(self, *child, pid);
            if (page != owner.captured->mapping.get(pid)) {
                return page;
            }
        }
        return owner.captured->mapping.get(pid);
    };
    for (size_t depth = 0; active && depth < owner.options.max_route_depth; ++depth) {
        const auto page = find(find, *this, root);
        if (!page || !page->inner || page->inner->children.size() != 1) {
            break;
        }
        const auto child = page->inner->children.front();
        // Root deletions belong to the original task that owns that PID.
        auto remove = [&](auto &&self, Subtree &task) -> Status {
            if (task.pid == root ||
                (task.builder && task.builder->lookup(root) != task.builder->view->mapping.get(root))) {
                task.prepare_builder();
                return task.builder->replace(root, {});
            }
            for (auto &next : task.children) {
                if (next != nullptr) {
                    auto status = self(self, *next);
                    if (!status.ok()) {
                        return status;
                    }
                }
            }
            return Status::Ok();
        };
        auto removed = remove(remove, *this);
        if (!removed.ok()) {
            return removed;
        }
        root = child;
    }
    final_root = root;
    return Status::Ok();
}

Status FlushBatch::State::collect_subtree(PublicationTransaction &transaction)
{
    if (!options.consolidation) {
        return Status::internal_error("flush consolidation options are absent");
    }
    if (subtree->final_root == kInvalidPageId) {
        auto prepared = subtree->prepare_root();
        if (!prepared.ok()) {
            return prepared;
        }
    }
    const auto root                   = subtree->final_root;
    const auto snapshot_metadata      = options.consolidation->snapshot_metadata;
    transaction.root_page_id          = root;
    transaction.expected_root_page_id = captured->root_page_id;
    transaction.expected_next_page_id = captured->next_page_id;
    transaction.next_page_id          = next_pid.load(std::memory_order_relaxed);
    transaction.snapshot_metadata     = snapshot_metadata;
    auto status                       = subtree->collect(transaction);
    if (!status.ok()) {
        return status;
    }
    std::sort(transaction.pages.begin(), transaction.pages.end(),
              [](const auto &a, const auto &b) { return a.page_id < b.page_id; });
    return Status::Ok();
}

} // namespace crowdb::tree
