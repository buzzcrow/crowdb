// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_inspection.h"

#include "btree/allocation_bound.h"
#include "btree/tree_core_read_internal.h"
#include "snapshot/captured_frames.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
constexpr uint32_t kInspectionBytes = 1024 * 1024;

struct Loaded
{
    Status                                status;
    std::shared_ptr<const LeafBase>       leaf;
    std::shared_ptr<const InnerPageState> inner;
    std::atomic<bool>                     ready{false};

    void finish(Status result) noexcept
    {
        status = std::move(result);
        ready.store(true, std::memory_order_release);
        ready.notify_one();
    }

    [[nodiscard]] Status wait() const
    {
        while (!ready.load(std::memory_order_acquire)) {
            ready.wait(false, std::memory_order_acquire);
        }
        return status;
    }
};

struct Inspection
{
    core_read_detail::ReadPreparation         prepared;
    std::shared_ptr<const PublishedTreeState> view;

    Status load(uint64_t pid, std::optional<size_t> base = {})
    {
        const auto old = view->mapping.get(pid);
        if (!old || (base && (!old->leaf || *base >= old->leaf->bases.size()))) {
            return Status::corruption("inspection route or base is absent");
        }
        if (!base && old->inner) {
            return Status::Ok();
        }
        auto event = detail::reserved_shared<Loaded>(prepared.credit);
        if (base) {
            prepared.loader->load_leaf(old->leaf->bases[*base], [event](Status status, auto value) {
                event->leaf = std::move(value);
                event->finish(std::move(status));
            });
        }
        else {
            prepared.loader->load_inner(old->inner_backing, [event](Status status, auto value) {
                event->inner = std::move(value);
                event->finish(std::move(status));
            });
        }
        auto status = event->wait();
        if (!status.ok()) {
            return status;
        }
        auto page = detail::reserved_shared<PublishedPage>(prepared.credit, *old);
        if (base) {
            if (!event->leaf) {
                return Status::corruption("inspection loader returned no leaf");
            }
            auto       leaf    = detail::reserved_shared<PageState>(prepared.credit, *old->leaf, prepared.credit);
            const auto maximum = leaf->bases[*base].max_slot;
            if (maximum) {
                for (size_t i = 0; i < event->leaf->count(); ++i) {
                    if (CellView(event->leaf->cell(i)).slot() > *maximum) {
                        return Status::corruption("inspection base exceeds captured coverage");
                    }
                }
            }
            leaf->bases[*base].resident = std::move(event->leaf);
            page->leaf                  = std::move(leaf);
        }
        else {
            if (!event->inner) {
                return Status::corruption("inspection loader returned no index");
            }
            page->inner = std::move(event->inner);
        }
        auto                   next = detail::reserved_shared<PublishedTreeState>(prepared.credit, *view);
        const PageReplacement  change{.page_id = pid, .expected = old, .replacement = std::move(page)};
        MappingView::CopyStats copied;
        next->mapping = next->mapping.replace({&change, 1}, copied, prepared.credit);
        view          = std::move(next);
        return Status::Ok();
    }

    Status edge(uint64_t pid, bool right, uint64_t *out)
    {
        for (size_t depth = 0; depth < prepared.limits.max_route_depth; ++depth) {
            auto page = view->mapping.get(pid);
            if (!page) {
                return Status::corruption("inspection fence route is absent");
            }
            if (page->level == 0) {
                *out = pid;
                return Status::Ok();
            }
            auto status = load(pid);
            if (!status.ok()) {
                return status;
            }
            page = view->mapping.get(pid);
            if (page->inner->children.empty()) {
                return Status::corruption("inspection index has no children");
            }
            const auto child = right ? page->inner->children.back() : page->inner->children.front();
            auto       next  = view->mapping.get(child);
            if (!next || next->level + 1 != page->level) {
                return Status::corruption("inspection fence route level mismatch");
            }
            pid = child;
        }
        return Status::resource_exhausted("inspection fence route exceeds depth bound");
    }
};

struct CopiedFrame
{
    CopiedFrame(const std::shared_ptr<AllocationBudget> &credit, size_t capacity)
        : bytes(credit, capacity, AllocationKind::kFrame)
    {
    }

    AllocationLease             bytes;
    std::shared_ptr<const void> dependencies;
};
} // namespace

Status inspect_published_page(TreeCore &core, const std::vector<uint32_t> &path, uint64_t expected_version,
                              NativeFrame *out, uint64_t *version, uint64_t *root, uint32_t *deltas)
try {
    if (path.size() > 32 || out == nullptr || version == nullptr || root == nullptr || deltas == nullptr) {
        return Status::invalid_argument("page inspection requires outputs and at most 32 levels");
    }
    CapturedFrameLimits frames;
    frames.max_depth                 = 33;
    frames.encode.frame_bytes        = kInspectionBytes;
    frames.encode.inline_value_bytes = kInspectionBytes;
    frames.encode.max_sources        = core.read_limits().max_page_sources;
    frames.encode.work_metrics       = core.publisher().work_metrics();
    const auto input                 = core.capture();
    const auto shape                 = input->mapping.copy_bounds();
    frames.page_metadata_bytes       = shape.max_leaf_bytes;
    Inspection inspection;
    auto       status = core_read_detail::prepare(core, input, core.read_limits(), &inspection.prepared);
    if (!status.ok()) {
        return status;
    }
    if (inspection.prepared.limits.max_route_depth > frames.max_depth) {
        return Status::resource_exhausted("inspection depth exceeds its bound");
    }
    inspection.view     = inspection.prepared.view;
    const auto captured = inspection.view;
    if (expected_version != kInvalidPageId && expected_version != captured->commit_seq) {
        return Status::unavailable("tree changed; restart page inspection");
    }
    uint64_t     pid = captured->root_page_id;
    PageKeyRange range;
    for (const auto child : path) {
        auto page = inspection.view->mapping.get(pid);
        if (!page || page->level == 0) {
            return Status::unavailable("page path changed; restart inspection");
        }
        status = inspection.load(pid);
        if (!status.ok()) {
            return status;
        }
        page              = inspection.view->mapping.get(pid);
        const auto &inner = *page->inner;
        if (inner.children.empty() || inner.children.size() != inner.separators.size() + 1 ||
            child >= inner.children.size()) {
            return Status::invalid_argument("child outside inspected page");
        }
        if (child != 0) {
            range.lower = inner.separators[child - 1];
        }
        if (child < inner.separators.size()) {
            range.upper = inner.separators[child];
        }
        const auto next = inspection.view->mapping.get(inner.children[child]);
        if (!next || next->level + 1 != page->level) {
            return Status::corruption("inspection route level mismatch");
        }
        pid = inner.children[child];
    }
    const auto page = inspection.view->mapping.get(pid);
    if (!page) {
        return Status::not_found();
    }
    CapturedEncodeContext context{.range = range};
    if (page->level != 0) {
        InnerEncodeFences fences;
        status = inspection.edge(pid, false, &fences.lower_leaf);
        if (status.ok()) {
            status = inspection.edge(pid, true, &fences.upper_leaf);
        }
        if (!status.ok()) {
            return status;
        }
        context.inner_fences = fences;
    }
    else {
        context.range = page->leaf->range;
    }
    frames.encode.metadata = inspection.prepared.credit;
    for (size_t attempts = 0; attempts <= frames.encode.max_sources; ++attempts) {
        auto encoded = prepare_final_page(inspection.view, pid, context, frames.encode);
        if (!encoded.status.ok()) {
            return encoded.status;
        }
        if (encoded.needs_base) {
            status = inspection.load(pid, encoded.needs_base->base_index);
        }
        else if (encoded.needs_inner) {
            status = inspection.load(pid);
        }
        else {
            if (encoded.encoder->summary().new_overflow_frames != 0) {
                return Status::resource_exhausted("inspection frame exceeds its 1 MiB output bound");
            }
            NativeFrame result;
            result.page_id = pid;
            result.frame.resize(kInspectionBytes);
            auto owner = detail::reserved_shared<CopiedFrame>(inspection.prepared.credit, inspection.prepared.credit,
                                                              result.frame.capacity());
            // Copied inline bytes are self-contained. Only reused overflow
            // addresses still need the captured backing after encoding ends.
            if (!encoded.encoder->reused_overflow().empty()) {
                owner->dependencies = encoded.encoder->dependency_owner();
            }
            result.allocation_owner = std::move(owner);
            auto step               = encoded.encoder->next(result.frame);
            if (!step.status.ok() || !step.frame || !step.complete) {
                return step.status.ok() ? Status::corruption("inspection did not produce one primary") : step.status;
            }
            const auto current = core.capture();
            if (current->generation != captured->generation || current->commit_seq != captured->commit_seq) {
                return Status::unavailable("tree changed during inspection");
            }
            *version = captured->commit_seq;
            *root    = captured->root_page_id;
            *deltas  = page->leaf ? static_cast<uint32_t>(page->leaf->overlays.size()) : 0;
            *out     = std::move(result);
            return Status::Ok();
        }
        if (!status.ok()) {
            return status;
        }
    }
    return Status::resource_exhausted("inspection source bound exceeded");
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree
