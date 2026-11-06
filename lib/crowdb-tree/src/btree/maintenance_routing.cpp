// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_publication_internal.h"

#include <algorithm>
#include <limits>
#include <ranges>

namespace crowdb::tree::detail
{
namespace
{
constexpr size_t kInnerOverhead = kFrameHeaderSize + kFrameTrailerSize + sizeof(uint64_t);

Status validate_inner(const InnerPageState &inner, size_t metadata_limit)
{
    if (inner.children.empty() || inner.separators.size() != inner.children.size() - 1 ||
        inner.children.size() > metadata_limit / sizeof(uint64_t)) {
        return Status::corruption("invalid inner routing shape or metadata bound");
    }
    size_t bytes = inner.children.size() * sizeof(uint64_t);
    for (size_t i = 0; i < inner.separators.size(); ++i) {
        if (inner.separators[i].size() > metadata_limit - bytes ||
            (i != 0 && inner.separators[i - 1] >= inner.separators[i])) {
            return Status::corruption("inner separators exceed the bound or are not ordered");
        }
        bytes += inner.separators[i].size();
    }
    return Status::Ok();
}
} // namespace

Status MaintenanceBuilder::path_to(uint64_t pid, MaintenancePath *path) const
{
    const auto leaf = view->mapping.get(pid);
    if (!leaf || leaf->level != 0 || !leaf->leaf) {
        return Status::invalid_argument("maintenance target must be a live leaf");
    }
    uint64_t     current = view->root_page_id;
    PageKeyRange bounds;
    while (current != pid) {
        if (path->size() >= options.max_path_pages) {
            return Status::resource_exhausted("maintenance route depth exceeded");
        }
        const auto parent = view->mapping.get(current);
        if (!parent || !parent->inner || parent->level == 0) {
            return Status::unavailable("target leaf is not reachable through its range");
        }
        auto status = validate_inner(*parent->inner, options.pages.max_metadata_bytes);
        if (!status.ok()) {
            return status;
        }
        const auto  &inner = *parent->inner;
        const size_t child = leaf->leaf->range.lower.has_value()
                               ? static_cast<size_t>(std::upper_bound(inner.separators.begin(), inner.separators.end(),
                                                                      *leaf->leaf->range.lower) -
                                                     inner.separators.begin())
                               : 0;
        if (child != 0) {
            bounds.lower = inner.separators[child - 1];
        }
        if (child < inner.separators.size()) {
            bounds.upper = inner.separators[child];
        }
        const auto next = view->mapping.get(inner.children[child]);
        if (!next || parent->level - 1 != next->level) {
            return Status::corruption("inner child level does not match its parent");
        }
        path->push_back({.pid = current, .child = child, .page = parent});
        current = inner.children[child];
    }
    if ((bounds.lower.has_value() && bounds.lower != leaf->leaf->range.lower) ||
        (bounds.upper.has_value() && bounds.upper != leaf->leaf->range.upper)) {
        return Status::corruption("leaf range disagrees with its parent route");
    }
    return Status::Ok();
}

Status MaintenanceBuilder::guard_path(const MaintenancePath &path)
{
    for (const auto &entry : path) {
        if (!changes_.contains(entry.pid)) {
            auto status = replace(entry.pid, entry.page);
            if (!status.ok()) {
                return status;
            }
        }
    }
    return Status::Ok();
}

Status MaintenanceBuilder::partition_inner(uint64_t pid, uint32_t level, const InnerPageState &inner,
                                           MaintenanceChildren *output)
{
    auto status = validate_inner(inner, options.pages.max_metadata_bytes);
    if (!status.ok()) {
        return status;
    }
    for (const auto &separator : inner.separators) {
        if (separator.size() > kMaxPhysicalPageBytes - kInnerOverhead - sizeof(uint64_t) - kInnerSlotSize) {
            return Status::invalid_argument("inner separator exceeds the physical page safety bound");
        }
    }
    size_t begin = 0;
    while (begin < inner.children.size()) {
        size_t end   = begin + 1;
        size_t bytes = kInnerOverhead;
        while (end < inner.children.size() && end - begin < options.max_inner_children) {
            const size_t added = sizeof(uint64_t) + kInnerSlotSize + inner.separators[end - 1].size();
            if (end != begin + 1 && added > options.max_inner_bytes - std::min(bytes, options.max_inner_bytes)) {
                break;
            }
            bytes += added;
            ++end;
        }
        uint64_t output_pid = pid;
        if (begin != 0 || output_pid == kInvalidPageId) {
            status = allocate_pid(&output_pid);
            if (!status.ok()) {
                return status;
            }
        }
        auto content = reserved_shared<InnerPageState>(options.metadata, options.metadata);
        content->children.assign(inner.children.begin() + static_cast<ptrdiff_t>(begin),
                                 inner.children.begin() + static_cast<ptrdiff_t>(end));
        content->separators.assign(inner.separators.begin() + static_cast<ptrdiff_t>(begin),
                                   inner.separators.begin() + static_cast<ptrdiff_t>(end - 1));
        auto       page = reserved_shared<PublishedPage>(options.metadata);
        const auto old  = view->mapping.get(output_pid);
        if (old && old->revision == UINT64_MAX) {
            return Status::resource_exhausted("inner revision exhausted");
        }
        page->revision                         = old ? old->revision + 1 : 1;
        page->level                            = level;
        page->inner                            = std::move(content);
        page->metrics.leaf                     = false;
        page->metrics.dirty                    = true;
        page->metrics.writeback_candidate      = true;
        page->metrics.page_size_known          = true;
        page->metrics.page_size_bytes          = bytes;
        page->metrics.page_size_estimate_bytes = bytes;
        status                                 = replace(output_pid, std::move(page));
        if (!status.ok()) {
            return status;
        }
        output->pids.push_back(output_pid);
        if (end < inner.children.size()) {
            output->separators.push_back(inner.separators[end - 1]);
        }
        begin = end;
    }
    return Status::Ok();
}

Status MaintenanceBuilder::grow_root(MaintenanceChildren children, uint32_t child_level)
{
    while (children.pids.size() > 1) {
        if (child_level >= options.max_path_pages || child_level == std::numeric_limits<uint32_t>::max()) {
            return Status::resource_exhausted("new root exceeds the route depth limit");
        }
        InnerPageState inner(options.metadata);
        inner.separators.assign(children.separators.begin(), children.separators.end());
        inner.children.assign(children.pids.begin(), children.pids.end());
        MaintenanceChildren parents(options.metadata);
        auto                status = partition_inner(kInvalidPageId, child_level + 1, inner, &parents);
        if (!status.ok()) {
            return status;
        }
        children = std::move(parents);
        ++child_level;
    }
    root_pid_ = children.pids.front();
    return Status::Ok();
}

Status MaintenanceBuilder::propagate(const MaintenancePath &path, size_t removed_children, MaintenanceChildren children,
                                     uint32_t child_level)
{
    for (const auto &parent : std::views::reverse(path)) {
        InnerPageState inner(*parent.page->inner, options.metadata);
        const auto     at = static_cast<ptrdiff_t>(parent.child);
        if (removed_children > inner.children.size() - parent.child) {
            return Status::corruption("maintenance child replacement is outside its parent");
        }
        inner.children.erase(inner.children.begin() + at,
                             inner.children.begin() + at + static_cast<ptrdiff_t>(removed_children));
        inner.children.insert(inner.children.begin() + at, children.pids.begin(), children.pids.end());
        inner.separators.erase(inner.separators.begin() + at,
                               inner.separators.begin() + at + static_cast<ptrdiff_t>(removed_children - 1));
        inner.separators.insert(inner.separators.begin() + at, children.separators.begin(), children.separators.end());
        MaintenanceChildren outputs(options.metadata);
        auto                status = partition_inner(parent.pid, parent.page->level, inner, &outputs);
        if (!status.ok()) {
            return status;
        }
        if (outputs.pids.size() == 1) {
            return Status::Ok();
        }
        children         = std::move(outputs);
        child_level      = parent.page->level;
        removed_children = 1;
    }
    return grow_root(std::move(children), child_level);
}

Status MaintenanceBuilder::shrink_root()
{
    for (size_t depth = 0; depth < options.max_path_pages; ++depth) {
        const auto root = lookup(root_pid_);
        if (!root || !root->inner || root->inner->children.size() != 1) {
            return root ? Status::Ok() : Status::corruption("maintenance root disappeared");
        }
        const auto child_pid = root->inner->children.front();
        const auto child     = lookup(child_pid);
        if (!child || root->level == 0 || child->level != root->level - 1) {
            return Status::corruption("root contraction has an invalid child");
        }
        auto status = replace(root_pid_, {});
        if (!status.ok()) {
            return status;
        }
        root_pid_ = child_pid;
    }
    return Status::resource_exhausted("root contraction depth exceeded");
}
} // namespace crowdb::tree::detail
