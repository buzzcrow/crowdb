// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "btree/maintenance_publication_internal.h"

#include <algorithm>

namespace crowdb::tree::detail
{
namespace
{
bool fits_merged(const InnerPageState &left, const InnerPageState &right, const OwnedKey &separator,
                 const MaintenancePublicationOptions &options)
{
    const auto count = AllocationBound(left.children.size()).add(right.children.size()).value();
    if (!count || *count > options.max_inner_children) {
        return false;
    }
    AllocationBound bytes(kFrameHeaderSize + kFrameTrailerSize);
    bytes.add_product(*count, sizeof(uint64_t)).add_product(*count - 1, kInnerSlotSize).add(separator.size());
    for (const auto &key : left.separators) {
        bytes.add(key.size());
    }
    for (const auto &key : right.separators) {
        bytes.add(key.size());
    }
    return bytes.value().has_value() && *bytes.value() <= options.max_inner_bytes;
}
} // namespace

Status MaintenanceBuilder::merge_inner_pair(const MaintenancePathEntry &ancestor, uint64_t target, bool *changed)
{
    const auto parent = lookup(ancestor.pid);
    const auto node   = lookup(target);
    if (!parent || !parent->inner || !node || !node->inner || parent->level != node->level + 1) {
        return Status::unavailable("inner merge needs a resident guarded route");
    }
    const auto &route = *parent->inner;
    const auto  found = std::find(route.children.begin(), route.children.end(), target);
    if (found == route.children.end()) {
        return Status::corruption("inner merge target is outside its parent");
    }
    if (route.children.size() < 2 || node->inner->separators.size() >= options.inner_merge_keys) {
        return Status::Ok();
    }
    const size_t index      = static_cast<size_t>(found - route.children.begin());
    const size_t left_index = index == 0 ? 0 : index - 1;
    const auto   left_pid   = route.children[left_index];
    const auto   right_pid  = route.children[left_index + 1];
    const auto   left       = lookup(left_pid);
    const auto   right      = lookup(right_pid);
    if (!left || !right || left->level != node->level || right->level != node->level) {
        return Status::corruption("inner merge sibling has an invalid level");
    }
    if (!left->inner || !right->inner) {
        // A later maintenance window loads the bounded route and its siblings.
        return Status::Ok();
    }
    if (!fits_merged(*left->inner, *right->inner, route.separators[left_index], options)) {
        return Status::Ok();
    }
    InnerPageState merged(*left->inner, options.metadata);
    merged.children.insert(merged.children.end(), right->inner->children.begin(), right->inner->children.end());
    merged.separators.push_back(route.separators[left_index]);
    merged.separators.insert(merged.separators.end(), right->inner->separators.begin(), right->inner->separators.end());
    MaintenanceChildren output(options.metadata);
    auto                status = partition_inner(left_pid, left->level, merged, &output);
    if (!status.ok()) {
        return status;
    }
    status = replace(right_pid, {});
    if (!status.ok()) {
        return status;
    }
    InnerPageState reduced(route, options.metadata);
    reduced.children.erase(reduced.children.begin() + static_cast<ptrdiff_t>(left_index + 1));
    reduced.separators.erase(reduced.separators.begin() + static_cast<ptrdiff_t>(left_index));
    MaintenanceChildren parents(options.metadata);
    status = partition_inner(ancestor.pid, parent->level, reduced, &parents);
    if (status.ok()) {
        ++inner_merges_;
    }
    *changed = *changed || status.ok();
    return status;
}

Status MaintenanceBuilder::merge_underfull(const MaintenancePath &path, bool *changed)
{
    for (size_t depth = path.size(); depth > 1; --depth) {
        auto status = merge_inner_pair(path[depth - 2], path[depth - 1].pid, changed);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::Ok();
}
} // namespace crowdb::tree::detail
