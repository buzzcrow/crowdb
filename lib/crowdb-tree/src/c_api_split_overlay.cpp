// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/prepared_split_overlay.h"
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/split_overlay_c_api.h"

struct ct_prepared_split_overlay
{
    crowdb::tree::AllocationLease                       allocation;
    std::shared_ptr<crowdb::tree::PreparedSplitOverlay> owner;
};

namespace
{
ct_status code(const crowdb::tree::Status &status) noexcept
{
    return static_cast<ct_status>(status.code());
}
} // namespace

extern "C" ct_status ct_arm_split_overlay(ct_tree *source, ct_tree *destination,
                                          const ct_split_overlay_limits_v1 *limits, ct_prepared_split_overlay **out,
                                          uint64_t *start_frontier)
try {
    using namespace crowdb::tree;
    auto *parent_tree = c_api_tree(source);
    auto *child_tree  = c_api_tree(destination);
    if (parent_tree == nullptr || child_tree == nullptr || limits == nullptr || limits->version != 1 ||
        limits->struct_size != sizeof(*limits) || limits->max_records > SIZE_MAX || limits->max_bytes > SIZE_MAX ||
        out == nullptr || *out != nullptr || start_frontier == nullptr) {
        return code(Status::invalid_argument());
    }
    auto *parent = TreeCoreAccess::get(*parent_tree);
    auto *child  = TreeCoreAccess::get(*child_tree);
    if (parent == nullptr || child == nullptr) {
        return code(Status::invalid_argument());
    }
    auto reserved = detail::ResourceBudget::try_reserve(
        child->binding()->account(), detail::BudgetClass::kRouting,
        sizeof(ct_prepared_split_overlay) + detail::shared_allocation_bound<detail::AllocationReservation>());
    if (!reserved) {
        return code(Status::resource_exhausted());
    }
    auto            metadata = detail::AllocationReservation::create(std::move(*reserved));
    AllocationLease charge(metadata, sizeof(ct_prepared_split_overlay), AllocationKind::kMetadata);
    auto            result = std::make_unique<ct_prepared_split_overlay>();
    result->allocation     = std::move(charge);
    auto status            = PreparedSplitOverlay::arm(
        *parent, *child,
        {.max_records = static_cast<size_t>(limits->max_records), .max_bytes = static_cast<size_t>(limits->max_bytes)},
        &result->owner, start_frontier);
    if (!status.ok()) {
        return code(status);
    }
    metadata->seal();
    *out = result.release();
    return code(Status::Ok());
}
catch (const std::bad_alloc &) {
    return code(crowdb::tree::Status::resource_exhausted());
}
catch (...) {
    return code(crowdb::tree::Status::internal_error());
}

extern "C" ct_status ct_prepared_split_overlay_prepare(ct_prepared_split_overlay *prepared, uint64_t replayed_prefix)
try {
    return code(prepared != nullptr && prepared->owner ? prepared->owner->prepare(replayed_prefix)
                                                       : crowdb::tree::Status::invalid_argument());
}
catch (const std::bad_alloc &) {
    return code(crowdb::tree::Status::resource_exhausted());
}
catch (...) {
    return code(crowdb::tree::Status::internal_error());
}

extern "C" ct_status ct_prepared_split_overlay_try_install(ct_prepared_split_overlay *prepared, uint64_t frontier,
                                                           ct_installed_split_overlay_v1 *out)
{
    using namespace crowdb::tree;
    if (prepared == nullptr || !prepared->owner || out == nullptr || out->version != 1 ||
        out->struct_size != sizeof(*out)) {
        return code(Status::invalid_argument());
    }
    InstalledSplitOverlay installed;
    auto                  status = prepared->owner->try_install(frontier, &installed);
    if (status.ok()) {
        out->generation = installed.generation;
        out->frontier   = installed.frontier;
    }
    return code(status);
}

extern "C" int ct_prepared_split_overlay_committed_frontier(const ct_prepared_split_overlay *prepared, uint64_t *out)
{
    if (prepared != nullptr && prepared->owner && out != nullptr) {
        if (auto frontier = prepared->owner->committed_frontier()) {
            *out = *frontier;
            return 1;
        }
    }
    return 0;
}

extern "C" void ct_prepared_split_overlay_destroy(ct_prepared_split_overlay *prepared)
{
    delete prepared;
}
