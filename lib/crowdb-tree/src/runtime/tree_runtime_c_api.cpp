// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_runtime_c_api.h"

#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/c_api.h"
#include "default_runtime.h"
#include "tree_runtime_internal.h"

#include <algorithm>

struct ct_runtime
{
    std::shared_ptr<crowdb::tree::TreeRuntime> owner;
};

namespace
{
using namespace crowdb::tree;

TreeResourceLimits from_c(ct_resource_limits limits) noexcept
{
    return {.handoff_bytes    = limits.handoff_bytes,
            .routing_bytes    = limits.routing_bytes,
            .reclaim_bytes    = limits.reclaim_bytes,
            .writer_bytes     = limits.writer_bytes,
            .gap_repair_bytes = limits.gap_repair_bytes,
            .io_operations    = limits.io_operations};
}

ct_resource_limits to_c(TreeResourceLimits limits) noexcept
{
    return {.handoff_bytes    = limits.handoff_bytes,
            .routing_bytes    = limits.routing_bytes,
            .reclaim_bytes    = limits.reclaim_bytes,
            .writer_bytes     = limits.writer_bytes,
            .gap_repair_bytes = limits.gap_repair_bytes,
            .io_operations    = limits.io_operations};
}

ct_executor_snapshot_v1 to_c(const TreeExecutorSnapshot &value) noexcept
{
    return {.queued     = value.queued,
            .inflight   = value.inflight,
            .submitted  = value.submitted,
            .completed  = value.completed,
            .rejected   = value.rejected,
            .queue_ns   = value.queue_ns,
            .execute_ns = value.execute_ns};
}

int32_t copy_usage(const ResourceUsageSnapshot &value, uint32_t count, uint64_t scope_id,
                   ct_resource_usage_snapshot_v1 *out)
{
    ct_resource_usage_snapshot_v1 result{.struct_size   = sizeof(*out),
                                         .version       = 1,
                                         .resource_mask = (uint32_t{1} << count) - 1,
                                         .scope_id      = scope_id,
                                         .resources     = {}};
    for (size_t i = 0; i < count; ++i) {
        const auto &item    = value[i];
        result.resources[i] = {.limit       = item.limit,
                               .used        = item.used,
                               .reserved    = item.reserved,
                               .allocated   = item.allocated,
                               .denied_tree = item.denied_tree,
                               .denied_host = item.denied_host,
                               .waits       = item.waits,
                               .wait_ns     = item.wait_ns,
                               .waiters     = item.waiters};
    }
    *out = result;
    return 0;
}
} // namespace

int32_t ct_runtime_resource_usage(const ct_runtime *runtime, ct_resource_usage_snapshot_v1 *out)
{
    if (runtime == nullptr || out == nullptr || out->struct_size != sizeof(*out) || out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    return copy_usage(runtime->owner->resource_usage(), 10, 0, out);
}

int32_t ct_tree_resource_usage(ct_tree *tree, ct_resource_usage_snapshot_v1 *out)
{
    if (tree == nullptr || out == nullptr || out->struct_size != sizeof(*out) || out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    const auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    ct_resource_usage_snapshot_v1 result{};
    copy_usage(core->binding()->resource_usage(), 8, core->binding()->account()->scope_id(), &result);
    const auto cache = core->cache()->snapshot();
    result.resource_mask |= (uint32_t{7} << 10);
    result.resources[10].limit     = core->config().buffer_pool_bytes;
    result.resources[10].used      = cache.resident_bytes;
    result.resources[11].limit     = core->cache()->retained_page_limit();
    result.resources[11].used      = std::min(result.resources[11].limit, cache.resident_pages + cache.inflight);
    result.resources[11].allocated = std::min(result.resources[11].used, cache.resident_pages);
    result.resources[11].reserved  = result.resources[11].used - result.resources[11].allocated;
    result.resources[12].limit     = core->binding()->options().cpu_queue_limit;
    result.resources[12].used      = std::min(result.resources[12].limit, cache.request_slots);
    result.resources[12].reserved  = std::min(result.resources[12].used, cache.reserved_waiters);
    result.resources[12].allocated = result.resources[12].used - result.resources[12].reserved;
    result.resources[12].waiters   = cache.waiters;
    const auto admission           = core->admission_resource_usage();
    result.resource_mask |= uint32_t{127} << 13;
    for (size_t index = 0; index < admission.size(); ++index) {
        const auto &value = admission[index];
        auto       &row   = result.resources[13 + index];
        row.limit         = value.limit;
        row.used          = value.used;
        row.denied_tree   = value.denied_tree;
    }
    *out = result;
    return 0;
}

int32_t ct_runtime_snapshot(const ct_runtime *runtime, ct_runtime_snapshot_v1 *out)
{
    if (runtime == nullptr || out == nullptr || out->struct_size != sizeof(*out) || out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    const auto value = runtime->owner->snapshot();
    *out             = {.struct_size      = sizeof(*out),
                        .version          = 1,
                        .stopped          = static_cast<uint32_t>(value.stopped),
                        .reserved         = to_c(value.reserved),
                        .allocated        = to_c(value.allocated),
                        .bindings         = value.bindings,
                        .cpu              = to_c(value.cpu),
                        .backend          = to_c(value.backend),
                        .budget_waiters   = value.budget_waiters,
                        .budget_grants    = value.budget_grants,
                        .budget_cancelled = value.budget_cancelled,
                        .budget_wait_ns   = value.budget_wait_ns};
    return 0;
}

void ct_runtime_options_default(ct_runtime_options *out)
{
    if (out != nullptr) {
        const TreeRuntimeOptions options;
        *out = {.struct_size           = sizeof(*out),
                .workers               = options.workers,
                .cpu_queue_limit       = options.cpu_queue_limit,
                .max_waiters_per_class = options.max_waiters_per_class,
                .lifecycle_bytes       = options.lifecycle_bytes,
                .limits                = to_c(options.limits),
                .backend_workers       = options.backend_workers,
                .backend_queue_limit   = options.backend_queue_limit};
    }
}

void ct_tree_runtime_options_default(ct_tree_runtime_options *out)
{
    if (out != nullptr) {
        const TreeBindingOptions options;
        *out = {.struct_size         = sizeof(*out),
                .cpu_queue_limit     = options.cpu_queue_limit,
                .lifecycle_bytes     = options.lifecycle_bytes,
                .limits              = to_c(options.limits),
                .backend_queue_limit = options.backend_queue_limit};
    }
}

int32_t ct_runtime_create(const ct_runtime_options *options, ct_runtime **out)
try {
    if (out == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out = nullptr;
    TreeRuntimeOptions config;
    if (options != nullptr) {
        if (options->struct_size != sizeof(*options)) {
            return static_cast<int32_t>(Code::kInvalidArgument);
        }
        config = {.workers               = options->workers,
                  .cpu_queue_limit       = options->cpu_queue_limit,
                  .max_waiters_per_class = options->max_waiters_per_class,
                  .backend_workers       = options->backend_workers,
                  .backend_queue_limit   = options->backend_queue_limit,
                  .lifecycle_bytes       = options->lifecycle_bytes,
                  .limits                = from_c(options->limits)};
    }
    std::shared_ptr<TreeRuntime> owner;
    auto                         status = TreeRuntime::create(config, &owner);
    if (!status.ok()) {
        return static_cast<int32_t>(status.code());
    }
    MetadataAllocator<ct_runtime> allocator(TreeRuntimeAccess::lifecycle(*owner));
    auto                         *handle = allocator.allocate(1);
    ::new (static_cast<void *>(handle)) ct_runtime{std::move(owner)};
    *out = handle;
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}

void ct_runtime_stop_admission(ct_runtime *runtime)
{
    if (runtime != nullptr) {
        runtime->owner->stop_admission();
    }
}

void ct_runtime_drain_cpu(ct_runtime *runtime)
{
    if (runtime != nullptr) {
        runtime->owner->drain_cpu();
    }
}

void ct_runtime_free(ct_runtime *runtime)
{
    if (runtime != nullptr) {
        MetadataAllocator<ct_runtime> allocator(TreeRuntimeAccess::lifecycle(*runtime->owner));
        std::destroy_at(runtime);
        allocator.deallocate(runtime, 1);
    }
}

namespace crowdb::tree
{
Status runtime_host_allocation_budget(ct_runtime *runtime, std::shared_ptr<AllocationBudget> *out) noexcept
{
    if (runtime == nullptr || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    if (runtime->owner->stopped()) {
        return Status::unavailable();
    }
    *out = TreeRuntimeAccess::lifecycle(*runtime->owner);
    return Status::Ok();
}

Status configure_tree_runtime(ct_runtime *runtime, const ct_tree_runtime_options *options, Config *out) noexcept
try {
    if (out == nullptr) {
        return Status::invalid_argument();
    }
    TreeBindingOptions binding;
    if (options != nullptr) {
        if (options->struct_size != sizeof(*options)) {
            return Status::invalid_argument();
        }
        binding = {.cpu_queue_limit     = options->cpu_queue_limit,
                   .backend_queue_limit = options->backend_queue_limit,
                   .lifecycle_bytes     = options->lifecycle_bytes,
                   .limits              = from_c(options->limits)};
    }
    Config configured;
    if (runtime != nullptr) {
        configured.runtime = runtime->owner;
    }
    auto status = configure_default_runtime(&configured);
    if (!status.ok()) {
        return status;
    }
    status = configured.runtime->validate_binding(binding);
    if (!status.ok()) {
        return status;
    }
    out->runtime         = std::move(configured.runtime);
    out->runtime_binding = binding;
    return {};
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (...) {
    return Status::internal_error();
}
} // namespace crowdb::tree

ct_status ct_open_with_runtime(const ct_options *opt, ct_runtime *runtime, const ct_tree_runtime_options *binding,
                               ct_tree **out)
try {
    if (out == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out = nullptr;
    if (opt == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    return ct_open_configured(opt, runtime, binding, out);
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}

ct_status ct_rebuild_range_with_runtime(ct_tree *source, const ct_options *opt, ct_runtime *runtime,
                                        const ct_tree_runtime_options *binding, ct_tree **out,
                                        ct_range_rebuild_stats *stats)
{
    if (out != nullptr) {
        *out = nullptr;
    }
    if (stats != nullptr) {
        *stats = {};
    }
    if (source == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    if (opt == nullptr || out == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    Config config;
    auto   status = configure_tree_runtime(runtime, binding, &config);
    if (!status.ok()) {
        return static_cast<int32_t>(status.code());
    }
    return ct_rebuild_configured(source, opt, std::move(config), out, stats);
}
