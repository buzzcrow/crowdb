// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "tree_runtime_internal.h"

namespace crowdb::tree
{
detail::ResourceBudget::Limits resource_limits(TreeResourceLimits limits) noexcept
{
    return {limits.handoff_bytes, limits.routing_bytes,    limits.reclaim_bytes,
            limits.writer_bytes,  limits.gap_repair_bytes, limits.io_operations};
}

namespace
{
detail::ResourceBudget::Limits operation_limits(const TreeRuntimeOptions &options) noexcept
{
    auto limits = resource_limits(options.limits);
    limits[static_cast<size_t>(detail::BudgetClass::kRouting)] -= options.lifecycle_bytes;
    return limits;
}

TreeResourceLimits public_limits(detail::ResourceBudget::Limits limits) noexcept
{
    return {.handoff_bytes    = limits[0],
            .routing_bytes    = limits[1],
            .reclaim_bytes    = limits[2],
            .writer_bytes     = limits[3],
            .gap_repair_bytes = limits[4],
            .io_operations    = limits[5]};
}
} // namespace

TreeRuntime::State::State(TreeRuntimeOptions options, std::shared_ptr<detail::BootstrapBudget> credit)
    : options(options),
      lifecycle(std::move(credit)),
      budget(operation_limits(options), options.max_waiters_per_class, lifecycle)
{
    const auto storage =
        detail::AllocationBound()
            .add(detail::FlushRuntime::allocation_bound(options.workers, options.cpu_queue_limit + 1))
            .add(detail::FlushRuntime::allocation_bound(options.backend_workers, options.backend_queue_limit))
            .add(2 * detail::shared_allocation_bound<detail::FlushRuntime>())
            .add(detail::shared_allocation_bound<TreeRuntime>())
            .add(detail::RetirementQueue::allocation_bound())
            .add(detail::shared_allocation_bound<detail::FlushRuntime::Lane>())
            .value();
    if (!storage) {
        throw std::invalid_argument("runtime storage size overflow");
    }
    if (*storage > lifecycle->available()) {
        throw std::bad_alloc();
    }
    cpu = std::allocate_shared<detail::FlushRuntime>(MetadataAllocator<detail::FlushRuntime>(lifecycle),
                                                     options.workers, options.cpu_queue_limit + 1, lifecycle);
    backend =
        std::allocate_shared<detail::FlushRuntime>(MetadataAllocator<detail::FlushRuntime>(lifecycle),
                                                   options.backend_workers, options.backend_queue_limit, lifecycle);
    auto retirement_lane =
        std::allocate_shared<detail::FlushRuntime::Lane>(MetadataAllocator<detail::FlushRuntime::Lane>(lifecycle), 1);
    auto status = detail::RetirementQueue::create(*cpu, std::move(retirement_lane), lifecycle, &retirement);
    if (!status.ok()) {
        throw std::bad_alloc();
    }
    budget.set_retirement_queue(retirement);
}

TreeRuntime::State::~State()
{
    stopped.store(true, std::memory_order_release);
    budget.close();
    retirement->close_and_drain();
    cpu->close();
    backend->close();
    cpu->drain();
    backend->drain();
    lifecycle->seal();
}

Status TreeRuntime::create(TreeRuntimeOptions options, std::shared_ptr<TreeRuntime> *out)
try {
    if (out == nullptr || options.workers == 0 || options.cpu_queue_limit == 0 || options.cpu_queue_limit == SIZE_MAX ||
        options.max_waiters_per_class == 0 || options.backend_workers == 0 || options.backend_queue_limit == 0 ||
        options.lifecycle_bytes == 0 || options.lifecycle_bytes > options.limits.routing_bytes) {
        return Status::invalid_argument();
    }
    auto lifecycle = detail::BootstrapBudget::create(options.lifecycle_bytes);
    auto state     = std::allocate_shared<State>(MetadataAllocator<State>(lifecycle), options, lifecycle);
    MetadataAllocator<TreeRuntime> allocator(lifecycle);
    auto                          *value = allocator.allocate(1);
    ::new (static_cast<void *>(value)) TreeRuntime(std::move(state));
    *out = std::shared_ptr<TreeRuntime>(
        value,
        [allocator](TreeRuntime *entry) mutable {
            std::destroy_at(entry);
            allocator.deallocate(entry, 1);
        },
        allocator);
    return {};
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (const std::invalid_argument &) {
    return Status::invalid_argument();
}
catch (...) {
    return Status::internal_error();
}

TreeRuntime::~TreeRuntime() = default;

void TreeRuntime::stop_admission() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->budget.close();
}

void TreeRuntime::drain_cpu() noexcept
{
    stop_admission();
    state_->cpu->close();
    state_->backend->close();
    state_->cpu->drain();
    state_->backend->drain();
}

bool TreeRuntime::stopped() const noexcept
{
    return state_->stopped.load(std::memory_order_acquire);
}

TreeRuntimeSnapshot TreeRuntime::snapshot() const noexcept
{
    auto       budget    = state_->budget.snapshot();
    const auto bootstrap = state_->lifecycle->snapshot();
    const auto routing   = static_cast<size_t>(detail::BudgetClass::kRouting);
    budget.allocated[routing] += bootstrap.allocated;
    budget.reserved[routing] += bootstrap.reserved;
    const auto cpu      = state_->cpu->snapshot();
    const auto backend  = state_->backend->snapshot();
    const auto executor = [](const detail::FlushRuntime::Snapshot &value) {
        return TreeExecutorSnapshot{.queued     = value.queued,
                                    .inflight   = value.inflight,
                                    .submitted  = value.submitted,
                                    .completed  = value.completed,
                                    .rejected   = value.rejected,
                                    .queue_ns   = value.queue_ns,
                                    .execute_ns = value.execute_ns};
    };
    return {.reserved         = public_limits(budget.reserved),
            .allocated        = public_limits(budget.allocated),
            .bindings         = state_->bindings.load(std::memory_order_relaxed),
            .queued           = cpu.queued + backend.queued,
            .inflight         = cpu.inflight + backend.inflight,
            .stopped          = stopped(),
            .cpu              = executor(cpu),
            .backend          = executor(backend),
            .budget_waiters   = budget.waiters,
            .budget_grants    = budget.grants,
            .budget_cancelled = budget.cancelled,
            .budget_wait_ns   = budget.wait_ns};
}

const TreeRuntimeOptions &TreeRuntime::options() const noexcept
{
    return state_->options;
}

ResourceUsageSnapshot TreeRuntime::resource_usage() const noexcept
{
    auto       result    = state_->budget.resource_usage();
    const auto bootstrap = state_->lifecycle->snapshot();
    result[1].limit      = state_->options.limits.routing_bytes;
    result[1].reserved += bootstrap.reserved;
    result[1].allocated += bootstrap.allocated;
    result[1].used += bootstrap.reserved + bootstrap.allocated;
    const auto cpu     = state_->cpu->snapshot();
    const auto backend = state_->backend->snapshot();
    result[6]          = {.limit       = state_->options.cpu_queue_limit + 1,
                          .used        = state_->cpu->queue_permits(),
                          .denied_host = cpu.rejected,
                          .waits       = cpu.submitted,
                          .wait_ns     = cpu.queue_ns,
                          .waiters     = cpu.queued};
    result[7]          = {.limit       = state_->options.backend_queue_limit,
                          .used        = state_->backend->queue_permits(),
                          .denied_host = backend.rejected,
                          .waits       = backend.submitted,
                          .wait_ns     = backend.queue_ns,
                          .waiters     = backend.queued};
    result[8]          = {.limit = state_->options.workers, .used = cpu.inflight};
    result[9]          = {.limit = state_->options.backend_workers, .used = backend.inflight};
    return result;
}

Status TreeRuntime::validate_binding(const TreeBindingOptions &options) const noexcept
{
    if (stopped()) {
        return Status::unavailable();
    }
    if (options.cpu_queue_limit == 0 || options.backend_queue_limit == 0 || options.lifecycle_bytes == 0 ||
        options.lifecycle_bytes > options.limits.routing_bytes) {
        return Status::invalid_argument();
    }
    const auto tree = resource_limits(options.limits);
    const auto host = resource_limits(state_->options.limits);
    for (size_t index = 0; index < tree.size(); ++index) {
        if (tree[index] > host[index]) {
            return Status::invalid_argument();
        }
    }
    return {};
}
} // namespace crowdb::tree
