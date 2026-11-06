// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "bootstrap_budget.h"
#include "btree/allocation_reservation.h"
#include "btree/flush_runtime.h"
#include "btree/retirement_queue.h"
#include "crowdb-tree/tree_runtime.h"

namespace crowdb::tree
{
[[nodiscard]] detail::ResourceBudget::Limits resource_limits(TreeResourceLimits limits) noexcept;

struct TreeRuntime::State
{
    State(TreeRuntimeOptions options, std::shared_ptr<detail::BootstrapBudget> credit);
    ~State();

    TreeRuntimeOptions                       options;
    std::shared_ptr<detail::BootstrapBudget> lifecycle;
    detail::ResourceBudget                   budget;
    std::shared_ptr<detail::FlushRuntime>    cpu;
    std::shared_ptr<detail::FlushRuntime>    backend;
    std::shared_ptr<detail::RetirementQueue> retirement;
    std::atomic<uint64_t>                    bindings{0};
    std::atomic<bool>                        stopped{false};
};

struct TreeRuntimeAccess
{
    static std::shared_ptr<AllocationBudget> lifecycle(const TreeRuntime &runtime) noexcept
    {
        return runtime.state_->lifecycle;
    }
};
} // namespace crowdb::tree
