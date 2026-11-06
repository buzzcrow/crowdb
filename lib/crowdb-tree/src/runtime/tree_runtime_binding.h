// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_reservation.h"
#include "btree/apply_dispatcher.h"
#include "btree/flush_runtime.h"
#include "crowdb-tree/tree_runtime.h"

namespace crowdb::tree
{
// Keep this owner in every Tree operation through its final backend ticket.
// It provides separate Tree quotas while preserving the host ledger domain.
class TreeRuntimeBinding
{
  public:
    static Status create(std::shared_ptr<TreeRuntime> runtime, TreeBindingOptions options,
                         std::shared_ptr<TreeRuntimeBinding> *out);
    ~TreeRuntimeBinding();
    TreeRuntimeBinding(const TreeRuntimeBinding &)            = delete;
    TreeRuntimeBinding &operator=(const TreeRuntimeBinding &) = delete;

    [[nodiscard]] const std::shared_ptr<detail::FlushRuntime> &cpu() const noexcept
    {
        return cpu_;
    }

    [[nodiscard]] const std::shared_ptr<detail::FlushRuntime::Lane> &lane() const noexcept
    {
        return lane_;
    }

    [[nodiscard]] const std::shared_ptr<detail::FlushRuntime> &backend_cpu() const noexcept
    {
        return backend_cpu_;
    }

    [[nodiscard]] const std::shared_ptr<detail::FlushRuntime::Lane> &backend_lane() const noexcept
    {
        return backend_lane_;
    }

    [[nodiscard]] const std::shared_ptr<ApplyDispatcher> &backend_progress() const noexcept
    {
        return backend_progress_;
    }

    [[nodiscard]] const std::shared_ptr<TreeWorkMetrics> &work_metrics() const noexcept
    {
        return work_metrics_;
    }

    [[nodiscard]] const std::shared_ptr<TreeResourceMetrics> &resource_metrics() const noexcept
    {
        return resource_metrics_;
    }

    // Host-only: all operation tickets must drain before joining scheduler tails.
    void                                drain() const noexcept;
    [[nodiscard]] ResourceUsageSnapshot resource_usage() const noexcept;

    [[nodiscard]] const std::shared_ptr<detail::ResourceBudget::Account> &account() const noexcept
    {
        return account_;
    }

    [[nodiscard]] const std::shared_ptr<detail::AllocationReservation> &lifecycle_budget() const noexcept
    {
        return lifecycle_;
    }

    [[nodiscard]] const TreeBindingOptions &options() const noexcept
    {
        return options_;
    }

  private:
    TreeRuntimeBinding(std::shared_ptr<TreeRuntime> runtime, TreeBindingOptions options,
                       std::shared_ptr<detail::ResourceBudget::Account> account,
                       std::shared_ptr<detail::AllocationReservation>   lifecycle,
                       std::shared_ptr<detail::FlushRuntime::Lane>      lane,
                       std::shared_ptr<detail::FlushRuntime::Lane>      backend_lane);

    std::shared_ptr<TreeRuntime>                     owner_;
    TreeBindingOptions                               options_;
    std::shared_ptr<detail::ResourceBudget::Account> account_;
    std::shared_ptr<detail::AllocationReservation>   lifecycle_;
    std::shared_ptr<TreeWorkMetrics>                 work_metrics_;
    std::shared_ptr<TreeResourceMetrics>             resource_metrics_;
    std::shared_ptr<detail::FlushRuntime>            cpu_;
    std::shared_ptr<detail::FlushRuntime::Lane>      lane_;
    std::shared_ptr<detail::FlushRuntime>            backend_cpu_;
    std::shared_ptr<detail::FlushRuntime::Lane>      backend_lane_;
    std::shared_ptr<ApplyDispatcher>                 backend_progress_;
};
} // namespace crowdb::tree
