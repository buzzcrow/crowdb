// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "flush_runtime.h"
#include "page_cache_identity.h"
#include "tree_metrics.h"

namespace crowdb::tree
{
struct PageReplacement;
struct PublishedPage;
class MappingView;

// A single live mapping owns this index. Old captures retain data owners only.
// All updates enter a reserved runtime continuation after preparing their nodes.
// The host owns the runtime through destruction and draining of this index.
class PageCacheResidency
{
    struct State;

  public:
    class Update;
    class Ready;
    explicit PageCacheResidency(std::shared_ptr<TreeResourceMetrics>        resources,
                                const std::shared_ptr<AllocationBudget>    &metadata = {},
                                detail::FlushRuntime                       *runtime  = nullptr,
                                std::shared_ptr<detail::FlushRuntime::Lane> lane     = {});
    ~PageCacheResidency();
    [[nodiscard]] std::shared_ptr<Update> prepare(std::span<const PageReplacement>         pages,
                                                  const std::shared_ptr<AllocationBudget> &metadata) const;
    [[nodiscard]] std::shared_ptr<Update> prepare_initial(const MappingView                       &mapping,
                                                          const std::shared_ptr<AllocationBudget> &metadata) const;
    [[nodiscard]] std::shared_ptr<Update> prepare_replacement(const MappingView                       &mapping,
                                                              const std::shared_ptr<AllocationBudget> &metadata) const;
    [[nodiscard]] static size_t           initial_allocation_bound(const MappingView &mapping) noexcept;
    static void                           publish(std::shared_ptr<Update> update) noexcept;
    [[nodiscard]] std::shared_ptr<Ready>  prepare_ready(const PageCacheKey                      &key,
                                                        const std::shared_ptr<AllocationBudget> &metadata) const;
    static void                           ready(const std::shared_ptr<Ready> &ready) noexcept;
    static void                           evict(const std::shared_ptr<Ready> &ready) noexcept;
    void                                  clear() noexcept;
    // Host-thread boundary: stop index producers, drain only this progress
    // registration and detach from the runtime. Late pin releases are safe.
    void                                                      close_and_drain() noexcept;
    [[nodiscard]] uint64_t                                    proxy_pages() const noexcept;
    [[nodiscard]] const std::shared_ptr<TreeResourceMetrics> &resources() const noexcept;
    [[nodiscard]] static size_t                               page_allocation_bound(size_t refs) noexcept;
    [[nodiscard]] static size_t                               allocation_bound() noexcept;
    [[nodiscard]] static size_t                               ready_allocation_bound() noexcept;

  private:
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
