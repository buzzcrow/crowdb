// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"

#include <exec/detail/pool_resource.hpp>
#include <memory_resource>

namespace crowdb::tree::detail
{
// The host owns this resource until all worker joins and pool container frees.
// Thread-system stacks/TLS are managed by pthread; engine worker controls and
// every pool container allocation use this resource.
class PoolStorage final : public std::pmr::memory_resource
{
  public:
    explicit PoolStorage(std::shared_ptr<AllocationBudget> budget) : budget_(std::move(budget))
    {
    }

    [[nodiscard]] exec::pool_thread_factory thread_factory() noexcept;
    [[nodiscard]] static size_t             worker_bytes() noexcept;

  private:
    struct Worker;
    static void *start(void *resource, void (*execute)(void *, uint32_t) noexcept, void *context, uint32_t index);
    static void  join(void *pointer) noexcept;
    static std::thread::id            identity(void *pointer) noexcept;
    static void                      *run(void *pointer) noexcept;
    void                             *do_allocate(size_t bytes, size_t alignment) override;
    void                              do_deallocate(void *pointer, size_t bytes, size_t alignment) override;
    [[nodiscard]] bool                do_is_equal(const std::pmr::memory_resource &other) const noexcept override;
    std::shared_ptr<AllocationBudget> budget_;
};
} // namespace crowdb::tree::detail
