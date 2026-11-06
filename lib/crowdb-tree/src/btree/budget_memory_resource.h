// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"

#include <atomic>
#include <memory_resource>

namespace crowdb::tree::detail
{
// Publication owns allocation admission. Each allocation embeds its original
// credit owner, so later transactions and off-thread retirement can free it
// after that reservation has sealed. No allocator binding escapes a commit.
class BudgetMemoryResource final : public std::pmr::memory_resource
{
  public:
    explicit BudgetMemoryResource(std::pmr::memory_resource *upstream = std::pmr::get_default_resource());

    class Scope
    {
      public:
        Scope(BudgetMemoryResource &resource, std::shared_ptr<AllocationBudget> budget);
        ~Scope();
        Scope(const Scope &)            = delete;
        Scope &operator=(const Scope &) = delete;

      private:
        BudgetMemoryResource             &resource_;
        std::shared_ptr<AllocationBudget> previous_;
    };

    [[nodiscard]] size_t retained_bytes() const noexcept
    {
        return retained_bytes_.load(std::memory_order_relaxed);
    }

  private:
    struct Header
    {
        AllocationLease credit;
    };

    void              *do_allocate(size_t bytes, size_t alignment) override;
    void               do_deallocate(void *pointer, size_t bytes, size_t alignment) override;
    [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override;

    std::pmr::memory_resource        *upstream_;
    std::shared_ptr<AllocationBudget> budget_;
    std::atomic<size_t>               retained_bytes_{0};
};
} // namespace crowdb::tree::detail
