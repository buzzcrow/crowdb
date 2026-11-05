// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"

#include <memory_resource>

namespace crowdb::tree::detail
{
// PMR ownership follows the caller's paid object. Both real allocation and
// actual free are reported; admission remains separate from observation.
class AllocationMemoryResource final : public std::pmr::memory_resource
{
  public:
    explicit AllocationMemoryResource(std::shared_ptr<AllocationBudget> budget,
                                      AllocationKind                    kind = AllocationKind::kMetadata)
        : budget_(std::move(budget)),
          kind_(kind)
    {
    }

  private:
    void                             *do_allocate(size_t bytes, size_t alignment) override;
    void                              do_deallocate(void *pointer, size_t bytes, size_t alignment) override;
    [[nodiscard]] bool                do_is_equal(const std::pmr::memory_resource &other) const noexcept override;
    std::shared_ptr<AllocationBudget> budget_;
    AllocationKind                    kind_;
};
} // namespace crowdb::tree::detail
