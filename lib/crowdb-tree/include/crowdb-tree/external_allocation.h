// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"

#include <cstdint>
#include <stdexcept>

namespace crowdb::tree
{
// Construct once at the allocation boundary, before making slices. Ownership
// of the complete allocation, including spare capacity, is transferred here.
// Every retained slice shares this identity and therefore one payload charge.
class ExternalAllocation
{
  public:
    using Owner = std::unique_ptr<void, void (*)(void *)>;

    ExternalAllocation(const uint8_t *data, size_t capacity, Owner owner, std::shared_ptr<AllocationBudget> budget)
        : budget_(std::move(budget)),
          credit_(budget_, capacity, AllocationKind::kPayload),
          owner_(std::move(owner)),
          data_(data),
          capacity_(capacity)
    {
        if ((capacity_ != 0 && data_ == nullptr) || !owner_ || owner_.get_deleter() == nullptr) {
            throw std::invalid_argument("external allocation requires a complete allocation owner");
        }
    }

    [[nodiscard]] static std::shared_ptr<const ExternalAllocation>
    create(const uint8_t *data, size_t capacity, Owner owner, const std::shared_ptr<AllocationBudget> &budget)
    {
        return std::allocate_shared<ExternalAllocation>(MetadataAllocator<ExternalAllocation>(budget), data, capacity,
                                                        std::move(owner), budget);
    }

    [[nodiscard]] const uint8_t *data() const noexcept
    {
        return data_;
    }

    [[nodiscard]] size_t capacity() const noexcept
    {
        return capacity_;
    }

    [[nodiscard]] const std::shared_ptr<AllocationBudget> &budget() const noexcept
    {
        return budget_;
    }

    [[nodiscard]] bool charged_to(const std::shared_ptr<AllocationBudget> &budget) const noexcept
    {
        return budget_ && budget && budget_->domain() == budget->domain();
    }

  private:
    std::shared_ptr<AllocationBudget> budget_;
    AllocationLease                   credit_;

    Owner          owner_;
    const uint8_t *data_;
    size_t         capacity_;
};
} // namespace crowdb::tree
