// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"
#include "retirement_traits.h"

#include <cstddef>
#include <limits>
#include <optional>

namespace crowdb::tree::detail
{
// Admission arithmetic never wraps into a smaller reservation. Once an
// operand overflows, no later operation can turn the result into a valid bound.
class AllocationBound
{
  public:
    explicit AllocationBound(size_t bytes = 0) : bytes_(bytes), valid_(bytes <= kLimit)
    {
    }

    AllocationBound &add(size_t bytes) noexcept
    {
        if (!valid_ || bytes > kLimit - bytes_) {
            valid_ = false;
        }
        else {
            bytes_ += bytes;
        }
        return *this;
    }

    AllocationBound &add_product(size_t count, size_t bytes) noexcept
    {
        if (bytes != 0 && count > kLimit / bytes) {
            valid_ = false;
            return *this;
        }
        return add(count * bytes);
    }

    AllocationBound &add(std::optional<size_t> bytes) noexcept
    {
        if (!bytes) {
            valid_ = false;
            return *this;
        }
        return add(*bytes);
    }

    [[nodiscard]] std::optional<size_t> value() const noexcept
    {
        return valid_ ? std::optional(bytes_) : std::nullopt;
    }

  private:
    // AllocationReservation uses the high bit to mark a sealed owner.
    static constexpr size_t kLimit = std::numeric_limits<size_t>::max() / 2;
    size_t                  bytes_;
    bool                    valid_;
};

// Control-block/allocator/alignment allowance for the supported standard
// libraries. Payload and container capacities are counted separately.
template <class T> constexpr size_t shared_allocation_bound() noexcept
{
    const auto storage = sizeof(T) + (RetirementTraits<T>::enabled ? 256 : 128);
    return storage;
}
} // namespace crowdb::tree::detail
