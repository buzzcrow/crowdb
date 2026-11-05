// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/buffer.h"
#include "crowdb-tree/external_allocation.h"
#include "crowdb-tree/status.h"

namespace crowdb::tree
{
// A bridge created at a complete allocation boundary. Capacity is charged
// once, while slice bounds use initialized bytes and never expose spare
// capacity. A host reservation is mandatory before accepting the owner.
class ExternalOwnerBridge
{
  public:
    ExternalOwnerBridge(std::shared_ptr<const ExternalAllocation> allocation, size_t initialized_bytes)
        : allocation_(std::move(allocation)),
          initialized_bytes_(initialized_bytes)
    {
        if (!allocation_ || !allocation_->budget() || initialized_bytes_ > allocation_->capacity()) {
            throw std::invalid_argument("external owner requires a charged complete allocation");
        }
    }

    static Status create(const uint8_t *data, size_t initialized_bytes, size_t capacity,
                         ExternalAllocation::Owner owner, const std::shared_ptr<AllocationBudget> &budget,
                         std::shared_ptr<const ExternalOwnerBridge> *out) noexcept;

    [[nodiscard]] Status slice(size_t offset, size_t length, buffer *out) const noexcept;

    [[nodiscard]] const ExternalAllocation *identity() const noexcept
    {
        return allocation_.get();
    }

    [[nodiscard]] size_t capacity() const noexcept
    {
        return allocation_->capacity();
    }

  private:
    std::shared_ptr<const ExternalAllocation> allocation_;
    size_t                                    initialized_bytes_;
};
} // namespace crowdb::tree
