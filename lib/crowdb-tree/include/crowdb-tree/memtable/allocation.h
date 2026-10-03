// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <atomic>
#include <memory>

namespace crowdb::tree
{
// Allocation accounting follows ownership, including descriptors and payloads
// awaiting epoch collection. Logical mutation events are aggregated per batch.
using AllocationCounter = std::atomic<size_t>;

class AllocationCharge
{
  public:
    AllocationCharge()                                    = default;
    AllocationCharge(const AllocationCharge &)            = delete;
    AllocationCharge &operator=(const AllocationCharge &) = delete;

    ~AllocationCharge()
    {
        if (counter_ != nullptr) {
            counter_->fetch_sub(bytes_, std::memory_order_relaxed);
        }
    }

    void set(std::shared_ptr<AllocationCounter> counter, size_t bytes)
    {
        if (counter_ != nullptr) {
            counter_->fetch_sub(bytes_, std::memory_order_relaxed);
        }
        counter_ = std::move(counter);
        bytes_   = bytes;
        counter_->fetch_add(bytes_, std::memory_order_relaxed);
    }

  private:
    std::shared_ptr<AllocationCounter> counter_;
    size_t                             bytes_ = 0;
};
} // namespace crowdb::tree
