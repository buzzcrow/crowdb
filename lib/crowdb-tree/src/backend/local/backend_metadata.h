// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"

namespace crowdb::tree::detail
{
// Ordinary unique_ptr deletion must release the object storage before its
// admission. A small allocation header keeps that owner past the destructor.
class BackendObjectStorage
{
  public:
    BackendObjectStorage(size_t bytes, size_t alignment, std::shared_ptr<AllocationBudget> budget);
    ~BackendObjectStorage();
    BackendObjectStorage(const BackendObjectStorage &)            = delete;
    BackendObjectStorage &operator=(const BackendObjectStorage &) = delete;

    [[nodiscard]] void *get() const noexcept
    {
        return pointer_;
    }

    [[nodiscard]] void *release() noexcept
    {
        return std::exchange(pointer_, nullptr);
    }

    static void destroy(void *pointer) noexcept;

  private:
    void *pointer_ = nullptr;
};
} // namespace crowdb::tree::detail
