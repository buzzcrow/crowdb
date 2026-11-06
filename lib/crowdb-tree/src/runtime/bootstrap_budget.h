// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"

#include <array>

namespace crowdb::tree::detail
{
// A host lifecycle partition has no reference back to the admission ledger.
// Ledger storage, waiter slots and executor controls can therefore use this
// budget without making their allocator own the ledger it pays for.
class BootstrapBudget final : public AllocationBudget
{
  public:
    struct Snapshot
    {
        size_t                                                          allocated = 0;
        size_t                                                          reserved  = 0;
        std::array<size_t, static_cast<size_t>(AllocationKind::kCount)> physical{};
    };

    [[nodiscard]] static std::shared_ptr<BootstrapBudget> create(size_t capacity);
    [[nodiscard]] bool                                    claim(size_t bytes) noexcept override;
    void                                                  release(size_t bytes) noexcept override;
    void                   record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept override;
    [[nodiscard]] size_t   available() const noexcept;
    [[nodiscard]] Snapshot snapshot() const noexcept;
    void                   seal() noexcept;

  private:
    template <class T> friend class BootstrapControlAllocator;
    explicit BootstrapBudget(size_t capacity);
    ~BootstrapBudget() override;
    void                    release_control(void *pointer, size_t bytes, size_t alignment) noexcept;
    static constexpr size_t kSealed = size_t{1} << (std::numeric_limits<size_t>::digits - 1);
    const size_t            capacity_;
    std::atomic<size_t>     used_{sizeof(BootstrapBudget)};
    std::array<std::atomic<size_t>, static_cast<size_t>(AllocationKind::kCount)> physical_{};
};
} // namespace crowdb::tree::detail
