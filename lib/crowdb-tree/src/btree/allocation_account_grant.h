// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_account.h"
#include "allocation_reservation.h"
#include "btree/allocation_bound.h"

namespace crowdb::tree::detail
{
// Per-operation backend capacity. Retained backend objects keep only this
// small budget and their stable account, never the Tree/operation/runtime.
// seal returns unused capacity; allocated nodes/fds keep their actual charge.
class BackendAllocationGrant final : public AllocationBudget
{
  public:
    BackendAllocationGrant(std::shared_ptr<AllocationAccount> owner, ResourceBudget::Credit credit);
    static Status create(std::shared_ptr<AllocationAccount> owner, ResourceBudget::Credit credit,
                         std::shared_ptr<BackendAllocationGrant> *out);

    [[nodiscard]] static constexpr size_t allocation_bound() noexcept
    {
        return shared_allocation_bound<BackendAllocationGrant>();
    }

    [[nodiscard]] bool        claim(size_t bytes) noexcept override;
    void                      release(size_t bytes) noexcept override;
    void                      record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept override;
    [[nodiscard]] const void *domain() const noexcept override;
    void                      seal() noexcept;

    [[nodiscard]] std::shared_ptr<TreeWorkMetrics> retirement_metrics() const noexcept override
    {
        return source_.account()->work_metrics();
    }

    [[nodiscard]] const std::shared_ptr<AllocationAccount> &account() const noexcept
    {
        return owner_;
    }

  private:
    std::shared_ptr<AllocationAccount> owner_;
    AllocationReservation              source_;
};
} // namespace crowdb::tree::detail
