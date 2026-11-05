// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_reservation.h"
#include "tree_work_counters.h"

#include <cassert>

namespace crowdb::tree::detail
{
void AllocationReservation::record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept
{
    const auto index = static_cast<size_t>(kind);
    if (kind == AllocationKind::kUnknown || index >= account_->physical_.size()) {
        return;
    }
    // The I/O admission partition counts operations, not storage bytes.
    assert(category_ != static_cast<size_t>(BudgetClass::kIo));
    auto &value = account_->physical_[index];
    if (acquired) {
        value.fetch_add(bytes, std::memory_order_relaxed);
    }
    else {
        [[maybe_unused]] const auto previous = value.fetch_sub(bytes, std::memory_order_relaxed);
        assert(previous >= bytes);
        record_work(account_->work_metrics(), TreeWorkCounter::MemoryReclaimedBytes, bytes, TreeWorkCause::Allocation);
    }
}

void ResourceBudget::record_allocation(const std::shared_ptr<Account> &account, AllocationKind kind, size_t bytes,
                                       bool acquired) noexcept
{
    const auto index = static_cast<size_t>(kind);
    if (!account || kind == AllocationKind::kUnknown || index >= account->physical_.size()) {
        return;
    }
    if (acquired) {
        account->physical_[index].fetch_add(bytes, std::memory_order_relaxed);
    }
    else {
        [[maybe_unused]] const auto previous = account->physical_[index].fetch_sub(bytes, std::memory_order_relaxed);
        assert(previous >= bytes);
        record_work(account->work_metrics(), TreeWorkCounter::MemoryReclaimedBytes, bytes, TreeWorkCause::Allocation);
    }
}

bool ResourceBudget::Credit::mark_allocation(AllocationKind kind) noexcept
{
    const auto index = static_cast<size_t>(kind);
    if (!account_ || !allocated_ || category_ == static_cast<size_t>(BudgetClass::kIo) ||
        kind_ != AllocationKind::kUnknown || kind == AllocationKind::kUnknown || index >= account_->physical_.size()) {
        return false;
    }
    kind_ = kind;
    account_->physical_[index].fetch_add(amount_, std::memory_order_relaxed);
    return true;
}
} // namespace crowdb::tree::detail
