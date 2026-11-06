// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account.h"
#include "allocation_reservation.h"

#include <cassert>

namespace crowdb::tree::detail
{
bool AllocationAccount::adopt_existing(const std::shared_ptr<ResourceBudget::Account> &source, BudgetClass category,
                                       size_t bytes) noexcept
{
    const auto entered = ingress_.fetch_add(1, std::memory_order_acq_rel);
    if ((entered & kClosed) != 0) {
        leave();
        return false;
    }
    const bool accepted = account_ && category == category_ &&
                          ResourceBudget::transfer_allocation(source, account_, category_, bytes, {});
    if (accepted) {
        allocated_.fetch_add(bytes, std::memory_order_relaxed);
    }
    leave();
    return accepted;
}

bool AllocationAccount::adopt_allocated(ResourceBudget::Credit *credit) noexcept
{
    if (credit == nullptr || !credit->allocated_ || !credit->account_ || credit->kind_ != AllocationKind::kUnknown ||
        !adopt_existing(credit->account_, static_cast<BudgetClass>(credit->category_), credit->amount_)) {
        return false;
    }
    credit->amount_ = 0;
    credit->account_.reset();
    return true;
}

bool AllocationAccount::claim_from(AllocationReservation &source, size_t bytes) noexcept
{
    if (!source.claim(bytes)) {
        return false;
    }
    if (adopt_existing(source.account(), source.category(), bytes)) {
        return true;
    }
    source.release(bytes);
    return false;
}

void AllocationAccount::release_to(AllocationReservation &source, size_t bytes) noexcept
{
    const auto entered = ingress_.fetch_add(1, std::memory_order_acq_rel);
    if ((entered & kClosed) != 0) {
        // The operation normally seals before a backend rebind. If a final
        // owner races that handoff, return its capacity through the same
        // bounded pending-release path instead of reviving a source reserve.
        pending_release_.fetch_add(bytes, std::memory_order_relaxed);
        ingress_.fetch_or(kDirty, std::memory_order_release);
        leave();
        return;
    }
    [[maybe_unused]] const auto before = allocated_.fetch_sub(bytes, std::memory_order_relaxed);
    assert(before >= bytes);
    if (ResourceBudget::transfer_allocation(account_, source.account(), category_, bytes, {})) {
        source.release(bytes);
    }
    else {
        ResourceBudget::release_allocation(account_, category_, bytes);
    }
    leave();
}
} // namespace crowdb::tree::detail
