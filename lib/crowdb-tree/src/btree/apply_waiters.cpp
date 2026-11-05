// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "apply_waiters.h"

namespace crowdb::tree
{
Status ApplyWaiters::create(const std::shared_ptr<detail::ResourceBudget::Account> &account,
                            const std::shared_ptr<detail::AllocationReservation>   &metadata,
                            std::shared_ptr<ApplyWaiters>                          *out)
{
    auto result = detail::reserved_shared<ApplyWaiters>(metadata);
    for (auto category : {detail::BudgetClass::kHandoff, detail::BudgetClass::kRouting, detail::BudgetClass::kReclaim,
                          detail::BudgetClass::kGapRepair, detail::BudgetClass::kIo}) {
        auto &slot = result->slots_[static_cast<size_t>(category)];
        slot       = detail::ResourceBudget::try_register(account, category);
        if (!slot) {
            return Status::unavailable("host budget waiter capacity must be admitted before choosing");
        }
    }
    *out = std::move(result);
    return Status::Ok();
}

const detail::ResourceBudget::Registration *ApplyWaiters::get(detail::BudgetClass category) const noexcept
{
    const auto &slot = slots_[static_cast<size_t>(category)];
    return slot ? &*slot : nullptr;
}
} // namespace crowdb::tree
