// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "allocation_reservation.h"

namespace crowdb::tree
{
// One complete wait path, reserved before consensus or at Tree startup. Replay
// borrowers serialize only the current gap; high slots await ApplyProgress.
class ApplyWaiters
{
  public:
    static Status create(const std::shared_ptr<detail::ResourceBudget::Account> &account,
                         const std::shared_ptr<detail::AllocationReservation>   &metadata,
                         std::shared_ptr<ApplyWaiters>                          *out);
    [[nodiscard]] const detail::ResourceBudget::Registration *get(detail::BudgetClass category) const noexcept;

    [[nodiscard]] bool try_acquire() noexcept
    {
        return !busy_.test_and_set(std::memory_order_acq_rel);
    }

    void release() noexcept
    {
        busy_.clear(std::memory_order_release);
    }

  private:
    std::array<std::optional<detail::ResourceBudget::Registration>, detail::ResourceBudget::kClasses> slots_;
    std::atomic_flag busy_ = ATOMIC_FLAG_INIT;
};
} // namespace crowdb::tree
