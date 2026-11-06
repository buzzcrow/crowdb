// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_load.h"
#include "btree/tree_metrics.h"

#include <gtest/gtest.h>

namespace crowdb::tree::test_read
{
struct LoadCredits
{
    LoadCredits()
    {
        account->set_resource_metrics(resources);
    }

    std::shared_ptr<TreeResourceMetrics> resources = std::make_shared<TreeResourceMetrics>();

    static constexpr size_t kBytes = static_cast<size_t>(detail::BudgetClass::kRouting);
    static constexpr size_t kIo    = static_cast<size_t>(detail::BudgetClass::kIo);

    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result{};
        result[kBytes] = 4 * 1024 * 1024;
        result[kIo]    = 1;
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());
    PageLoadBudget credits{.bytes = reserve(detail::BudgetClass::kRouting, limits()[kBytes]),
                           .io    = reserve(detail::BudgetClass::kIo, limits()[kIo])};

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> reserve(detail::BudgetClass category,
                                                                         size_t              amount) const
    {
        auto credit = detail::ResourceBudget::try_reserve(account, category, amount);
        if (!credit) {
            ADD_FAILURE() << "test load reservation failed";
            return {};
        }
        return std::make_shared<detail::AllocationReservation>(std::move(credit.value()));
    }

    void seal() const
    {
        credits.reservation()->seal();
        credits.io->seal();
    }
};
} // namespace crowdb::tree::test_read
