// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/local/extent_ledger.h"
#include "load_budget_test_support.h"

namespace crowdb::tree::test_read
{
template <class Submit> ExtentLedgerResult await_ledger(Submit submit)
{
    ExtentLedgerResult result;
    std::atomic<bool>  done{false};
    auto               status = submit([&](ExtentLedgerResult value) {
        result = std::move(value);
        done.store(true, std::memory_order_release);
        done.notify_one();
    });
    if (!status.ok()) {
        return {.status = std::move(status), .extent = {}};
    }
    while (!done.load(std::memory_order_acquire)) {
        done.wait(false, std::memory_order_acquire);
    }
    return result;
}

struct LedgerFixture
{
    LoadCredits                   credits;
    std::shared_ptr<ExtentLedger> ledger;

    explicit LedgerFixture(ExtentLedgerLimits limits = {.backend_id = 1})
    {
        EXPECT_TRUE(ExtentLedger::create(limits, credits.credits.reservation(), &ledger).ok());
        EXPECT_LE(credits.budget.snapshot().allocated[LoadCredits::kBytes], ExtentLedger::creation_metadata_bound());
    }

    ExtentLedgerResult activate()
    {
        return await_ledger(
            [&](auto done) { return ledger->activate(credits.credits.reservation(), std::move(done)); });
    }

    ExtentLedgerResult recover(uint64_t address, uint64_t length)
    {
        return await_ledger([&](auto done) {
            return ledger->recover(address, length, credits.credits.reservation(), std::move(done));
        });
    }

    ExtentLedgerResult reserve(uint64_t length)
    {
        return await_ledger(
            [&](auto done) { return ledger->reserve(length, credits.credits.reservation(), std::move(done)); });
    }
};
} // namespace crowdb::tree::test_read
