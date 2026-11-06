// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "load_budget_test_support.h"
#include "published_read_test_support.h"

#include <thread>

namespace crowdb::tree
{
namespace
{
using namespace test_read;

TEST(PublishedReadBudget, FreshWorkerReadsSealedSourceWithItsOwnCreditAndRetainsResultKey)
{
    LoadCredits credits;
    auto        raw = credits.credits.reservation()->take_reserved(16384);
    if (!raw) {
        FAIL() << "initial source credit unavailable";
    }
    auto              initial = detail::AllocationReservation::create(std::move(*raw));
    auto              table   = std::make_shared<MemTable>(1, nullptr, initial);
    const std::string key(80, 'k');
    ASSERT_TRUE(table->upsert(key, 1, encode_cell_buf(1, OpKind::kPut, "value")));
    table->close();
    initial->seal();
    initial.reset();
    auto                              captured = view(page({}), {table});
    std::optional<PublishedScanEntry> retained;
    std::jthread                      worker([&] {
        PublishedReadLimits limits;
        limits.metadata = credits.credits.reservation();
        std::shared_ptr<PublishedGet> get;
        ASSERT_TRUE(PublishedGet::create(captured, {}, key, limits, &get).ok());
        complete_inline(get->sender());
        ASSERT_TRUE(get->result().found);
        EXPECT_EQ(get->result().cell.value.to_string(), "value");
        std::shared_ptr<PublishedScan> scan;
        ASSERT_TRUE(PublishedScan::create(captured, {}, {}, false, limits, &scan).ok());
        complete_inline(scan->next_sender());
        ASSERT_EQ(scan->result().entries.size(), 1U);
        EXPECT_EQ(scan->result().entries.get_allocator().owner(), credits.credits.reservation());
        retained = scan->result().entries.front();
    });
    worker.join();
    captured.reset();
    table.reset();
    credits.seal();
    if (!retained) {
        FAIL() << "scan did not return an owned result";
    }
    EXPECT_EQ(retained->key, key);
    EXPECT_EQ(retained->cell.value.to_string(), "value");
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    retained.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PublishedReadBudget, ColdReadCancellationKeepsChargedCallbackUntilLateCompletion)
{
    LoadCredits credits;
    auto        logical = std::make_shared<PageState>();
    logical->bases.push_back(cold(1));
    auto captured     = view(logical);
    auto loader       = std::make_shared<TestLoader>();
    loader->deferred  = true;
    loader->frames[1] = base({
        {.key = "a", .value = "value"}
    });
    std::shared_ptr<PublishedGet> get;
    ASSERT_TRUE(PublishedGet::create(captured, loader, "a", {.metadata = credits.credits.reservation()}, &get).ok());
    Completion terminal;
    {
        auto connected = stdexec::connect(get->sender(), Receiver{.done = &terminal});
        stdexec::start(connected);
        ASSERT_EQ(loader->pending.size(), 1U);
        get->request_stop();
        EXPECT_EQ(terminal.stopped.load(), 1U);
    }
    get.reset();
    credits.seal();
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    loader->finish();
    EXPECT_EQ(terminal.stopped.load(), 1U);
    EXPECT_EQ(terminal.values.load(), 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PublishedReadBudget, ExhaustedAdmissionReturnsStatusWithoutUnbudgetedHandleFallback)
{
    LoadCredits credits;
    auto        captured = view(page({
        {.key = "a", .value = "value"}
    }));
    credits.seal();
    PublishedReadLimits limits;
    limits.metadata = credits.credits.reservation();
    std::shared_ptr<PublishedGet> get;
    EXPECT_EQ(PublishedGet::create(captured, {}, "a", limits, &get).code(), Code::kResourceExhausted);
    EXPECT_FALSE(get);
    std::shared_ptr<PublishedScan> scan;
    EXPECT_EQ(PublishedScan::create(captured, {}, {}, false, limits, &scan).code(), Code::kResourceExhausted);
    EXPECT_FALSE(scan);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PublishedReadBudget, CapturedDescriptorOverflowRejectsAdmission)
{
    PublishedReadLimits limits;
    limits.page_metadata_bytes = SIZE_MAX;
    EXPECT_FALSE(PublishedGet::allocation_bound(limits, 1));
    EXPECT_FALSE(PublishedScan::allocation_bound(limits, 1));
}

TEST(PublishedReadBudget, ColdLeafBeyondSourceWindowUsesCapturedDescriptorBound)
{
    auto logical = std::make_shared<PageState>();
    logical->bases.push_back(cold(1));
    auto run = page({
                        {.key = "z", .value = "overlay"}
    })
                   ->overlays.front();
    logical->overlays.assign(8192, run);
    auto captured = view(logical);
    for (bool scan_read : {false, true}) {
        LoadCredits credits;
        credits.credits.reservation()->seal();
        PublishedReadLimits limits;
        limits.max_key_bytes       = 1;
        limits.max_memtables       = 1;
        limits.max_route_depth     = 1;
        limits.max_entries         = 2;
        limits.max_result_bytes    = 32;
        limits.page_metadata_bytes = captured->mapping.copy_bounds().max_leaf_bytes;
        const auto bound =
            scan_read ? PublishedScan::allocation_bound(limits, 1) : PublishedGet::allocation_bound(limits, 1);
        if (!bound) {
            FAIL() << "read bound overflow";
        }
        limits.metadata   = credits.reserve(detail::BudgetClass::kRouting, *bound);
        auto loader       = std::make_shared<TestLoader>();
        loader->frames[1] = base({
            {.key = "a", .value = "cold"}
        });
        if (scan_read) {
            std::shared_ptr<PublishedScan> scan;
            ASSERT_TRUE(PublishedScan::create(captured, loader, {}, false, limits, &scan).ok());
            const auto entries = collect(scan);
            ASSERT_TRUE(scan->result().status.ok()) << scan->result().status.to_string();
            ASSERT_EQ(entries.size(), 2U);
            EXPECT_EQ(entries.front().key, "a");
            EXPECT_EQ(entries.front().cell.value.to_string(), "cold");
            EXPECT_EQ(entries.back().key, "z");
        }
        else {
            std::shared_ptr<PublishedGet> get;
            ASSERT_TRUE(PublishedGet::create(captured, loader, "a", limits, &get).ok());
            complete_inline(get->sender());
            ASSERT_TRUE(get->result().status.ok()) << get->result().status.to_string();
            ASSERT_TRUE(get->result().found);
            EXPECT_EQ(get->result().cell.value.to_string(), "cold");
        }
        EXPECT_EQ(loader->calls, 1U);
    }
}
} // namespace
} // namespace crowdb::tree
