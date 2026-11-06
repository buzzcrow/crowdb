// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_cache.h"
#include "btree/resource_budget.h"
#include "btree/tree_metrics.h"
#include "btree/tree_work_registry.h"
#include "load_budget_test_support.h"
#include "published_read_test_support.h"
#include "runtime/allocation_memory_resource.h"

#include <cstdio>
#include <sstream>

namespace crowdb::tree
{
namespace
{
using detail::BudgetClass;
using detail::ResourceBudget;

ResourceBudget::Limits limits(size_t amount)
{
    ResourceBudget::Limits result;
    result.fill(amount);
    return result;
}

struct TestResult
{
    size_t values = 0, errors = 0, stopped = 0;
};

struct TestReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    TestResult *result;

    void set_value() const noexcept
    {
        ++result->values;
    }

    void set_error(Status /*status*/) const noexcept
    {
        ++result->errors;
    }

    void set_stopped() const noexcept
    {
        ++result->stopped;
    }
};

TEST(TreeWaitMetrics, CumulativeClassesAndExactDurationsDoNotDrainStages)
{
    auto                 metrics   = std::make_shared<TreeWorkMetrics>();
    auto                 collector = metrics->claim_collector();
    TreeMetricsOperation work(metrics, TreeStage::BudgetWait, 100);
    TreeWaitSpan         wait;
    wait.begin(metrics, TreeWaitClass::Routing, 100);
    auto       totals = metrics->waits.totals(130);
    const auto index  = static_cast<size_t>(TreeWaitClass::Routing);
    EXPECT_EQ(totals[index].inflight, 1U);
    EXPECT_EQ(totals[index].oldest_age_ns, 30U);
    EXPECT_TRUE(totals[index].complete);
    wait.finish(TreeWaitOutcome::Granted, 150);
    wait.finish(TreeWaitOutcome::Error, 200);
    ASSERT_TRUE(work.finish(TreeOutcome::Success, 150));
    totals = metrics->waits.totals(200);
    EXPECT_EQ(totals[index].attempts, 1U);
    EXPECT_EQ(totals[index].count, 1U);
    EXPECT_EQ(totals[index].sum_ns, 50U);
    EXPECT_EQ(totals[index].max_ns, 50U);
    EXPECT_EQ(totals[index].outcomes[0], 1U);
    EXPECT_EQ(totals[index].inflight, 0U);
    EXPECT_EQ(collector->capture_window()[static_cast<size_t>(TreeStage::BudgetWait)].count, 1U);
    EXPECT_EQ(metrics->waits.totals(300)[index].count, 1U);
}

TEST(TreeWaitMetrics, AgeCapacityNeverRejectsWorkAndLateOwnerFinishesAfterMetricsCallerDrops)
{
    auto                                               metrics = std::make_shared<TreeWorkMetrics>();
    std::weak_ptr<TreeWorkMetrics>                     weak    = metrics;
    std::array<TreeWaitSpan, kTreeWaitAgeCapacity + 1> waits;
    for (auto &wait : waits) {
        wait.begin(metrics, TreeWaitClass::Io, 100);
    }
    const auto index  = static_cast<size_t>(TreeWaitClass::Io);
    const auto totals = metrics->waits.totals(150)[index];
    EXPECT_EQ(totals.inflight, waits.size());
    EXPECT_EQ(totals.tracked, kTreeWaitAgeCapacity);
    EXPECT_EQ(totals.untracked, 1U);
    EXPECT_FALSE(totals.complete);
    metrics.reset();
    for (auto &wait : waits) {
        wait.finish(TreeWaitOutcome::Cancelled, 200);
    }
    EXPECT_TRUE(weak.expired());
}

TEST(TreeWaitMetrics, BudgetGrantAndCancellationAreSeparatedForEveryPartition)
{
    auto           metrics = std::make_shared<TreeWorkMetrics>();
    ResourceBudget budget(limits(1), 1);
    auto           account = budget.make_account(limits(1));
    account->set_work_metrics(metrics);
    for (size_t i = 0; i < ResourceBudget::kClasses; ++i) {
        const auto kind = static_cast<BudgetClass>(i);
        auto       held = ResourceBudget::try_reserve(account, kind, 1);
        ASSERT_TRUE(held);
        ResourceBudget::Request request(account, kind, 1);
        TestResult              result;
        auto                    operation = stdexec::connect(request.sender(), TestReceiver{&result});
        stdexec::start(operation);
        EXPECT_EQ(metrics->waits.totals(tree_metrics_now_ns())[i].inflight, 1U);
        if (i % 2 == 0) {
            held.reset();
        }
        else {
            request.request_stop();
            request.request_stop();
        }
        EXPECT_EQ(result.values, i % 2 == 0 ? 1U : 0U);
        EXPECT_EQ(result.stopped, i % 2 == 0 ? 0U : 1U);
        const auto total = metrics->waits.totals(tree_metrics_now_ns())[i];
        EXPECT_EQ(total.count, 1U);
        EXPECT_EQ(total.inflight, 0U);
        EXPECT_EQ(total.outcomes[i % 2 == 0 ? 0 : 2], 1U);
    }
}

TEST(TreeWaitMetrics, ImpossibleAndFullWaitersRecordErrorWhileCloseCancelsOnce)
{
    auto           metrics = std::make_shared<TreeWorkMetrics>();
    ResourceBudget budget(limits(1), 1);
    auto           account = budget.make_account(limits(1));
    account->set_work_metrics(metrics);
    auto held = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 1);
    ASSERT_TRUE(held);
    ResourceBudget::Request first(account, BudgetClass::kHandoff, 1);
    ResourceBudget::Request full(account, BudgetClass::kHandoff, 1);
    ResourceBudget::Request impossible(account, BudgetClass::kHandoff, 2);
    TestResult              result;
    auto                    a = stdexec::connect(first.sender(), TestReceiver{&result});
    auto                    b = stdexec::connect(full.sender(), TestReceiver{&result});
    auto                    c = stdexec::connect(impossible.sender(), TestReceiver{&result});
    stdexec::start(a);
    stdexec::start(b);
    stdexec::start(c);
    EXPECT_EQ(result.errors, 2U);
    budget.close();
    first.request_stop();
    EXPECT_EQ(result.stopped, 1U);
    auto duplicate = stdexec::connect(first.sender(), TestReceiver{&result});
    stdexec::start(duplicate);
    EXPECT_EQ(result.errors, 3U);
    const auto total = metrics->waits.totals(tree_metrics_now_ns())[0];
    EXPECT_EQ(total.attempts, 3U);
    EXPECT_EQ(total.count, 3U);
    EXPECT_EQ(total.outcomes[1], 2U);
    EXPECT_EQ(total.outcomes[2], 1U);
    EXPECT_EQ(total.inflight, 0U);
    EXPECT_TRUE(total.complete);
}

TEST(TreeWaitMetrics, SharedCacheWaitersHaveIndependentProofOutcomesAndNoHitOrLoaderDoubleCount)
{
    auto                   metrics = std::make_shared<TreeWorkMetrics>();
    test_read::LoadCredits credits;
    credits.account->set_work_metrics(metrics);
    SharedPageCache cache({.max_pages = 1, .max_waiters = 8});
    auto            upstream = std::make_shared<test_read::TestLoader>();
    upstream->deferred       = true;
    upstream->frames[1]      = test_read::base({
        {.key = "key", .slot = 7, .value = "value"}
    });
    auto   loader            = cache.bind(upstream, credits.credits.reservation());
    auto   input             = test_read::cold(1, 10);
    size_t success           = 0;
    size_t failure           = 0;
    auto   ready             = [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
        ++success;
    };
    loader->load_leaf(input, ready);
    auto invalid     = input;
    invalid.max_slot = 2;
    loader->load_leaf(invalid, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kCorruption);
        EXPECT_FALSE(leaf);
        ++failure;
    });
    loader->load_leaf(input, ready);
    constexpr auto index = static_cast<size_t>(TreeWaitClass::SharedCache);
    EXPECT_EQ(metrics->waits.totals(tree_metrics_now_ns())[index].inflight, 2U);
    ASSERT_EQ(upstream->pending.size(), 1U);
    auto duplicate = upstream->pending.front();
    upstream->finish();
    duplicate(Status::io_error(), {});
    loader->load_leaf(input, ready);
    EXPECT_EQ(success, 3U);
    EXPECT_EQ(failure, 1U);
    EXPECT_EQ(upstream->calls, 1U);
    const auto total = metrics->waits.totals(tree_metrics_now_ns())[index];
    EXPECT_EQ(total.attempts, 2U);
    EXPECT_EQ(total.count, 2U);
    EXPECT_EQ(total.outcomes[0], 1U);
    EXPECT_EQ(total.outcomes[1], 1U);
    EXPECT_EQ(total.inflight, 0U);
}

TEST(TreeWaitMetrics, NativeRegistrySamplesOnceWithoutDrainingAndReleasesEveryPaidByte)
{
    test_read::LoadCredits credits;
    auto                   metrics   = std::make_shared<TreeWorkMetrics>();
    auto                   collector = metrics->claim_collector();
    TreeWaitSpan           wait;
    wait.begin(metrics, TreeWaitClass::SharedCache, 100);
    wait.finish(TreeWaitOutcome::Granted, 150);
    TreeMetricsOperation stage(metrics, TreeStage::PageLoadPipeline, 100);
    ASSERT_TRUE(stage.finish(TreeOutcome::Success, 150));
    const auto available = credits.credits.reservation()->available();
    {
        auto resource = std::make_shared<detail::AllocationMemoryResource>(credits.credits.reservation());
        auto registry = create_tree_work_registry(resource, "tree", metrics, credits.credits.reservation());
        EXPECT_LT(credits.credits.reservation()->available(), available);
        std::unique_ptr<FILE, decltype(&std::fclose)> file(std::tmpfile(), &std::fclose);
        ASSERT_NE(file, nullptr);
        registry->flush_to(file.get(), 1, "test");
        EXPECT_EQ(std::fseek(file.get(), 0, SEEK_SET), 0);
        std::array<char, 2048> bytes{};
        std::string            rendered;
        while (const auto count = std::fread(bytes.data(), 1, bytes.size(), file.get())) {
            rendered.append(bytes.data(), count);
        }
        std::istringstream lines(rendered);
        std::string        line;
        size_t             fields_seen = 0;
        bool               found_sum   = false;
        while (std::getline(lines, line)) {
            std::istringstream fields(line);
            std::string        name;
            uint64_t           value = 0;
            if (fields >> name >> value; name.starts_with("tree.work.wait.")) {
                ++fields_seen;
                if (name == "tree.work.wait.shared_cache.latency.sum.ns.c") {
                    found_sum = true;
                    EXPECT_EQ(value, 50U);
                }
            }
        }
        EXPECT_TRUE(found_sum);
        EXPECT_EQ(fields_seen, kTreeWaitClasses * 12);
        EXPECT_EQ(collector->capture_window()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count, 1U);
        EXPECT_EQ(metrics->waits.totals(300)[static_cast<size_t>(TreeWaitClass::SharedCache)].count, 1U);
    }
    EXPECT_EQ(credits.credits.reservation()->available(), available);
}
} // namespace
} // namespace crowdb::tree
