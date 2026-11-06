// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_operation.h"
#include "published_read_test_support.h"

#include <future>

namespace crowdb::tree
{
namespace
{
struct TestColdMaintenance
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(64 * 1024 * 1024);
        return result;
    }

    Status publish(std::shared_ptr<const PublishedPage> expected, std::shared_ptr<const PublishedPage> page)
    {
        auto request         = std::make_shared<PublicationRequest>();
        request->transaction = {
            .record       = std::make_shared<InstallRecord>(InstallToken{.generation = 1}),
            .pages        = {{.page_id = 0, .expected = std::move(expected), .replacement = std::move(page)}},
            .root_page_id = 0};
        std::promise<Status> completed;
        auto                 terminal = completed.get_future();
        request->on_done = [&](PublicationResult result) { completed.set_value(std::move(result.status)); };
        return publisher.try_submit(request) ? terminal.get() : Status::unavailable();
    }

    void prepare()
    {
        auto leaf = std::make_shared<PageState>(*test_read::page({
            {.key = "a", .value = std::string(80, 'a')},
            {.key = "c", .value = std::string(80, 'c')}
        }));
        leaf->bases.push_back(test_read::cold(1));
        ASSERT_TRUE(publish({}, test_read::published(std::move(leaf))).ok());
        loader->deferred = true;
        loader->frames.emplace(1, test_read::base({
                                      {.key = "b", .value = std::string(80, 'b')}
        }));
        PageMaintenanceOperationOptions options;
        options.max_steps                             = 1;
        options.max_stale_replans                     = 0;
        options.publication.pages.primary_page_bytes  = 512;
        options.publication.pages.target_page_bytes   = 384;
        options.publication.pages.merge_trigger_bytes = 64;
        options.publication.pages.merge_target_bytes  = 128;
        options.publication.pages.max_work_entries    = 8;
        options.acquire                               = [this](auto *out) {
            auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, 16 * 1024 * 1024);
            if (!credit) {
                return Status::resource_exhausted();
            }
            *out = detail::AllocationReservation::create(std::move(*credit));
            return Status::Ok();
        };
        maintenance = make_page_maintenance(publisher, runtime, lane, std::move(options), loader, {});
    }

    void begin()
    {
        done.store(false);
        maintenance(publisher.capture(), [this](Status result, uint64_t) {
            status = std::move(result);
            done.store(true, std::memory_order_release);
        });
        runtime.drain();
    }

    detail::ResourceBudget                           budget{limits(), 2};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());
    PublicationCoordinator                           publisher;
    detail::FlushRuntime                             runtime{2, 8};
    std::shared_ptr<detail::FlushRuntime::Lane>      lane   = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<test_read::TestLoader>           loader = std::make_shared<test_read::TestLoader>();
    FlushBatch::Maintenance                          maintenance;
    std::atomic<bool>                                done{false};
    Status                                           status;
};

TEST(ColdPageMaintenance, StaleLoadedViewDoesNotReplaceTheConcurrentWriter)
{
    TestColdMaintenance test;
    test.prepare();
    test.begin();
    ASSERT_FALSE(test.done.load(std::memory_order_acquire));
    ASSERT_EQ(test.loader->pending.size(), 1U);
    const auto before  = test.publisher.capture()->mapping.get(0);
    auto       changed = std::make_shared<PublishedPage>(*before);
    changed->revision++;
    changed->leaf = test_read::page({
        {.key = "new", .slot = 2, .value = "concurrent"}
    });
    ASSERT_TRUE(test.publish(before, changed).ok());
    test.loader->finish();
    test.runtime.drain();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    ASSERT_TRUE(test.status.ok()) << test.status.to_string();
    EXPECT_EQ(test.publisher.capture()->mapping.get(0), changed);
    EXPECT_EQ(test.loader->calls, 1U);
}

TEST(ColdPageMaintenance, FailedReadRetainsTheOriginalCandidateAndNextPassRetries)
{
    TestColdMaintenance test;
    test.prepare();
    const auto before = test.publisher.capture();
    test.begin();
    ASSERT_FALSE(test.done.load(std::memory_order_acquire));
    ASSERT_EQ(test.loader->pending.size(), 1U);
    test.loader->finish(Status::io_error("injected cold read failure"));
    test.runtime.drain();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    EXPECT_EQ(test.status.code(), Code::kIoError);
    EXPECT_EQ(test.publisher.capture(), before);
    test.begin();
    ASSERT_FALSE(test.done.load(std::memory_order_acquire));
    ASSERT_EQ(test.loader->pending.size(), 1U);
    test.loader->finish();
    test.runtime.drain();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    ASSERT_TRUE(test.status.ok()) << test.status.to_string();
    EXPECT_GT(test.publisher.capture()->stats.leaf_pages, 1U);
    EXPECT_EQ(test.loader->calls, 2U);
}
} // namespace
} // namespace crowdb::tree
