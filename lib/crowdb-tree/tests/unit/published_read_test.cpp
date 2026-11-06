// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "published_read_test_support.h"

#include <thread>

namespace crowdb::tree
{
using namespace test_read;

TEST(PublishedRead, PointMergesTwoMemTablesOverlayAndBaseWithCapturedFloor)
{
    auto first  = std::make_shared<MemTable>();
    auto second = std::make_shared<MemTable>();
    ASSERT_TRUE(first->upsert("key", 5, encode_cell_buf(5, OpKind::kPut, "covered")));
    ASSERT_TRUE(first->upsert("key", 11, encode_cell_buf(11, OpKind::kPut, "first"), 0));
    ASSERT_TRUE(second->upsert("key", 12, encode_cell_buf(12, OpKind::kPut, "second")));
    auto leaf = std::make_shared<PageState>(*page({
        {.key = "key", .slot = 10, .value = "overlay"}
    }));
    leaf->bases.push_back({
        .identity = 1, .resident = base({{.key = "key", .slot = 3, .value = "base"}}
             )
    });
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(view(leaf, {first, second}, 8), {}, "key", {}, &request).ok());
    complete_inline(request->sender());
    EXPECT_TRUE(request->result().found);
    EXPECT_EQ(request->result().cell.slot, 12U);
    EXPECT_EQ(request->result().cell.value.to_string(), "second");
}

TEST(PublishedRead, PointDistinguishesMissingAndTombstoneAndBorrowsValue)
{
    const auto leaf = page({
        {.key = "a", .slot = 4, .value = "value"},
        {.key = "b", .slot = 5, .value = {}, .tombstone = true}
    });
    for (const auto *key : {"a", "b", "c"}) {
        std::shared_ptr<PublishedGet> request;
        ASSERT_TRUE(PublishedGet::create(view(leaf), {}, key, {}, &request).ok());
        complete_inline(request->sender());
        EXPECT_EQ(request->result().found, std::string_view(key) == "a");
        if (std::string_view(key) == "a") {
            EXPECT_EQ(request->result().cell.value.data(), leaf->lookup("a").cell.value.data());
        }
        if (std::string_view(key) == "b") {
            EXPECT_TRUE(request->result().cell.tombstone());
            EXPECT_EQ(request->result().cell.slot, 5U);
        }
    }
}

TEST(PublishedRead, ColdGetCompletesInlineWithoutRecursiveRestart)
{
    auto leaf   = std::make_shared<PageState>();
    auto loader = std::make_shared<TestLoader>();
    for (uint64_t id = 1; id <= 20; ++id) {
        leaf->bases.push_back(cold(id));
        loader->frames[id] = base({
            {.key = "key", .slot = id, .value = std::to_string(id)}
        });
    }
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(view(leaf), loader, "key", {}, &request).ok());
    complete_inline(request->sender());
    EXPECT_EQ(loader->calls, 20U);
    EXPECT_EQ(request->result().cell.value.to_string(), "20");
    EXPECT_EQ(leaf->bases[0].resident, nullptr);
}

TEST(PublishedRead, ColdLateCompletionHoldsNoThreadBoundGuard)
{
    EpochManager epoch;
    auto         table = std::make_shared<MemTable>(1, &epoch);
    ASSERT_TRUE(table->upsert("key", 5, encode_cell_buf(5, OpKind::kPut, "l0")));
    auto leaf = std::make_shared<PageState>();
    leaf->bases.push_back(cold(1));
    auto loader       = std::make_shared<TestLoader>();
    loader->deferred  = true;
    loader->frames[1] = base({
        {.key = "key", .slot = 7, .value = "disk"}
    });
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(view(leaf, {table}), loader, "key", {}, &request).ok());
    Completion done;
    auto       operation = stdexec::connect(request->sender(), Receiver{&done});
    stdexec::start(operation);
    EXPECT_EQ(done.values.load(), 0U);
    EXPECT_EQ(epoch.active_guards(), 0U);
    std::thread worker([loader] { loader->finish(); });
    worker.join();
    EXPECT_EQ(done.values.load(), 1U);
    EXPECT_EQ(request->result().cell.value.to_string(), "disk");
}

TEST(PublishedRead, LateIoErrorAndStopAreTerminalExactlyOnce)
{
    for (bool stopped : {false, true}) {
        auto leaf = std::make_shared<PageState>();
        leaf->bases.push_back(cold(1));
        auto loader       = std::make_shared<TestLoader>();
        loader->deferred  = true;
        loader->frames[1] = base({
            {.key = "key", .slot = 1, .value = "disk"}
        });
        std::shared_ptr<PublishedGet> request;
        ASSERT_TRUE(PublishedGet::create(view(leaf), loader, "key", {}, &request).ok());
        Completion done;
        auto       operation = stdexec::connect(request->sender(), Receiver{&done});
        stdexec::start(operation);
        if (stopped) {
            request->request_stop();
            EXPECT_EQ(done.stopped.load(), 1U);
        }
        loader->finish(Status::io_error("read failed"));
        EXPECT_EQ(done.values.load(), 0U);
        EXPECT_EQ(done.errors.load(), stopped ? 0U : 1U);
        EXPECT_EQ(done.stopped.load(), stopped ? 1U : 0U);
    }
}

TEST(PublishedRead, ActiveMemTableAboveCoverageIsVisibleAndEqualSlotUsesSourceOrder)
{
    auto table = std::make_shared<MemTable>();
    ASSERT_TRUE(table->upsert("key", 20, encode_cell_buf(20, OpKind::kPut, "active")));
    auto                          leaf = page({
        {.key = "key", .slot = 20, .value = "overlay"}
    });
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(view(leaf, {table}, 10), {}, "key", {}, &request).ok());
    complete_inline(request->sender());
    EXPECT_EQ(request->result().cell.value.to_string(), "active");
}

TEST(PublishedRead, OldCapturedMappingSurvivesAConcurrentPublication)
{
    PublicationCoordinator coordinator;
    auto                   original = published(page({
        {.key = "key", .slot = 1, .value = "old"}
    }));
    auto                   install  = std::make_shared<PublicationRequest>();
    install->transaction.record     = std::make_shared<InstallRecord>(InstallToken{.batch = 1});
    install->transaction.pages      = {
        {.page_id = 0, .expected = {}, .replacement = original}
    };
    install->transaction.root_page_id = 0;
    install->on_done                  = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(coordinator.try_submit(install));
    const auto captured         = coordinator.capture();
    auto       replace          = std::make_shared<PublicationRequest>();
    replace->transaction.record = std::make_shared<InstallRecord>(InstallToken{.batch = 2});
    replace->transaction.pages  = {
        {.page_id     = 0,
         .expected    = original,
         .replacement = published(page({{.key = "key", .slot = 2, .value = "new"}}))}
    };
    replace->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(coordinator.try_submit(replace));
    coordinator.reclaim();
    for (const auto &[snapshot, expected] : std::array{
             std::pair{captured,              std::string("old")},
             std::pair{coordinator.capture(), std::string("new")}
    }) {
        std::shared_ptr<PublishedGet> request;
        ASSERT_TRUE(PublishedGet::create(snapshot, {}, "key", {}, &request).ok());
        complete_inline(request->sender());
        EXPECT_EQ(request->result().cell.value.to_string(), expected);
    }
}
} // namespace crowdb::tree
