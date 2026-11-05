// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/overflow_backing.h"
#include "load_budget_test_support.h"
#include "overflow_load_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

TEST(OverflowLoad, PrimaryDependenciesRetainOnlyItsChainWithoutGlobalResolver)
{
    auto runtime  = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane     = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto store    = std::make_shared<TestOverflowStore>();
    auto captured = std::make_shared<TestBackingMap>();
    add_overflow(store, captured.get(), 1, 2, "hello");
    add_overflow(store, captured.get(), 2, kInvalidPageId, "world");
    add_overflow(store, captured.get(), 3, kInvalidPageId, "unrelated");
    std::weak_ptr<const BackingRef> unrelated    = captured->at(3);
    auto                            dependencies = std::make_shared<OverflowBackingSet>(nullptr);
    dependencies->pages.emplace(1, OverflowBackingPage{.backing = captured->at(1)});
    dependencies->pages.emplace(2, OverflowBackingPage{.backing = captured->at(2)});
    auto primary      = std::make_shared<BackingRef>();
    primary->overflow = dependencies;
    captured.reset();
    EXPECT_TRUE(unrelated.expired());
    AsyncStorePageLoader loader(store, runtime, lane);
    size_t               calls = 0;
    loader.load_overflow(primary, 1, 10, [&](Status status, std::shared_ptr<const buffer> value) {
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_TRUE(value);
        EXPECT_EQ(value->slice().to_string(), "helloworld");
        ++calls;
    });
    runtime->drain();
    EXPECT_EQ(calls, 1U);
}

TEST(OverflowLoad, PrimaryCannotResolveForeignPidThroughFallback)
{
    auto runtime  = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane     = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto store    = std::make_shared<TestOverflowStore>();
    auto captured = std::make_shared<TestBackingMap>();
    add_overflow(store, captured.get(), 1, kInvalidPageId, "hello");
    auto primary      = std::make_shared<BackingRef>();
    primary->overflow = std::make_shared<OverflowBackingSet>(nullptr);
    AsyncStorePageLoader loader(store, runtime, lane, {}, resolver(captured));
    size_t               calls = 0;
    loader.load_overflow(primary, 1, 5, [&](Status status, std::shared_ptr<const buffer> value) {
        EXPECT_EQ(status.code(), Code::kCorruption);
        EXPECT_FALSE(value);
        ++calls;
    });
    runtime->drain();
    EXPECT_EQ(calls, 1U);
    EXPECT_EQ(store->reads.load(), 0U);
}

TEST(OverflowLoad, ValueCapacityRemainsChargedAfterDecodeScratchAndLoaderAreGone)
{
    LoadCredits       credits;
    auto              runtime  = std::make_shared<detail::FlushRuntime>(1, 8);
    auto              lane     = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto              store    = std::make_shared<TestOverflowStore>();
    auto              captured = std::make_shared<TestBackingMap>();
    const std::string first(4096, 'a');
    const std::string second(4096, 'b');
    add_overflow(store, captured.get(), 1, 2, first);
    add_overflow(store, captured.get(), 2, kInvalidPageId, second);
    auto loader = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{}, resolver(captured),
                                                         credits.credits);
    std::shared_ptr<const buffer> result;
    loader->load_overflow(1, 8192, [&](Status status, auto value) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(value);
    });
    runtime->drain();
    ASSERT_NE(result, nullptr);
    loader.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], result->capacity());
    EXPECT_LT(credits.budget.snapshot().allocated[LoadCredits::kBytes], result->capacity() + 512);
    EXPECT_EQ(result->slice().to_string(), first + second);
    result.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(OverflowLoad, CapturedResolverAndBackingSurviveDeferredTransport)
{
    auto runtime    = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane       = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto store      = std::make_shared<TestOverflowStore>();
    store->deferred = true;
    auto captured   = std::make_shared<TestBackingMap>();
    add_overflow(store, captured.get(), 1, 2, "hello");
    add_overflow(store, captured.get(), 2, kInvalidPageId, "world");
    std::weak_ptr<const TestBackingMap> retained = captured;
    auto loader = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{}, resolver(captured));
    std::shared_ptr<const buffer> result;
    std::atomic<size_t>           calls{0};
    loader->load_overflow(1, 10, [&](Status status, std::shared_ptr<const buffer> value) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(value);
        ++calls;
    });
    runtime->drain();
    captured.reset();
    loader.reset();
    EXPECT_FALSE(retained.expired());
    store->finish();
    runtime->drain();
    EXPECT_EQ(calls.load(), 0U);
    store->finish();
    runtime->drain();
    EXPECT_EQ(calls.load(), 1U);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->slice().to_string(), "helloworld");
    EXPECT_TRUE(retained.expired());
}

TEST(OverflowLoad, RejectsShortLongAndCyclicChains)
{
    auto runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    for (uint64_t length : {9, 11, 12}) {
        auto store    = std::make_shared<TestOverflowStore>();
        auto captured = std::make_shared<TestBackingMap>();
        add_overflow(store, captured.get(), 1, 2, "hello");
        add_overflow(store, captured.get(), 2, length == 12 ? 1 : kInvalidPageId, "world");
        AsyncStorePageLoader loader(store, runtime, lane, {}, resolver(captured));
        std::atomic<size_t>  errors{0};
        loader.load_overflow(1, length, [&](Status status, std::shared_ptr<const buffer> value) {
            EXPECT_EQ(status.code(), Code::kCorruption);
            EXPECT_EQ(value, nullptr);
            ++errors;
        });
        runtime->drain();
        EXPECT_EQ(errors.load(), 1U);
        EXPECT_LE(store->reads.load(), 2U);
    }
}

TEST(OverflowLoad, BoundsValueBytesAndPagesAndValidatesChecksum)
{
    auto runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    for (size_t scenario = 0; scenario < 3; ++scenario) {
        auto store    = std::make_shared<TestOverflowStore>();
        auto captured = std::make_shared<TestBackingMap>();
        add_overflow(store, captured.get(), 1, 2, "hello");
        add_overflow(store, captured.get(), 2, kInvalidPageId, "world");
        PageLoadLimits limits;
        limits.max_value_bytes    = scenario == 0 ? 9 : 10;
        limits.max_overflow_pages = scenario == 1 ? 1 : 2;
        if (scenario == 2) {
            store->blobs[2].back() ^= 1;
        }
        AsyncStorePageLoader loader(store, runtime, lane, limits, resolver(captured));
        std::atomic<size_t>  errors{0};
        loader.load_overflow(1, 10, [&](Status status, std::shared_ptr<const buffer> value) {
            EXPECT_EQ(status.code(), scenario == 2 ? Code::kCorruption : Code::kResourceExhausted);
            EXPECT_EQ(value, nullptr);
            ++errors;
        });
        runtime->drain();
        EXPECT_EQ(errors.load(), 1U);
        EXPECT_EQ(store->reads.load(), scenario);
    }
}

TEST(OverflowLoad, GetAndScanMaterializeTheSameCapturedChain)
{
    auto runtime  = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane     = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto store    = std::make_shared<TestOverflowStore>();
    auto captured = std::make_shared<TestBackingMap>();
    add_overflow(store, captured.get(), 1, 2, "hello");
    add_overflow(store, captured.get(), 2, kInvalidPageId, "world");
    auto loader   = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{}, resolver(captured));
    auto snapshot = view(overflow_leaf());
    std::shared_ptr<PublishedGet> get;
    ASSERT_TRUE(PublishedGet::create(snapshot, loader, "key", {}, &get).ok());
    Completion got;
    auto       point = stdexec::connect(get->sender(), Receiver{.done = &got});
    stdexec::start(point);
    runtime->drain();
    EXPECT_EQ(got.values.load(), 1U);
    EXPECT_EQ(got.errors.load(), 0U);
    EXPECT_TRUE(get->result().found);
    EXPECT_FALSE(get->result().cell.overflow());
    EXPECT_EQ(get->result().cell.value.to_string(), "helloworld");
    for (bool reverse : {false, true}) {
        std::shared_ptr<PublishedScan> scan;
        ASSERT_TRUE(PublishedScan::create(snapshot, loader, {}, reverse, {.max_entries = 1}, &scan).ok());
        Completion scanned;
        auto       range = stdexec::connect(scan->next_sender(), Receiver{.done = &scanned});
        stdexec::start(range);
        runtime->drain();
        EXPECT_EQ(scanned.values.load(), 1U);
        ASSERT_EQ(scan->result().entries.size(), 1U);
        EXPECT_EQ(scan->result().entries[0].cell.value.to_string(), "helloworld");
        complete_inline(scan->next_sender());
        EXPECT_TRUE(scan->result().done);
    }
}

TEST(OverflowLoad, InlineChainUsesBoundedCpuContinuations)
{
    auto             runtime  = std::make_shared<detail::FlushRuntime>(1, 1);
    auto             lane     = std::make_shared<detail::FlushRuntime::Lane>(1);
    auto             store    = std::make_shared<TestOverflowStore>();
    auto             captured = std::make_shared<TestBackingMap>();
    constexpr size_t pages    = 128;
    for (size_t id = 1; id <= pages; ++id) {
        add_overflow(store, captured.get(), id, id == pages ? kInvalidPageId : id + 1, "x");
    }
    AsyncStorePageLoader loader(store, runtime, lane, {}, resolver(captured));
    std::atomic<size_t>  calls{0};
    loader.load_overflow(1, pages, [&](Status status, std::shared_ptr<const buffer> value) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        ASSERT_NE(value, nullptr);
        EXPECT_EQ(value->slice().to_string(), std::string(pages, 'x'));
        ++calls;
    });
    runtime->drain();
    EXPECT_EQ(calls.load(), 1U);
    EXPECT_EQ(store->reads.load(), pages);
    EXPECT_EQ(runtime->snapshot().completed, pages + 1);
}

TEST(OverflowLoad, LateReadErrorReleasesSharedInflightPermit)
{
    auto runtime    = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane       = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto store      = std::make_shared<TestOverflowStore>();
    store->deferred = true;
    auto captured   = std::make_shared<TestBackingMap>();
    add_overflow(store, captured.get(), 1, kInvalidPageId, "hello");
    AsyncStorePageLoader loader(store, runtime, lane, {.max_inflight = 1}, resolver(captured));
    std::vector<Code>    outcomes;
    auto complete = [&](Status status, std::shared_ptr<const buffer>) { outcomes.push_back(status.code()); };
    loader.load_overflow(1, 5, complete);
    runtime->drain();
    loader.load_overflow(1, 5, complete);
    store->finish(Status::io_error("deferred failure"));
    loader.load_overflow(1, 5, complete);
    runtime->drain();
    store->finish(Status::io_error("second failure"));
    EXPECT_EQ(outcomes, (std::vector<Code>{Code::kResourceExhausted, Code::kIoError, Code::kIoError}));
    EXPECT_EQ(store->reads.load(), 2U);
}
} // namespace crowdb::tree
