// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "inner_load_test_support.h"
#include "load_budget_test_support.h"

#include <thread>

namespace crowdb::tree
{
using namespace test_read;

TEST(InnerLoad, SharedSeparatorKeepsOnlyItsChargedCapacityAfterIndexRelease)
{
    LoadCredits       credits;
    auto              runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto              lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto              store   = std::make_shared<TestInnerStore>();
    const std::string key(4096, 'm');
    auto              backing = inner_backing(store, 7, *inner({key, std::string(4096, 't')}, {1, 2, 3}));
    auto              loader  = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{},
                                                                       OverflowBackingResolver{}, credits.credits);
    std::shared_ptr<const InnerPageState> result;
    loader->load_inner(backing, [&](Status status, auto index) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(index);
    });
    runtime->drain();
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->separators.size(), 2U);
    OwnedKey retained = result->separators.front();
    loader.reset();
    credits.seal();
    const auto all = credits.budget.snapshot().allocated[LoadCredits::kBytes];
    EXPECT_GT(all, 8192U);
    result.reset();
    EXPECT_LT(credits.budget.snapshot().allocated[LoadCredits::kBytes], all);
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], key.size());
    EXPECT_EQ(Slice(retained).to_string(), key);
    retained = {};
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}

TEST(InnerLoad, InlineIoDecodesOnCpuAndReturnsIndependentImmutableRouting)
{
    auto                                  runtime = std::make_shared<detail::FlushRuntime>(1, 1);
    auto                                  lane    = std::make_shared<detail::FlushRuntime::Lane>(1);
    auto                                  store   = std::make_shared<TestInnerStore>();
    auto                                  backing = inner_backing(store, 0, *inner({"m", "t"}, {1, 2, 3}));
    AsyncStorePageLoader                  loader(store, runtime, lane);
    std::shared_ptr<const InnerPageState> result;
    std::atomic<size_t>                   calls{0};
    const auto                            caller = std::this_thread::get_id();
    loader.load_inner(backing, [&](Status status, std::shared_ptr<const InnerPageState> loaded) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        EXPECT_NE(std::this_thread::get_id(), caller);
        result = std::move(loaded);
        ++calls;
    });
    runtime->drain();
    store->blobs.clear();
    backing.reset();
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->separators, (InnerPageState::Keys{"m", "t"}));
    EXPECT_EQ(result->children, (InnerPageState::Children{1, 2, 3}));
    EXPECT_EQ(calls.load(), 1U);
}

TEST(InnerLoad, ValidatesChecksumChildIdsAndMetadataBounds)
{
    auto runtime = std::make_shared<detail::FlushRuntime>(1, 1);
    auto lane    = std::make_shared<detail::FlushRuntime::Lane>(1);
    for (size_t scenario = 0; scenario < 4; ++scenario) {
        auto store   = std::make_shared<TestInnerStore>();
        auto state   = inner({"m"}, {1, scenario == 1 ? kInvalidPageId : 2});
        auto backing = inner_backing(store, 0, *state);
        if (scenario == 0) {
            store->blobs[0].back() ^= 1;
        }
        PageLoadLimits limits;
        limits.max_inner_children  = scenario == 2 ? 1 : 2;
        limits.max_inner_key_bytes = scenario == 3 ? 0 : 1;
        AsyncStorePageLoader loader(store, runtime, lane, limits);
        std::atomic<size_t>  calls{0};
        loader.load_inner(backing, [&](Status status, std::shared_ptr<const InnerPageState> loaded) {
            EXPECT_EQ(status.code(), scenario < 2 ? Code::kCorruption : Code::kResourceExhausted);
            EXPECT_EQ(loaded, nullptr);
            ++calls;
        });
        runtime->drain();
        EXPECT_EQ(calls.load(), 1U);
    }
}

TEST(InnerLoad, SlowIoDoesNotHoldCpuWorkerAndLateErrorReturnsPermit)
{
    auto runtime                 = std::make_shared<detail::FlushRuntime>(1, 1);
    auto lane                    = std::make_shared<detail::FlushRuntime::Lane>(1);
    auto store                   = std::make_shared<TestInnerStore>();
    store->deferred              = true;
    auto                 backing = inner_backing(store, 0, *inner({"m"}, {1, 2}));
    AsyncStorePageLoader loader(store, runtime, lane, {.max_inflight = 1});
    std::vector<Code>    outcomes;
    auto complete = [&](Status status, std::shared_ptr<const InnerPageState>) { outcomes.push_back(status.code()); };
    loader.load_inner(backing, complete);
    std::atomic<bool> progressed{false};
    ASSERT_TRUE(runtime
                    ->try_submit(
                        lane, [](void *context) noexcept { static_cast<std::atomic<bool> *>(context)->store(true); },
                        &progressed)
                    .ok());
    runtime->drain();
    EXPECT_TRUE(progressed.load());
    EXPECT_TRUE(outcomes.empty());
    loader.load_inner(backing, complete);
    store->finish(Status::io_error("first read failed"));
    loader.load_inner(backing, complete);
    store->finish(Status::io_error("second read failed"));
    EXPECT_EQ(outcomes, (std::vector<Code>{Code::kResourceExhausted, Code::kIoError, Code::kIoError}));
    EXPECT_EQ(store->reads.load(), 2U);
}

TEST(InnerLoad, RejectsWrongFrameTypeAndCpuQueueShutdown)
{
    auto       runtime = std::make_shared<detail::FlushRuntime>(1, 1);
    auto       lane    = std::make_shared<detail::FlushRuntime::Lane>(1);
    auto       store   = std::make_shared<TestInnerStore>();
    auto       backing = std::make_shared<BackingRef>();
    const auto leaf    = base({
        {.key = "a", .value = "leaf"}
    });
    encode_durable_page(leaf->frame(), leaf->page_bytes(), compress_algo::kNone, &store->blobs[0]);
    backing->length = store->blobs[0].size();
    backing->owner  = std::make_shared<int>(1);
    AsyncStorePageLoader loader(store, runtime, lane);
    std::atomic<size_t>  calls{0};
    loader.load_inner(backing, [&](Status status, std::shared_ptr<const InnerPageState> loaded) {
        EXPECT_EQ(status.code(), Code::kCorruption);
        EXPECT_EQ(loaded, nullptr);
        ++calls;
    });
    runtime->drain();
    runtime->close();
    loader.load_inner(backing, [&](Status status, std::shared_ptr<const InnerPageState> loaded) {
        EXPECT_EQ(status.code(), Code::kUnavailable);
        EXPECT_EQ(loaded, nullptr);
        ++calls;
    });
    EXPECT_EQ(calls.load(), 2U);
}
} // namespace crowdb::tree
