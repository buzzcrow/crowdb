// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/maptable/compressor.h"
#include "load_budget_test_support.h"
#include "published_read_test_support.h"

#include <cstring>
#include <thread>

namespace crowdb::tree
{
using namespace test_read;

namespace
{
class TestAsyncStore final : public AsyncPageStore
{
  public:
    bool                        deferred = false;
    size_t                      reads    = 0;
    std::vector<uint8_t>        blob;
    std::deque<AsyncCompletion> pending;

    uint64_t submit_read(PageAddr /*addr*/, void *destination, size_t size, AsyncCompletion complete) override
    {
        ++reads;
        EXPECT_EQ(size, blob.size());
        EXPECT_EQ(reinterpret_cast<uintptr_t>(destination) % 4096, 0U);
        std::memcpy(destination, blob.data(), std::min(size, blob.size()));
        if (deferred) {
            pending.push_back(complete);
        }
        else {
            complete.complete(Status::Ok());
        }
        return reads;
    }

    uint64_t submit_write(PageAddr /*addr*/, const void * /*buf*/, size_t /*len*/, AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return 0;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return Status::not_supported();
    }

    void cancel(uint64_t /*op_id*/) override
    {
    }

    void finish(Status status = Status::Ok())
    {
        ASSERT_FALSE(pending.empty());
        auto callback = pending.front();
        pending.pop_front();
        callback.complete(std::move(status));
    }
};

BaseRef test_backing(const std::shared_ptr<TestAsyncStore> &store)
{
    const auto frame = base({
        {.key = "key", .slot = 5, .value = "disk"}
    });
    encode_durable_page(frame->frame(), frame->page_bytes(), compress_algo::kNone, &store->blob);
    auto backing    = std::make_shared<BackingRef>();
    backing->length = store->blob.size();
    backing->owner  = store;
    return {.identity = 1, .max_slot = 5, .backing = std::move(backing)};
}
} // namespace

TEST(PageLoad, InlineTransportDecodesAndCompletesOnCpuPool)
{
    auto                 runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto                 lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto                 store   = std::make_shared<TestAsyncStore>();
    auto                 backing = test_backing(store);
    AsyncStorePageLoader loader(store, runtime, lane);
    std::atomic<size_t>  calls{0};
    const auto           caller = std::this_thread::get_id();
    loader.load_leaf(backing, [&](Status status, std::shared_ptr<const LeafBase> leaf) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        EXPECT_NE(std::this_thread::get_id(), caller);
        ASSERT_NE(leaf, nullptr);
        EXPECT_EQ(CellView(leaf->cell(0)).value().to_string(), "disk");
        ++calls;
    });
    runtime->drain();
    EXPECT_EQ(calls.load(), 1U);
    EXPECT_EQ(store->reads, 1U);
}

TEST(PageLoad, DecodedLeafRetainsItsActualFrameChargeAfterLoaderAndReserveAreReleased)
{
    LoadCredits credits;
    auto        runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto        lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto        store   = std::make_shared<TestAsyncStore>();
    auto        backing = test_backing(store);
    auto        loader  = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{},
                                                                 OverflowBackingResolver{}, credits.credits);
    std::shared_ptr<const LeafBase> result;
    loader->load_leaf(backing, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(leaf);
    });
    runtime->drain();
    ASSERT_NE(result, nullptr);
    loader.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().reserved[LoadCredits::kBytes], 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    EXPECT_GE(credits.budget.snapshot().allocated[LoadCredits::kBytes], result->page_bytes() + sizeof(LeafBase));
    EXPECT_EQ(CellView(result->cell(0)).value().to_string(), "disk");
    result.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageLoad, IndependentIoCreditSurvivesLateFailureAndIsReusable)
{
    LoadCredits credits;
    auto        runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto        lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto        store   = std::make_shared<TestAsyncStore>();
    store->deferred     = true;
    auto   backing      = test_backing(store);
    auto   loader       = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{},
                                                                 OverflowBackingResolver{}, credits.credits);
    size_t failures     = 0;
    loader->load_leaf(backing, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kIoError);
        EXPECT_FALSE(leaf);
        ++failures;
    });
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
    auto second         = backing;
    auto second_backing = std::make_shared<BackingRef>(*backing.backing);
    ++second_backing->address;
    second.backing = std::move(second_backing);
    loader->load_leaf(second, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kResourceExhausted);
        EXPECT_FALSE(leaf);
        ++failures;
    });
    EXPECT_EQ(store->reads, 1U);
    store->finish(Status::io_error("controlled transport failure"));
    EXPECT_EQ(failures, 2U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    size_t successes = 0;
    loader->load_leaf(backing, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_NE(leaf, nullptr);
        ++successes;
    });
    store->finish();
    runtime->drain();
    EXPECT_EQ(successes, 1U);
    EXPECT_EQ(store->reads, 2U);
    loader.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageLoad, SealedDecodeCreditRetainsInflightBlobUntilTransportTerminal)
{
    LoadCredits credits;
    auto        runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto        lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto        store   = std::make_shared<TestAsyncStore>();
    store->deferred     = true;
    auto   backing      = test_backing(store);
    auto   loader       = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{},
                                                                 OverflowBackingResolver{}, credits.credits);
    size_t calls        = 0;
    loader->load_leaf(backing, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kResourceExhausted);
        EXPECT_FALSE(leaf);
        ++calls;
    });
    loader.reset();
    credits.seal();
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], backing.backing->length + 4095);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
    store->finish();
    runtime->drain();
    EXPECT_EQ(calls, 1U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageLoad, RejectsMixedAccountsAndByteIoUnitsBeforeAllocatingLoaderState)
{
    LoadCredits first;
    LoadCredits second;
    auto        runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto        lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto        store   = std::make_shared<TestAsyncStore>();
    for (const auto &invalid : {
             PageLoadBudget{.bytes = first.credits.reservation(), .io = second.credits.io},
             PageLoadBudget{.bytes = first.credits.io,            .io = first.credits.io }
    }) {
        EXPECT_THROW((void)AsyncStorePageLoader(store, runtime, lane, {}, {}, invalid), std::invalid_argument);
    }
    EXPECT_EQ(first.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    EXPECT_EQ(first.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    EXPECT_EQ(second.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}

TEST(PageLoad, SlowReadLeavesSingleCpuWorkerAvailable)
{
    auto runtime                = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane                   = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto store                  = std::make_shared<TestAsyncStore>();
    store->deferred             = true;
    auto                backing = test_backing(store);
    auto                loader  = std::make_shared<AsyncStorePageLoader>(store, runtime, lane);
    std::atomic<size_t> completed{0};
    loader->load_leaf(backing, [&](Status status, std::shared_ptr<const LeafBase> leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_NE(leaf, nullptr);
        ++completed;
    });
    std::atomic<bool> unrelated{false};
    ASSERT_TRUE(runtime
                    ->try_submit(
                        lane, [](void *context) noexcept { static_cast<std::atomic<bool> *>(context)->store(true); },
                        &unrelated)
                    .ok());
    runtime->drain();
    EXPECT_TRUE(unrelated.load());
    EXPECT_EQ(completed.load(), 0U);
    loader.reset();
    store->finish();
    runtime->drain();
    EXPECT_EQ(completed.load(), 1U);
}

TEST(PageLoad, InflightBudgetIsReleasedOnLateIoFailure)
{
    auto runtime                 = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane                    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto store                   = std::make_shared<TestAsyncStore>();
    store->deferred              = true;
    auto                 backing = test_backing(store);
    AsyncStorePageLoader loader(store, runtime, lane, {.max_inflight = 1});
    std::vector<Code>    codes;
    auto complete = [&](Status status, std::shared_ptr<const LeafBase>) { codes.push_back(status.code()); };
    loader.load_leaf(backing, complete);
    loader.load_leaf(backing, complete);
    EXPECT_EQ(codes, (std::vector<Code>{Code::kResourceExhausted}));
    store->finish(Status::io_error("late failure"));
    loader.load_leaf(backing, complete);
    store->finish(Status::io_error("second failure"));
    EXPECT_EQ(codes, (std::vector<Code>{Code::kResourceExhausted, Code::kIoError, Code::kIoError}));
    EXPECT_EQ(store->reads, 2U);
}

TEST(PageLoad, CorruptionAndProvenSlotBoundsRejectLoadedFrames)
{
    auto runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    for (bool corrupt_bytes : {false, true}) {
        auto store   = std::make_shared<TestAsyncStore>();
        auto backing = test_backing(store);
        if (corrupt_bytes) {
            store->blob.back() ^= 1;
        }
        else {
            backing.max_slot = 4;
        }
        AsyncStorePageLoader loader(store, runtime, lane);
        std::atomic<size_t>  errors{0};
        loader.load_leaf(backing, [&](Status status, std::shared_ptr<const LeafBase> leaf) {
            EXPECT_EQ(status.code(), Code::kCorruption);
            EXPECT_EQ(leaf, nullptr);
            ++errors;
        });
        runtime->drain();
        EXPECT_EQ(errors.load(), 1U);
    }
}

TEST(PageLoad, RejectedCpuQueueFailsWithoutDecodingOnIoThread)
{
    auto runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto store   = std::make_shared<TestAsyncStore>();
    auto backing = test_backing(store);
    runtime->close();
    AsyncStorePageLoader loader(store, runtime, lane);
    size_t               calls = 0;
    loader.load_leaf(backing, [&](Status status, std::shared_ptr<const LeafBase> leaf) {
        EXPECT_EQ(status.code(), Code::kUnavailable);
        EXPECT_EQ(leaf, nullptr);
        ++calls;
    });
    EXPECT_EQ(calls, 1U);
}

TEST(PageLoad, BoundsAndMissingRuntimeRejectBeforeIo)
{
    auto                 runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto                 lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto                 store   = std::make_shared<TestAsyncStore>();
    auto                 backing = test_backing(store);
    AsyncStorePageLoader bounded(store, runtime, lane, {.max_blob_bytes = 1});
    bounded.load_leaf(backing, [](Status status, std::shared_ptr<const LeafBase>) {
        EXPECT_EQ(status.code(), Code::kResourceExhausted);
    });
    AsyncStorePageLoader missing(store, {}, lane);
    missing.load_leaf(backing, [](Status status, std::shared_ptr<const LeafBase>) {
        EXPECT_EQ(status.code(), Code::kInvalidArgument);
    });
    missing.load_overflow(
        1, 4, [](Status status, std::shared_ptr<const buffer>) { EXPECT_EQ(status.code(), Code::kNotSupported); });
    EXPECT_EQ(store->reads, 0U);
}
} // namespace crowdb::tree
