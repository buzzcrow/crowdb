// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_cache.h"
#include "inner_load_test_support.h"
#include "load_budget_test_support.h"

#include <thread>

namespace crowdb::tree
{
namespace
{
using namespace test_read;

TEST(SharedPageCache, ConcurrentRangeViewsAndBindingsJoinOneLoadAndValidateEachSlotProof)
{
    SharedPageCache cache({.max_pages = 2, .max_waiters = 32});
    auto            first  = std::make_shared<TestLoader>();
    auto            second = std::make_shared<TestLoader>();
    first->deferred = second->deferred = true;
    first->frames[1] = second->frames[1] = base({
        {.key = "a", .slot = 7, .value = "value"}
    });
    auto left                            = cache.bind(first);
    auto right                           = cache.bind(second);
    auto input                           = cold(1, 2);
    input.generation                     = 3;
    input.range.upper                    = "m";
    size_t invalid                       = 0;
    left->load_leaf(input, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kCorruption);
        EXPECT_FALSE(leaf);
        ++invalid;
    });
    input.max_slot = 10;
    input.range    = {.lower = "m"};
    std::atomic<size_t>       done{0};
    std::vector<std::jthread> readers;
    readers.reserve(16);
    for (size_t i = 0; i < 16; ++i) {
        readers.emplace_back([&, input] {
            right->load_leaf(input, [&](Status status, auto leaf) {
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(leaf, first->frames[1]);
                done.fetch_add(1);
            });
        });
    }
    readers.clear();
    EXPECT_EQ(first->calls, 1U);
    EXPECT_EQ(second->calls, 0U);
    EXPECT_EQ(cache.snapshot().joined, 16U);
    first->finish();
    EXPECT_EQ(invalid, 1U);
    EXPECT_EQ(done.load(), 16U);
    EXPECT_EQ(cache.snapshot().waiters, 0U);
    EXPECT_EQ(cache.snapshot().resident_pages, 1U);
    right->load_leaf(input, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(leaf, first->frames[1]);
        done.fetch_add(1);
    });
    EXPECT_EQ(done.load(), 17U);
    EXPECT_EQ(cache.snapshot().hits, 1U);
}

TEST(SharedPageCache, CancelledReaderDoesNotCancelOtherWaiterOrOverwriteNewOverlay)
{
    SharedPageCache cache;
    auto            upstream = std::make_shared<TestLoader>();
    upstream->deferred       = true;
    upstream->frames[1]      = base({
        {.key = "a", .value = "base"}
    });
    auto loader              = cache.bind(upstream);
    auto logical             = std::make_shared<PageState>();
    logical->bases.push_back(cold(1));
    auto old_view                          = view(logical);
    auto changed                           = std::make_shared<PageState>(*page({
        {.key = "b", .slot = 2, .value = "overlay"}
    }));
    changed->bases                         = logical->bases;
    auto                          new_view = view(changed);
    std::shared_ptr<PublishedGet> first;
    std::shared_ptr<PublishedGet> second;
    ASSERT_TRUE(PublishedGet::create(old_view, loader, "a", {}, &first).ok());
    ASSERT_TRUE(PublishedGet::create(new_view, loader, "a", {}, &second).ok());
    Completion a;
    Completion b;
    auto       op_a = stdexec::connect(first->sender(), Receiver{.done = &a});
    auto       op_b = stdexec::connect(second->sender(), Receiver{.done = &b});
    stdexec::start(op_a);
    stdexec::start(op_b);
    EXPECT_EQ(upstream->calls, 1U);
    first->request_stop();
    EXPECT_EQ(a.stopped.load(), 1U);
    upstream->finish();
    EXPECT_EQ(a.stopped.load(), 1U);
    EXPECT_EQ(a.values.load(), 0U);
    EXPECT_EQ(b.values.load(), 1U);
    EXPECT_EQ(second->result().cell.value.to_string(), "base");
    EXPECT_EQ(new_view->mapping.get(0)->leaf, changed);
    EXPECT_EQ(changed->lookup("b").cell.value.to_string(), "overlay");
    EXPECT_FALSE(changed->bases[0].resident);
}

class TestReplayLoader final : public PageLoader
{
  public:
    std::vector<LeafComplete> callbacks;

    void load_leaf(BaseRef /*base*/, LeafComplete done) override
    {
        callbacks.push_back(std::move(done));
    }
};

TEST(SharedPageCache, FailureRetriesAndLateDuplicateCannotCompleteRecycledEntry)
{
    SharedPageCache cache({.max_pages = 1, .max_waiters = 4});
    auto            upstream  = std::make_shared<TestReplayLoader>();
    auto            loader    = cache.bind(upstream);
    auto            input     = cold(1);
    size_t          failed    = 0;
    size_t          completed = 0;
    loader->load_leaf(input, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kIoError);
        EXPECT_FALSE(leaf);
        ++failed;
    });
    ASSERT_EQ(upstream->callbacks.size(), 1U);
    auto obsolete = upstream->callbacks[0];
    obsolete(Status::io_error(), {});
    EXPECT_EQ(failed, 1U);
    EXPECT_EQ(cache.snapshot().resident_pages, 0U);
    loader->load_leaf(input, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        ASSERT_TRUE(leaf);
        EXPECT_EQ(CellView(leaf->cell(0)).value().to_string(), "new");
        ++completed;
    });
    ASSERT_EQ(upstream->callbacks.size(), 2U);
    obsolete(Status::Ok(), base({
                               {.key = "a", .value = "obsolete"}
    }));
    EXPECT_EQ(completed, 0U);
    upstream->callbacks[1](Status::Ok(), base({
                                             {.key = "a", .value = "new"}
    }));
    EXPECT_EQ(completed, 1U);
    EXPECT_EQ(cache.snapshot().inflight, 0U);
    EXPECT_EQ(cache.snapshot().waiters, 0U);
}

TEST(SharedPageCache, IdentityIncludesGenerationAndBackendProtection)
{
    SharedPageCache cache({.max_pages = 4, .max_waiters = 8});
    auto            upstream = std::make_shared<TestLoader>();
    upstream->frames[1]      = base({
        {.key = "a", .value = "one"}
    });
    auto loader              = cache.bind(upstream);
    auto input               = cold(1);
    auto read                = [&](const BaseRef &base) {
        loader->load_leaf(base, [](Status status, auto leaf) {
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(leaf);
        });
    };
    read(input);
    read(input);
    EXPECT_EQ(upstream->calls, 1U);
    ++input.generation;
    read(input);
    EXPECT_EQ(upstream->calls, 2U);
    auto backing   = std::make_shared<BackingRef>(*input.backing);
    backing->owner = std::make_shared<int>(2);
    input.backing  = backing;
    read(input);
    EXPECT_EQ(upstream->calls, 3U);
    auto newer = std::make_shared<BackingRef>(*backing);
    ++newer->generation;
    input.backing = newer;
    read(input);
    EXPECT_EQ(upstream->calls, 4U);
}

TEST(SharedPageCache, FixedCapacityRejectsBeforeIoAndCallbacksCanReenter)
{
    SharedPageCache cache({.max_pages = 1, .max_waiters = 2});
    auto            upstream = std::make_shared<TestLoader>();
    upstream->deferred       = true;
    upstream->frames[1] = upstream->frames[2] = base({
        {.key = "a", .value = "value"}
    });
    auto   loader                             = cache.bind(upstream);
    auto   input                              = cold(1);
    size_t done                               = 0;
    loader->load_leaf(input, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
        ++done;
        loader->load_leaf(input, [&](Status nested, auto frame) {
            EXPECT_TRUE(nested.ok());
            EXPECT_TRUE(frame);
            ++done;
        });
    });
    EXPECT_EQ(upstream->calls, 1U);
    loader->load_leaf(input, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
        ++done;
    });
    loader->load_leaf(input, [](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kUnavailable);
        EXPECT_FALSE(leaf);
    });
    upstream->finish();
    EXPECT_EQ(done, 3U);
    EXPECT_EQ(cache.snapshot().waiters, 0U);
    EXPECT_EQ(cache.snapshot().rejected, 1U);
}

TEST(SharedPageCache, ReservedWaiterSurvivesOrdinaryPressureAndWaitsForInFlightPageCapacity)
{
    LoadCredits     credits;
    SharedPageCache cache({.max_pages = 1, .max_waiters = 2}, credits.credits.bytes);
    auto            upstream = std::make_shared<TestLoader>();
    upstream->deferred       = true;
    upstream->frames[1] = upstream->frames[2] = base({
        {.key = "a", .value = "value"}
    });
    auto                        ordinary      = cache.bind(upstream, credits.credits.bytes);
    std::shared_ptr<PageLoader> reserved;
    ASSERT_TRUE(cache.bind_reserved(upstream, credits.credits.bytes, &reserved).ok());
    EXPECT_EQ(cache.snapshot().reserved_waiters, 1U);
    credits.seal();
    size_t ordinary_done = 0;
    size_t reserved_done = 0;
    ordinary->load_leaf(cold(1), [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
        ++ordinary_done;
    });
    ordinary->load_leaf(cold(1), [](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kUnavailable);
        EXPECT_FALSE(leaf);
    });
    reserved->load_leaf(cold(2), [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
        ++reserved_done;
    });
    EXPECT_EQ(cache.snapshot().waiters, 2U);
    EXPECT_EQ(upstream->calls, 1U);
    upstream->finish();
    EXPECT_EQ(ordinary_done, 1U);
    EXPECT_EQ(reserved_done, 0U);
    EXPECT_EQ(upstream->calls, 2U);
    upstream->finish();
    EXPECT_EQ(reserved_done, 1U);
    EXPECT_EQ(cache.snapshot().waiters, 0U);
    reserved.reset();
    EXPECT_EQ(cache.snapshot().reserved_waiters, 0U);
}

TEST(SharedPageCache, EvictionReturnsCacheReferenceButPinnedFrameRemainsCharged)
{
    LoadCredits credits;
    auto        runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    auto        lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto        store   = std::make_shared<TestInnerStore>();
    auto        frame   = base({
        {.key = "a", .value = "value"}
    });
    auto       &blob    = store->blobs[0];
    encode_durable_page(frame->frame(), frame->page_bytes(), compress_algo::kNone, &blob);
    auto backing    = std::make_shared<BackingRef>();
    backing->length = blob.size();
    backing->owner  = store;
    auto upstream   = std::make_shared<AsyncStorePageLoader>(store, runtime, lane, PageLoadLimits{},
                                                             OverflowBackingResolver{}, credits.credits);
    {
        SharedPageCache                 cache({.max_pages = 2, .max_waiters = 8}, credits.credits.bytes);
        auto                            loader         = cache.bind(upstream, credits.credits.bytes);
        const auto                      infrastructure = credits.budget.snapshot().allocated[LoadCredits::kBytes];
        std::shared_ptr<const LeafBase> pinned;
        loader->load_leaf({.identity = 1, .backing = backing}, [&](Status status, auto leaf) {
            EXPECT_TRUE(status.ok());
            pinned = std::move(leaf);
        });
        runtime->drain();
        ASSERT_TRUE(pinned);
        credits.seal();
        const auto allocated = credits.budget.snapshot().allocated[LoadCredits::kBytes];
        EXPECT_GT(allocated, infrastructure);
        EXPECT_EQ(cache.snapshot().resident_pages, 1U);
        loader->load_leaf({.identity = 1, .backing = backing}, [&](Status status, auto leaf) {
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(leaf, pinned);
        });
        EXPECT_EQ(store->reads.load(), 1U);
        cache.evict();
        EXPECT_EQ(cache.snapshot().resident_pages, 0U);
        EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], allocated);
        EXPECT_EQ(CellView(pinned->cell(0)).value().to_string(), "value");
        pinned.reset();
        EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], infrastructure);
    }
    upstream.reset();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(SharedPageCache, FailedBindingAllocationReturnsReservedSlot)
{
    LoadCredits     credits;
    SharedPageCache cache({.max_pages = 1, .max_waiters = 1});
    auto            upstream = std::make_shared<TestLoader>();
    upstream->frames[1]      = base({
        {.key = "a", .value = "value"}
    });
    credits.seal();
    std::shared_ptr<PageLoader> binding;
    EXPECT_EQ(cache.bind_reserved(upstream, credits.credits.bytes, &binding).code(), Code::kResourceExhausted);
    EXPECT_FALSE(binding);
    EXPECT_EQ(cache.snapshot().reserved_waiters, 0U);
    ASSERT_TRUE(cache.bind_reserved(upstream, {}, &binding).ok());
    size_t done = 0;
    binding->load_leaf(cold(1), [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
        ++done;
    });
    EXPECT_EQ(done, 1U);
    binding.reset();
    EXPECT_EQ(cache.snapshot().reserved_waiters, 0U);
}

TEST(SharedPageCache, ResidentByteLimitEvictsWithoutInvalidatingPinnedFrame)
{
    auto upstream       = std::make_shared<TestLoader>();
    auto frame          = base({
        {.key = "a", .value = "value"}
    });
    upstream->frames[1] = upstream->frames[2] = frame;
    SharedPageCache cache({.max_pages = 4, .max_waiters = 2, .max_resident_bytes = frame->page_bytes()});
    auto            loader = cache.bind(upstream);
    const auto      first  = cold(1);
    const auto      second = cold(2);
    std::shared_ptr<const LeafBase> pinned;
    loader->load_leaf(first, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        pinned = std::move(leaf);
    });
    loader->load_leaf(second, [](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
    });
    EXPECT_EQ(cache.snapshot().resident_pages, 1U);
    EXPECT_EQ(cache.snapshot().resident_bytes, frame->page_bytes());
    EXPECT_EQ(cache.snapshot().evictions, 1U);
    ASSERT_TRUE(pinned);
    EXPECT_EQ(CellView(pinned->cell(0)).value().to_string(), "value");
    loader->load_leaf(second, [](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
    });
    EXPECT_EQ(upstream->calls, 2U);
    loader->load_leaf(first, [](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
    });
    EXPECT_EQ(upstream->calls, 3U);
}

TEST(SharedPageCache, MissingSuccessfulPayloadFailsWaitersAndAllowsRetry)
{
    SharedPageCache cache({.max_pages = 1, .max_waiters = 2});
    auto            upstream = std::make_shared<TestReplayLoader>();
    auto            loader   = cache.bind(upstream);
    const auto      input    = cold(1);
    size_t          failed   = 0;
    for (size_t i = 0; i < 2; ++i) {
        loader->load_leaf(input, [&](Status status, auto leaf) {
            EXPECT_EQ(status.code(), Code::kCorruption);
            EXPECT_FALSE(leaf);
            ++failed;
        });
    }
    ASSERT_EQ(upstream->callbacks.size(), 1U);
    upstream->callbacks[0](Status::Ok(), {});
    EXPECT_EQ(failed, 2U);
    EXPECT_EQ(cache.snapshot().resident_pages, 0U);
    EXPECT_EQ(cache.snapshot().waiters, 0U);
    size_t done = 0;
    loader->load_leaf(input, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
        ++done;
    });
    ASSERT_EQ(upstream->callbacks.size(), 2U);
    upstream->callbacks[1](Status::Ok(), base({
                                             {.key = "a", .value = "value"}
    }));
    EXPECT_EQ(done, 1U);
}

TEST(SharedPageCache, CloseAndEvictionBeforeCompletionDoNotRetainLateResult)
{
    for (const bool close : {false, true}) {
        SharedPageCache cache;
        auto            upstream = std::make_shared<TestLoader>();
        upstream->deferred       = true;
        upstream->frames[1]      = base({
            {.key = "a", .value = "value"}
        });
        auto   loader            = cache.bind(upstream);
        size_t done              = 0;
        loader->load_leaf(cold(1), [&](Status status, auto leaf) {
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(leaf);
            ++done;
        });
        if (close) {
            cache.close();
        }
        else {
            cache.evict();
        }
        upstream->finish();
        EXPECT_EQ(done, 1U);
        EXPECT_EQ(cache.snapshot().resident_pages, 0U);
        EXPECT_EQ(cache.snapshot().inflight, 0U);
        if (close) {
            loader->load_leaf(cold(1), [](Status status, auto leaf) {
                EXPECT_EQ(status.code(), Code::kUnavailable);
                EXPECT_FALSE(leaf);
            });
        }
    }
}

TEST(SharedPageCache, TwoColdIndexReadersShareBothLoadsWithOneWorker)
{
    TestColdTree tree;
    tree.store->deferred                   = true;
    auto                          runtime  = std::make_shared<detail::FlushRuntime>(1, 8);
    auto                          lane     = std::make_shared<detail::FlushRuntime::Lane>(8);
    auto                          upstream = std::make_shared<AsyncStorePageLoader>(tree.store, runtime, lane);
    SharedPageCache               cache;
    auto                          loader = cache.bind(upstream);
    std::shared_ptr<PublishedGet> a;
    std::shared_ptr<PublishedGet> b;
    ASSERT_TRUE(PublishedGet::create(tree.view, loader, "a", {}, &a).ok());
    ASSERT_TRUE(PublishedGet::create(tree.view, loader, "g", {}, &b).ok());
    Completion first;
    Completion second;
    auto       op_a = stdexec::connect(a->sender(), Receiver{.done = &first});
    auto       op_b = stdexec::connect(b->sender(), Receiver{.done = &second});
    stdexec::start(op_a);
    stdexec::start(op_b);
    EXPECT_EQ(tree.store->reads.load(), 1U);
    tree.store->finish();
    runtime->drain();
    EXPECT_EQ(tree.store->reads.load(), 2U);
    tree.store->finish();
    runtime->drain();
    EXPECT_EQ(first.values.load(), 1U);
    EXPECT_EQ(second.values.load(), 1U);
    EXPECT_EQ(a->result().cell.value.to_string(), "a");
    EXPECT_EQ(b->result().cell.value.to_string(), "g");
    EXPECT_EQ(cache.snapshot().misses, 2U);
    EXPECT_EQ(cache.snapshot().joined, 2U);
}

TEST(SharedPageCache, HighWaterPressureReclaimsToLowWaterAndKeepsReadPinsValid)
{
    auto            upstream = std::make_shared<TestLoader>();
    auto            frame    = base({
        {.key = "a", .value = "value"}
    });
    const auto      bytes    = frame->page_bytes();
    SharedPageCache cache(
        {.max_pages = 8, .max_waiters = 2, .max_resident_bytes = 4 * bytes, .low_resident_bytes = 2 * bytes});
    auto                            loader = cache.bind(upstream);
    std::shared_ptr<const LeafBase> pinned;
    for (uint64_t pid = 1; pid <= 5; ++pid) {
        upstream->frames[pid] = frame;
        loader->load_leaf(cold(pid), [&](Status status, auto leaf) {
            EXPECT_TRUE(status.ok());
            if (!pinned) {
                pinned = std::move(leaf);
            }
        });
    }
    EXPECT_EQ(cache.snapshot().resident_bytes, 2 * bytes);
    EXPECT_EQ(cache.snapshot().resident_pages, 2U);
    EXPECT_EQ(cache.snapshot().evictions, 3U);
    ASSERT_TRUE(pinned);
    EXPECT_EQ(CellView(pinned->cell(0)).value().to_string(), "value");
}

TEST(SharedPageCache, ForegroundReadsHaveBoundedPriorityOverQueuedMaintenance)
{
    SharedPageCache             cache({.max_pages = 1, .max_waiters = 8, .max_read_burst = 2});
    auto                        upstream = std::make_shared<TestReplayLoader>();
    auto                        read     = cache.bind(upstream);
    std::shared_ptr<PageLoader> maintenance;
    ASSERT_TRUE(cache.bind_reserved(upstream, {}, &maintenance, PageLoadPriority::Maintenance).ok());
    std::vector<uint64_t> completed;
    auto                  load = [&](const std::shared_ptr<PageLoader> &loader, uint64_t pid) {
        loader->load_leaf(cold(pid), [&, pid](Status status, auto leaf) {
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(leaf);
            completed.push_back(pid);
        });
    };
    load(read, 1);
    load(maintenance, 2);
    load(read, 3);
    load(read, 4);
    load(read, 5);
    auto frame = base({
        {.key = "a", .value = "value"}
    });
    for (size_t i = 0; i < 5; ++i) {
        ASSERT_EQ(upstream->callbacks.size(), i + 1);
        auto done = upstream->callbacks[i];
        done(Status::Ok(), frame);
    }
    EXPECT_EQ(completed, (std::vector<uint64_t>{1, 3, 4, 2, 5}));
    EXPECT_EQ(cache.snapshot().waiters, 0U);
}

TEST(SharedPageCache, MaintenanceFramesAreEvictedBeforeRecentlyReadFrames)
{
    SharedPageCache cache({.max_pages = 2, .max_waiters = 4});
    auto            upstream = std::make_shared<TestLoader>();
    auto            frame    = base({
        {.key = "a", .value = "value"}
    });
    for (uint64_t pid = 1; pid <= 3; ++pid) {
        upstream->frames[pid] = frame;
    }
    auto read        = cache.bind(upstream);
    auto maintenance = cache.bind(upstream, {}, PageLoadPriority::Maintenance);
    auto load        = [](const std::shared_ptr<PageLoader> &loader, const BaseRef &input) {
        loader->load_leaf(input, [](Status status, auto leaf) {
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(leaf);
        });
    };
    const auto hot        = cold(1);
    const auto background = cold(2);
    load(read, hot);
    load(maintenance, background);
    load(maintenance, cold(3));
    EXPECT_EQ(upstream->calls, 3U);
    load(read, hot);
    EXPECT_EQ(upstream->calls, 3U);
    load(read, background);
    EXPECT_EQ(upstream->calls, 4U);
}

TEST(SharedPageCache, InvalidWatermarksAndUnboundedReadPriorityRejectAtConstruction)
{
    EXPECT_THROW(SharedPageCache({.max_resident_bytes = 10, .low_resident_bytes = 11}), std::invalid_argument);
    EXPECT_THROW(SharedPageCache({.max_read_burst = 0}), std::invalid_argument);
}
} // namespace
} // namespace crowdb::tree
