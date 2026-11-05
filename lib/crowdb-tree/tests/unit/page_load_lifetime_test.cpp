// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_dispatcher.h"
#include "btree/tree_metrics.h"
#include "crowdb-tree/maptable/compressor.h"
#include "load_budget_test_support.h"
#include "published_read_test_support.h"
#include "runtime/tree_operation_lifetime.h"

#include <cstring>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
using namespace test_read;
enum class Kind : uint8_t { kLeaf, kInner, kOverflow };

class UncertainReadStore final : public AsyncPageStore
{
  public:
    struct Pending
    {
        uint64_t        address;
        void           *buffer;
        size_t          bytes;
        AsyncCompletion complete;
    };

    bool                                     throw_after_accept = true;
    bool                                     inline_completion  = false;
    bool                                     allocation_failure = false;
    std::map<uint64_t, std::vector<uint8_t>> blobs;
    std::deque<Pending>                      pending;
    size_t                                   reads = 0;

    uint64_t submit_read(PageAddr addr, void *buffer, size_t bytes, AsyncCompletion complete) override
    {
        ++reads;
        EXPECT_TRUE(complete.owner);
        pending.push_back({.address = addr, .buffer = buffer, .bytes = bytes, .complete = std::move(complete)});
        if (inline_completion) {
            finish();
        }
        if (throw_after_accept) {
            if (allocation_failure) {
                throw std::bad_alloc();
            }
            throw std::runtime_error("read accepted before failure");
        }
        return reads;
    }

    uint64_t submit_write(PageAddr /*addr*/, const void * /*buffer*/, size_t /*bytes*/,
                          AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return 0;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return Status::not_supported();
    }

    void cancel(uint64_t /*id*/) override
    {
    }

    void finish()
    {
        ASSERT_FALSE(pending.empty());
        auto read = std::move(pending.front());
        pending.pop_front();
        const auto &blob = blobs.at(read.address);
        ASSERT_EQ(read.bytes, blob.size());
        std::memcpy(read.buffer, blob.data(), blob.size());
        read.complete.complete(Status::Ok());
        read.complete.complete(Status::io_error()); // a duplicate must not report another terminal
    }
};

struct Reads
{
    LoadCredits                                           credits;
    std::shared_ptr<TreeWorkMetrics>                      metrics = std::make_shared<TreeWorkMetrics>();
    std::shared_ptr<detail::FlushRuntime>                 runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    std::shared_ptr<detail::FlushRuntime::Lane>           lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<UncertainReadStore>                   store   = std::make_shared<UncertainReadStore>();
    std::map<uint64_t, std::shared_ptr<const BackingRef>> backings;
    std::shared_ptr<AsyncStorePageLoader>                 loader;
    size_t                                                calls = 0;
    Code                                                  last  = Code::kOk;

    explicit Reads(Kind kind, bool chain = false, bool prepaid = false)
    {
        credits.credits.work_metrics = metrics;
        if (prepaid) {
            EXPECT_TRUE(
                ApplyDispatcher::create(runtime, lane, 1, credits.credits.bytes, &credits.credits.cpu_progress).ok());
        }
        if (kind == Kind::kLeaf) {
            auto leaf = base({
                {.key = "key", .value = "data"}
            });
            add(11, leaf->frame(), leaf->page_bytes());
        }
        else if (kind == Kind::kInner) {
            const std::unique_ptr<InnerBase> inner(InnerBase::build({"m"}, {1, 2}));
            add(11, inner->frame(), inner->page_bytes());
        }
        else {
            const std::unique_ptr<OverflowBase> first(
                OverflowBase::build(chain ? 12 : kInvalidPageId, Slice("a").bytes(), 1));
            add(11, first->frame(), first->page_bytes());
            if (chain) {
                const std::unique_ptr<OverflowBase> second(OverflowBase::build(kInvalidPageId, Slice("b").bytes(), 1));
                add(12, second->frame(), second->page_bytes());
            }
        }
        loader = std::make_shared<AsyncStorePageLoader>(
            store, runtime, lane, PageLoadLimits{.max_inflight = 1},
            [captured = backings](uint64_t id, std::shared_ptr<const BackingRef> *out) {
                *out = captured.at(id);
                return Status::Ok();
            },
            credits.credits);
    }

    void add(uint64_t id, const uint8_t *frame, uint32_t bytes)
    {
        std::vector<uint8_t> raw(frame, frame + bytes);
        frame_put_u64(raw.data(), fh::kSelfpage_id, id);
        frame_restamp_crc(raw.data(), raw.size());
        auto &blob = store->blobs[id];
        encode_durable_page(raw.data(), raw.size(), compress_algo::kNone, &blob);
        auto backing     = std::make_shared<BackingRef>();
        backing->address = id;
        backing->length  = blob.size();
        backing->owner   = std::make_shared<int>(1);
        backings[id]     = std::move(backing);
    }

    void start(Kind kind, uint64_t length = 1)
    {
        auto complete = [this](Status status, auto result) {
            ++calls;
            last = status.code();
            EXPECT_EQ(status.ok(), static_cast<bool>(result));
        };
        if (kind == Kind::kLeaf) {
            loader->load_leaf({.identity = 11, .backing = backings.at(11)}, complete);
        }
        else if (kind == Kind::kInner) {
            loader->load_inner(backings.at(11), complete);
        }
        else {
            loader->load_overflow(11, length, complete);
        }
        runtime->drain();
    }
};

TEST(PageLoadLifetime, PrepaidDecodeContinuesAcrossColdReadsWithAllOrdinaryCpuSlotsOccupied)
{
    for (auto kind : {Kind::kLeaf, Kind::kInner, Kind::kOverflow}) {
        Reads test(kind, true, true);
        test.store->throw_after_accept = false;
        test.store->inline_completion  = true;
        auto lane                      = std::make_shared<detail::FlushRuntime::Lane>(16);
        std::vector<std::shared_ptr<detail::FlushRuntime::Progress>> occupied;
        for (;;) {
            std::shared_ptr<detail::FlushRuntime::Progress> slot;
            const auto status = test.runtime->reserve_progress(lane, [](void *) noexcept {}, nullptr, {}, &slot);
            if (!status.ok()) {
                EXPECT_EQ(status.code(), Code::kResourceExhausted);
                break;
            }
            occupied.push_back(std::move(slot));
        }
        EXPECT_FALSE(occupied.empty());
        test.start(kind, kind == Kind::kOverflow ? 2 : 1);
        EXPECT_EQ(test.calls, 1U);
        EXPECT_EQ(test.last, Code::kOk);
        EXPECT_EQ(test.store->reads, kind == Kind::kOverflow ? 2U : 1U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
        test.credits.credits.cpu_progress->close_and_drain();
    }
}

TEST(PageLoadLifetime, UnknownAcceptancePinsWindowAndProgressUntilLateCallback)
{
    for (auto kind : {Kind::kLeaf, Kind::kInner, Kind::kOverflow}) {
        Reads test(kind);
        test.start(kind);
        ASSERT_EQ(test.calls, 1U);
        EXPECT_EQ(test.last, Code::kIoError);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
        test.start(kind);
        EXPECT_EQ(test.calls, 2U);
        EXPECT_EQ(test.last, Code::kResourceExhausted);
        EXPECT_EQ(test.store->reads, 1U);
        test.loader.reset();
        test.credits.seal();
        EXPECT_GT(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
        EXPECT_EQ(test.credits.resources->snapshot().read_inflight_bytes,
                  test.credits.account->snapshot().physical[static_cast<size_t>(AllocationKind::kIo)]);
        EXPECT_GT(test.credits.resources->snapshot().read_inflight_bytes, 0U);
        test.store->finish();
        EXPECT_EQ(test.credits.resources->snapshot().read_inflight_bytes, 0U);
        test.runtime->drain();
        EXPECT_EQ(test.calls, 2U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    }
}

TEST(PageLoadLifetime, TreeShutdownWaitsForUnknownTransportOwnershipAfterLogicalTerminalAndCpuDrain)
{
    for (const bool callback : {true, false}) {
        Reads                                          test(Kind::kLeaf);
        TreeOperationLifetime                          lifetime(1, test.credits.credits.bytes);
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        ASSERT_TRUE(lifetime.acquire(test.credits.credits.bytes, &ticket).ok());
        test.loader = std::make_shared<AsyncStorePageLoader>(ticket->backend(test.store.get()), test.runtime, test.lane,
                                                             PageLoadLimits{.max_inflight = 1},
                                                             OverflowBackingResolver{}, test.credits.credits);
        test.start(Kind::kLeaf);
        ASSERT_EQ(test.calls, 1U);
        EXPECT_EQ(test.last, Code::kIoError);
        ticket.reset();
        test.loader.reset();
        lifetime.close();
        test.runtime->drain();
        EXPECT_EQ(lifetime.outstanding(), 1U);
        if (callback) {
            test.store->finish();
        }
        else {
            test.store->pending.clear();
        }
        EXPECT_EQ(lifetime.outstanding(), 0U);
        lifetime.close_and_drain();
        EXPECT_EQ(test.calls, 1U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    }
}

TEST(PageLoadLifetime, InlineSuccessThenThrowDoesNotDecodeOrPublishSuccess)
{
    for (auto kind : {Kind::kLeaf, Kind::kInner, Kind::kOverflow}) {
        Reads test(kind);
        test.store->inline_completion = true;
        test.start(kind);
        EXPECT_EQ(test.calls, 1U);
        EXPECT_EQ(test.last, Code::kIoError);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
        test.loader.reset();
        test.credits.seal();
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    }
}

TEST(PageLoadLifetime, AcceptedReadAllocationFailureAlsoRetainsItsProgressCredit)
{
    for (auto kind : {Kind::kLeaf, Kind::kInner, Kind::kOverflow}) {
        Reads test(kind);
        test.store->allocation_failure = true;
        test.start(kind);
        EXPECT_EQ(test.calls, 1U);
        EXPECT_EQ(test.last, Code::kResourceExhausted);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
        test.loader.reset();
        test.credits.seal();
        test.store->finish();
        test.runtime->drain();
        EXPECT_EQ(test.calls, 1U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    }
}

TEST(PageLoadLifetime, TransportDiscardOfUnknownCallbackReleasesAdmissionForNextRead)
{
    for (auto kind : {Kind::kLeaf, Kind::kInner, Kind::kOverflow}) {
        Reads test(kind);
        test.start(kind);
        test.store->pending.clear();
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
        test.store->throw_after_accept = false;
        test.store->inline_completion  = true;
        test.start(kind);
        EXPECT_EQ(test.calls, 2U);
        EXPECT_EQ(test.last, Code::kOk);
        EXPECT_EQ(test.store->reads, 2U);
        test.loader.reset();
        test.credits.seal();
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    }
}

TEST(PageLoadLifetime, PriorOverflowCallbackCannotAffectNextInflightFrame)
{
    Reads test(Kind::kOverflow, true);
    test.store->throw_after_accept = false;
    test.start(Kind::kOverflow, 2);
    ASSERT_EQ(test.store->pending.size(), 1U);
    auto duplicate = test.store->pending.front().complete;
    test.store->finish();
    test.runtime->drain();
    ASSERT_EQ(test.store->pending.size(), 1U);
    EXPECT_EQ(test.calls, 0U);
    duplicate.complete(Status::io_error());
    test.runtime->drain();
    EXPECT_EQ(test.calls, 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
    test.store->finish();
    test.runtime->drain();
    EXPECT_EQ(test.calls, 1U);
    EXPECT_EQ(test.last, Code::kOk);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    test.loader.reset();
    test.credits.seal();
    duplicate = {};
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(PageLoadMetrics, ErrorAndBudgetRejectionHaveOneTerminalBeforeLateDuplicateCallback)
{
    for (auto kind : {Kind::kLeaf, Kind::kInner, Kind::kOverflow}) {
        Reads test(kind);
        test.start(kind);
        test.start(kind);
        const auto terminal = test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)];
        EXPECT_EQ(terminal.attempts, 2U);
        EXPECT_EQ(terminal.count, 2U);
        EXPECT_EQ(terminal.inflight, 0U);
        EXPECT_EQ(terminal.outcomes[static_cast<size_t>(TreeOutcome::Error)], 2U);
        test.loader.reset();
        test.store->finish();
        test.runtime->drain();
        const auto late = test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)];
        EXPECT_EQ(late.count, terminal.count);
        EXPECT_EQ(late.sum_ns, terminal.sum_ns);
        EXPECT_EQ(test.calls, 2U);
    }
}

TEST(PageLoadMetrics, DemandRemainsInflightUntilDecodedValueIsReady)
{
    for (auto kind : {Kind::kLeaf, Kind::kInner, Kind::kOverflow}) {
        Reads test(kind);
        test.store->throw_after_accept = false;
        test.start(kind);
        const auto accepted = test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)];
        EXPECT_EQ(accepted.attempts, 1U);
        EXPECT_EQ(accepted.inflight, 1U);
        EXPECT_EQ(accepted.count, 0U);
        test.store->finish();
        test.runtime->drain();
        const auto ready = test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)];
        EXPECT_EQ(ready.attempts, 1U);
        EXPECT_EQ(ready.inflight, 0U);
        EXPECT_EQ(ready.count, 1U);
        EXPECT_EQ(ready.outcomes[static_cast<size_t>(TreeOutcome::Success)], 1U);
        EXPECT_EQ(test.calls, 1U);
    }
}

TEST(PageLoadMetrics, DiscardingAcceptedReadOwnerCountsCancellationOnce)
{
    Reads test(Kind::kLeaf);
    test.store->throw_after_accept = false;
    test.start(Kind::kLeaf);
    test.loader.reset();
    test.store->pending.clear();
    const auto cancelled = test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)];
    EXPECT_EQ(cancelled.attempts, 1U);
    EXPECT_EQ(cancelled.inflight, 0U);
    EXPECT_EQ(cancelled.count, 1U);
    EXPECT_EQ(cancelled.outcomes[static_cast<size_t>(TreeOutcome::Cancelled)], 1U);
    EXPECT_EQ(test.calls, 0U);
    test.store.reset();
    EXPECT_EQ(test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count, 1U);
}
} // namespace
} // namespace crowdb::tree
