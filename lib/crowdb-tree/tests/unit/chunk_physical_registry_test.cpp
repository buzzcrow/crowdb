// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_runtime_page_store.h"
#include "common/test_chunk_physical.h"
#include "common/test_chunk_root.h"

#include <latch>

namespace crowdb::tree
{
namespace
{
using namespace test_chunk;

TEST(ChunkPhysicalRegistry, RepeatedObserversCountAllMirrorCapacityOnceUntilActualDelete)
{
    TestPhysicalHost host;
    auto             pack     = host.prepare();
    auto             repeated = host.prepare();
    ASSERT_EQ(pack, repeated);
    ASSERT_TRUE(host.registry->observe(pack, TestPhysicalHost::receipt, host.budget).ok());
    ASSERT_TRUE(host.registry->observe(pack, TestPhysicalHost::receipt, host.budget).ok());
    auto tail = ChunkPhysicalRegistry::extent(pack);
    pack.reset();
    repeated.reset();
    host.lane->drain();
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 8192U);
    ASSERT_TRUE(
        host.registry->deleted(TestPhysicalHost::stage, TestPhysicalHost::chunk, TestPhysicalHost::receipt, host.budget)
            .ok());
    // Metadata observers are still alive, but the physical allocation is gone.
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 0U);
    ASSERT_TRUE(
        host.registry->deleted(TestPhysicalHost::stage, TestPhysicalHost::chunk, TestPhysicalHost::receipt, host.budget)
            .ok());
    tail.reset();
    host.lane->drain();
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 0U);
}

TEST(ChunkPhysicalRegistry, OnePageStoreReopensIntoTheOriginalCanonicalPhysicalLedger)
{
    TestPhysicalHost              host;
    TestRootBackend               backend;
    detail::ChunkRuntimePageStore store({.tree = 19, .owner_epoch = 1}, backend.catalog, backend.transport,
                                        host.budget);
    auto                          other = std::make_shared<TestPhysicalBudget>();
    other->queue                        = host.queue;
    std::shared_ptr<const ChunkBackingDomain> first;
    std::shared_ptr<const ChunkBackingDomain> second;
    ASSERT_TRUE(store.make_domain(51, host.budget, &first).ok());
    ASSERT_TRUE(store.make_domain(52, other, &second).ok());
    ASSERT_EQ(first->physical_registry, second->physical_registry);
    std::shared_ptr<ChunkPhysicalPack> one;
    std::shared_ptr<ChunkPhysicalPack> two;
    ASSERT_TRUE(
        first->physical_registry->prepare(TestPhysicalHost::stage, TestPhysicalHost::chunk, host.budget, &one).ok());
    ASSERT_TRUE(first->physical_registry->observe(one, TestPhysicalHost::receipt, host.budget).ok());
    one.reset();
    host.lane->drain();
    ASSERT_TRUE(second->physical_registry->prepare(TestPhysicalHost::stage, TestPhysicalHost::chunk, other, &two).ok());
    ASSERT_TRUE(second->physical_registry->observe(two, TestPhysicalHost::receipt, other).ok());
    EXPECT_EQ(second->physical_registry->backing_metrics()->snapshot().allocated_backing_bytes, 8192U);
    EXPECT_EQ(other->metrics->snapshot().allocated_backing_bytes, 0U);
    ASSERT_TRUE(second->physical_registry
                    ->deleted(TestPhysicalHost::stage, TestPhysicalHost::chunk, TestPhysicalHost::receipt, other)
                    .ok());
    EXPECT_EQ(second->physical_registry->backing_metrics()->snapshot().allocated_backing_bytes, 0U);
}

TEST(ChunkPhysicalRegistry, ExistingPageStoreCannotMixCanonicalRolesAcrossHostQueues)
{
    TestPhysicalHost                          first_host;
    TestPhysicalHost                          other_host;
    TestRootBackend                           backend;
    detail::ChunkRuntimePageStore             store({.tree = 19, .owner_epoch = 1}, backend.catalog, backend.transport,
                                                    first_host.budget);
    std::shared_ptr<const ChunkBackingDomain> first;
    std::shared_ptr<const ChunkBackingDomain> rejected;
    ASSERT_TRUE(store.make_domain(51, first_host.budget, &first).ok());
    EXPECT_EQ(store.make_domain(52, other_host.budget, &rejected).code(), Code::kInvalidArgument);
    EXPECT_FALSE(rejected);
    EXPECT_TRUE(first->physical_registry->uses_queue(first_host.queue.get()));
}

TEST(ChunkPhysicalRegistry, ChangedIncarnationCannotRemoveAnotherPhysicalAllocation)
{
    TestPhysicalHost host;
    auto             old = host.prepare();
    ASSERT_TRUE(host.registry->observe(old, TestPhysicalHost::receipt, host.budget).ok());
    auto wrong = TestPhysicalHost::receipt;
    ++wrong.created_ms;
    EXPECT_EQ(host.registry->deleted(TestPhysicalHost::stage, TestPhysicalHost::chunk, wrong, host.budget).code(),
              Code::kCorruption);
    auto stage = TestPhysicalHost::stage;
    ++stage.owner_epoch;
    auto receipt = TestPhysicalHost::receipt;
    ++receipt.owner_epoch;
    ++receipt.created_ms;
    std::shared_ptr<ChunkPhysicalPack> fresh;
    ASSERT_TRUE(host.registry->prepare(stage, TestPhysicalHost::chunk, host.budget, &fresh).ok());
    ASSERT_TRUE(host.registry->observe(fresh, receipt, host.budget).ok());
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 16384U);
    ASSERT_TRUE(host.registry->deleted(TestPhysicalHost::stage, TestPhysicalHost::chunk, {}, host.budget).ok());
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 8192U);
    EXPECT_EQ(host.registry->deleted(stage, TestPhysicalHost::chunk, TestPhysicalHost::receipt, host.budget).code(),
              Code::kCorruption);
    ASSERT_TRUE(host.registry->deleted(stage, TestPhysicalHost::chunk, receipt, host.budget).ok());
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 0U);
}

TEST(ChunkPhysicalRegistry, SingleBackendWorkerWaitsOnlyOnTheIndependentHostRetirementLane)
{
    TestPhysicalHost     host;
    detail::FlushRuntime backend(1, 1, host.budget);
    auto                 lane = std::make_shared<detail::FlushRuntime::Lane>(1);

    struct Call
    {
        TestPhysicalHost *host;
        Status            status;
        std::latch        done{1};
    } call{.host = &host, .status = {}};

    ASSERT_TRUE(backend
                    .try_submit(
                        lane,
                        [](void *ptr) noexcept {
                            auto                              &call = *static_cast<Call *>(ptr);
                            std::shared_ptr<ChunkPhysicalPack> pack;
                            call.status = call.host->registry->prepare(TestPhysicalHost::stage, TestPhysicalHost::chunk,
                                                                       call.host->budget, &pack);
                            if (call.status.ok())
                                call.status =
                                    call.host->registry->observe(pack, TestPhysicalHost::receipt, call.host->budget);
                            call.done.count_down();
                        },
                        &call)
                    .ok());
    call.done.wait();
    lane->drain();
    ASSERT_TRUE(call.status.ok()) << call.status.to_string();
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 8192U);
    EXPECT_GT(host.queue->snapshot().cpu_steps, 0U);
}

TEST(ChunkPhysicalRegistry, CommandAdmissionRejectsBeforeEnqueueAndClosedHostAcceptsOnlyMemoryRetirement)
{
    TestPhysicalHost host;
    const auto       before = host.budget->claimed.load();
    host.budget->reject     = true;
    std::shared_ptr<ChunkPhysicalPack> pack;
    EXPECT_EQ(host.registry->prepare(TestPhysicalHost::stage, TestPhysicalHost::chunk, host.budget, &pack).code(),
              Code::kResourceExhausted);
    EXPECT_FALSE(pack);
    EXPECT_EQ(host.budget->claimed.load(), before);
    host.budget->reject = false;
    pack                = host.prepare();
    ASSERT_TRUE(host.registry->observe(pack, TestPhysicalHost::receipt, host.budget).ok());
    host.queue->close_and_drain();
    ASSERT_TRUE(
        host.registry->deleted(TestPhysicalHost::stage, TestPhysicalHost::chunk, TestPhysicalHost::receipt, host.budget)
            .ok());
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 0U);
}
} // namespace

TEST(ChunkPhysicalRegistry, EveryPreparationAllocationFailureReleasesUnallocatedCanonicalMetadata)
{
    TestPhysicalHost host;
    bool             reached_success = false;
    for (int allowance = 0; allowance < 8; ++allowance) {
        auto credit        = std::make_shared<TestPhysicalBudget>();
        credit->queue      = host.queue;
        credit->allowances = allowance;
        std::shared_ptr<ChunkPhysicalPack> pack;
        {
            ChunkPhysicalAdmission admission;
            const auto             status =
                host.registry->prepare(TestPhysicalHost::stage, TestPhysicalHost::chunk, credit, &pack, &admission);
            if (status.ok()) {
                reached_success = true;
            }
            else {
                EXPECT_EQ(status.code(), Code::kResourceExhausted) << status.to_string();
            }
            credit->reject = true;
        }
        pack.reset();
        host.lane->drain();
        EXPECT_EQ(credit->claimed.load(), 0U) << allowance;
    }
    EXPECT_TRUE(reached_success);
}
} // namespace crowdb::tree
