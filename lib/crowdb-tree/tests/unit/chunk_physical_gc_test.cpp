// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_stage_gc.h"
#include "common/test_chunk_physical.h"
#include "common/test_chunk_stage_backend.h"

namespace crowdb::tree
{
namespace
{
using namespace test_chunk;
using namespace detail;

ChunkStageRecord retired()
{
    return {.tree_id             = 17,
            .id                  = TestPhysicalHost::stage,
            .revision            = 2,
            .chunk_high          = TestPhysicalHost::chunk.high,
            .chunk_low           = TestPhysicalHost::chunk.low,
            .capacity            = 4096,
            .manifest_generation = 0,
            .purpose             = 2,
            .state               = ChunkStageState::Retired,
            .packs               = {}};
}

TEST(ChunkPhysicalGc, LostDeleteReplyKeepsPhysicalChargeUntilDeletingNotFoundRetryAcknowledges)
{
    TestPhysicalHost host;
    auto             catalog = std::make_shared<TestStageCatalog>();
    catalog->record          = retired();
    TestFixedTransport transport(catalog);
    transport.unknown_delete      = true;
    transport.absent_after_delete = true;
    auto result =
        reclaim_retired_stage(*catalog, transport, 17, 1, TestPhysicalHost::stage, host.budget, host.registry);
    EXPECT_FALSE(result.status.ok());
    EXPECT_FALSE(result.deletion_acknowledged);
    EXPECT_TRUE(transport.physically_deleted);
    if (!catalog->record) {
        FAIL() << "expected protected stage";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Deleting);
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 8192U);
    transport.unknown_delete = false;
    result = reclaim_retired_stage(*catalog, transport, 17, 1, TestPhysicalHost::stage, host.budget, host.registry);
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    EXPECT_TRUE(result.stage_removed);
    EXPECT_TRUE(result.deletion_acknowledged);
    EXPECT_EQ(transport.deletions, 2U);
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 0U);
}

TEST(ChunkPhysicalGc, ReusedChunkEpochCannotAuthorizeDeleteOrRemoveItsPhysicalCharge)
{
    TestPhysicalHost host;
    auto             catalog = std::make_shared<TestStageCatalog>();
    catalog->record          = retired();
    TestFixedTransport transport(catalog);
    transport.physical_epoch = 2;
    auto stage               = TestPhysicalHost::stage;
    ++stage.owner_epoch;
    auto receipt = TestPhysicalHost::receipt;
    ++receipt.owner_epoch;
    std::shared_ptr<ChunkPhysicalPack> pack;
    ASSERT_TRUE(host.registry->prepare(stage, TestPhysicalHost::chunk, host.budget, &pack).ok());
    ASSERT_TRUE(host.registry->observe(pack, receipt, host.budget).ok());
    const auto result =
        reclaim_retired_stage(*catalog, transport, 17, 2, TestPhysicalHost::stage, host.budget, host.registry);
    EXPECT_EQ(result.status.code(), Code::kUnavailable);
    EXPECT_EQ(transport.deletions, 0U);
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 8192U);
}

TEST(ChunkPhysicalGc, UnknownAllocateReplyAlreadyAccountsTheExactMirrorsWhileStageRemainsReserved)
{
    TestPhysicalHost host;
    auto             catalog    = std::make_shared<TestStageCatalog>();
    auto             transport  = std::make_shared<TestFixedTransport>(catalog);
    transport->unknown_allocate = true;
    ChunkFixedPackStore                store(17, 1, 2, catalog, transport, host.registry);
    std::array<uint8_t, 32>            input{};
    ChunkFixedPackRef                  output;
    std::shared_ptr<ChunkPhysicalPack> physical;
    const auto status = store.write(TestPhysicalHost::stage, TestPhysicalHost::chunk, PagePurpose::kBtreePage, input,
                                    host.budget, &output, &physical);
    EXPECT_EQ(status.code(), Code::kUnavailable);
    if (!catalog->record) {
        FAIL() << "expected protected stage";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Reserved);
    EXPECT_EQ(transport->allocations, 1U);
    EXPECT_NE(catalog->record->capacity, 8192U);
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 8192U);
    EXPECT_EQ(output.length, 0U);
}

TEST(ChunkPhysicalGc, MissingCommandCreditRejectsBeforeExternalAllocation)
{
    TestPhysicalHost    host;
    auto                catalog   = std::make_shared<TestStageCatalog>();
    auto                transport = std::make_shared<TestFixedTransport>(catalog);
    ChunkFixedPackStore store(17, 1, 2, catalog, transport, host.registry);
    host.budget->reject = true;
    std::array<uint8_t, 32>            input{};
    ChunkFixedPackRef                  output;
    std::shared_ptr<ChunkPhysicalPack> physical;
    EXPECT_EQ(store
                  .write(TestPhysicalHost::stage, TestPhysicalHost::chunk, PagePurpose::kBtreePage, input, host.budget,
                         &output, &physical)
                  .code(),
              Code::kResourceExhausted);
    EXPECT_EQ(transport->allocations, 0U);
    EXPECT_FALSE(catalog->record);
    host.budget->reject = false;
}
} // namespace

TEST(ChunkPhysicalGc, AdvertisedWriteBoundPaysCanonicalOwnersAtTheHardLimit)
{
    TestPhysicalHost                host;
    auto                            catalog   = std::make_shared<TestStageCatalog>();
    auto                            transport = std::make_shared<TestFixedTransport>(catalog);
    ChunkFixedPackStore             store(17, 1, 2, catalog, transport, host.registry);
    const std::array<uint8_t, 4096> input{};
    const auto                      bound = ChunkFixedPackStore::write_allocation_bound(input.size());
    if (!bound) {
        FAIL() << "expected bounded admission";
        return;
    }
    auto credit   = std::make_shared<TestPhysicalBudget>();
    credit->queue = host.queue;
    credit->limit = *bound;
    ChunkFixedPackRef                  output;
    std::shared_ptr<ChunkPhysicalPack> physical;
    const auto status = store.write(TestPhysicalHost::stage, TestPhysicalHost::chunk, PagePurpose::kBtreePage, input,
                                    credit, &output, &physical);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_LE(credit->claimed.load(), *bound);
    EXPECT_EQ(host.registry->backing_stats().allocated_bytes, 8192U);
    ASSERT_TRUE(host.registry->deleted(TestPhysicalHost::stage, TestPhysicalHost::chunk, {}, credit).ok());
    physical.reset();
    catalog->record.reset();
    host.lane->drain();
    EXPECT_EQ(credit->claimed.load(), 0U);
}

TEST(ChunkPhysicalGc, RejectedStageReturnsAllPreallocatedRegistryMetadataWithoutAllocatingRollback)
{
    TestPhysicalHost host;
    auto             catalog      = std::make_shared<TestStageCatalog>();
    catalog->record               = retired();
    auto                transport = std::make_shared<TestFixedTransport>(catalog);
    ChunkFixedPackStore store(17, 1, 2, catalog, transport, host.registry);
    auto                credit = std::make_shared<TestPhysicalBudget>();
    credit->queue              = host.queue;
    std::array<uint8_t, 32>            input{};
    ChunkFixedPackRef                  output;
    std::shared_ptr<ChunkPhysicalPack> physical;
    const auto status = store.write(TestPhysicalHost::stage, TestPhysicalHost::chunk, PagePurpose::kBtreePage, input,
                                    credit, &output, &physical);
    EXPECT_EQ(status.code(), Code::kUnavailable);
    EXPECT_EQ(transport->allocations, 0U);
    credit->reject = true;
    physical.reset();
    host.lane->drain();
    EXPECT_EQ(credit->claimed.load(), 0U);
    credit->reject = false;
}

TEST(ChunkPhysicalGc, DurableReclaimedReceiptEndsObservationBeforeInterruptedMetadataEraseRetries)
{
    TestPhysicalHost host;
    auto             pack = host.prepare();
    ASSERT_TRUE(host.registry->observe(pack, TestPhysicalHost::receipt, host.budget).ok());
    auto catalog              = std::make_shared<TestStageCatalog>();
    catalog->record           = retired();
    catalog->record->state    = ChunkStageState::Reclaimed;
    catalog->record->revision = 4;
    TestFixedTransport transport(catalog);
    transport.physically_deleted  = true;
    transport.absent_after_delete = true;
    const auto result =
        reclaim_retired_stage(*catalog, transport, 17, 1, TestPhysicalHost::stage, host.budget, host.registry);
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    EXPECT_TRUE(result.stage_removed);
    EXPECT_EQ(transport.deletions, 0U);
    EXPECT_EQ(host.registry->backing_metrics()->snapshot().allocated_backing_bytes, 0U);
}
} // namespace crowdb::tree
