// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_chunk_collector.h"
#include "runtime/bootstrap_budget.h"

namespace crowdb::tree::detail
{
TEST(ChunkCollector, ReferencedOrPinnedStageRemainsUntilActualOwnersRelease)
{
    auto catalog        = std::make_shared<TestCollectorCatalog>();
    catalog->record     = test_collector_stage();
    catalog->references = 1;
    catalog->pins       = 65;
    TestFixedTransport      transport(catalog);
    ChunkCollector          collector;
    auto                    budget = BootstrapBudget::create(1024 * 1024);
    ChunkCollectionProgress progress;
    for (size_t step = 0; step < 12; ++step) {
        ASSERT_TRUE(collector.step(*catalog, transport, 17, 1, budget, &progress).ok());
    }
    EXPECT_EQ(transport.deletions, 0U);
    EXPECT_EQ(catalog->largest_pin_window, 64U);
    EXPECT_GT(catalog->pin_calls, 1U);
    catalog->references = 0;
    for (size_t step = 0; step < 12; ++step) {
        ASSERT_TRUE(collector.step(*catalog, transport, 17, 1, budget, &progress).ok());
    }
    EXPECT_EQ(transport.deletions, 0U);
    catalog->pins = 0;
    for (size_t step = 0; step < 12 && catalog->record; ++step) {
        ASSERT_TRUE(collector.step(*catalog, transport, 17, 1, budget, &progress).ok());
    }
    EXPECT_FALSE(catalog->record);
    EXPECT_EQ(transport.drains, 1U);
    EXPECT_EQ(transport.deletions, 1U);
    EXPECT_EQ(progress.logical_capacity_bytes, 4096U);
    EXPECT_EQ(progress.stages_removed, 1U);
}

TEST(ChunkCollector, UnknownReferenceReleaseReloadsSameCandidateBeforeAdvancing)
{
    auto catalog        = std::make_shared<TestCollectorCatalog>();
    catalog->record     = test_collector_stage();
    catalog->references = 2;
    catalog->candidate  = {
        .id                  = {.owner_epoch = 1, .operation_high = 3, .operation_low = 4},
        .generation          = 1,
        .reference_count     = 65,
        .prepared_references = 65,
        .prepared_windows    = 2,
        .released_windows    = 0,
        .state               = ChunkCandidateState::Aborted
    };
    TestFixedTransport      transport(catalog);
    ChunkCollector          collector;
    auto                    budget = BootstrapBudget::create(1024 * 1024);
    ChunkCollectionProgress progress;
    catalog->lose_release = true;
    EXPECT_EQ(collector.step(*catalog, transport, 17, 1, budget, &progress).code(), Code::kUnavailable);
    EXPECT_EQ(catalog->references, 1U);
    catalog->lose_release = false;
    for (size_t step = 0; step < 20 && catalog->record; ++step) {
        ASSERT_TRUE(collector.step(*catalog, transport, 17, 1, budget, &progress).ok());
    }
    EXPECT_EQ(catalog->references, 0U);
    EXPECT_EQ(catalog->release_calls, 2U);
    EXPECT_FALSE(catalog->record);
    EXPECT_EQ(transport.deletions, 1U);
}

TEST(ChunkCollector, UnknownPhysicalDeleteKeepsIdentityForTheNextAcceptedPass)
{
    auto catalog           = std::make_shared<TestCollectorCatalog>();
    catalog->record        = test_collector_stage();
    catalog->record->state = ChunkStageState::Retired;
    TestFixedTransport transport(catalog);
    transport.unknown_delete = true;
    ChunkCollector          collector;
    auto                    budget = BootstrapBudget::create(1024 * 1024);
    ChunkCollectionProgress progress;
    ASSERT_TRUE(collector.step(*catalog, transport, 17, 2, budget, &progress).ok());
    EXPECT_EQ(collector.step(*catalog, transport, 17, 2, budget, &progress).code(), Code::kUnavailable);
    ASSERT_TRUE(catalog->record);
    if (catalog->record) {
        EXPECT_EQ(catalog->record->state, ChunkStageState::Deleting);
    }
    EXPECT_EQ(progress.logical_capacity_bytes, 0U);
    transport.unknown_delete = false;
    ASSERT_TRUE(collector.step(*catalog, transport, 17, 2, budget, &progress).ok());
    EXPECT_FALSE(catalog->record);
    EXPECT_EQ(progress.logical_capacity_bytes, 4096U);
    EXPECT_EQ(transport.deletions, 2U);
}

TEST(ChunkCollector, MissingRecoveryProofCannotRetireOrDeleteOlderStages)
{
    auto catalog       = std::make_shared<TestCollectorCatalog>();
    catalog->record    = test_collector_stage();
    catalog->recovered = false;
    TestFixedTransport      transport(catalog);
    ChunkCollector          collector;
    auto                    budget = BootstrapBudget::create(1024 * 1024);
    ChunkCollectionProgress progress;
    ASSERT_TRUE(collector.step(*catalog, transport, 17, 2, budget, &progress).ok());
    EXPECT_EQ(collector.step(*catalog, transport, 17, 2, budget, &progress).code(), Code::kUnavailable);
    EXPECT_EQ(transport.deletions, 0U);
    catalog->recovered = true;
    for (size_t step = 0; step < 8 && catalog->record; ++step) {
        ASSERT_TRUE(collector.step(*catalog, transport, 17, 2, budget, &progress).ok());
    }
    EXPECT_FALSE(catalog->record);
    EXPECT_EQ(transport.deletions, 1U);
}
} // namespace crowdb::tree::detail
