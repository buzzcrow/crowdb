// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_stage_gc.h"
#include "common/test_chunk_stage_backend.h"

namespace crowdb::tree::detail
{
namespace
{
ChunkStageRecord retired_stage()
{
    return {
        .tree_id             = 17,
        .id                  = {.owner_epoch = 1, .operation_high = 10, .operation_low = 20},
        .revision            = 2,
        .chunk_high          = uint64_t{2}
                           << 56,
        .chunk_low           = 1,
        .capacity            = 4096,
        .manifest_generation = 0,
        .purpose             = 2,
        .state               = ChunkStageState::Retired,
        .packs               = {}
    };
}
} // namespace

TEST(ChunkStageGc, UnknownDeleteKeepsDurableProtectionUntilRetryConfirmsPhysicalDeletion)
{
    auto catalog    = std::make_shared<TestStageCatalog>();
    catalog->record = retired_stage();
    TestFixedTransport transport(catalog);
    transport.unknown_delete = true;
    const auto id            = retired_stage().id;
    auto       result        = reclaim_retired_stage(*catalog, transport, 17, 1, id);
    EXPECT_FALSE(result.status.ok());
    EXPECT_FALSE(result.stage_removed);
    EXPECT_FALSE(result.deletion_acknowledged);
    EXPECT_TRUE(transport.physically_deleted);
    if (!catalog->record.has_value()) {
        FAIL() << "unknown delete dropped stage";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Deleting);
    transport.unknown_delete = false;
    result                   = reclaim_retired_stage(*catalog, transport, 17, 1, id);
    EXPECT_TRUE(result.status.ok());
    EXPECT_TRUE(result.deletion_acknowledged);
    EXPECT_TRUE(result.stage_removed);
    EXPECT_FALSE(catalog->record.has_value());
    EXPECT_EQ(transport.deletions, 2U);
}

TEST(ChunkStageGc, UnconfirmedDiskDrainRetainsDeletingStageWithoutPhysicalDelete)
{
    auto catalog    = std::make_shared<TestStageCatalog>();
    catalog->record = retired_stage();
    TestFixedTransport transport(catalog);
    transport.fail_drain = true;
    const auto id        = retired_stage().id;
    auto       result    = reclaim_retired_stage(*catalog, transport, 17, 1, id);
    EXPECT_FALSE(result.status.ok());
    EXPECT_FALSE(result.stage_removed);
    EXPECT_FALSE(result.deletion_acknowledged);
    EXPECT_EQ(transport.deletions, 0U);
    if (!catalog->record.has_value()) {
        FAIL() << "unconfirmed disk drain dropped stage";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Deleting);
    transport.fail_drain = false;
    result               = reclaim_retired_stage(*catalog, transport, 17, 1, id);
    EXPECT_TRUE(result.status.ok());
    EXPECT_TRUE(result.stage_removed);
    EXPECT_EQ(transport.drains, 2U);
    EXPECT_EQ(transport.deletions, 1U);
}

TEST(ChunkStageGc, LiveStageAndFutureEpochCannotAuthorizeDelete)
{
    auto catalog    = std::make_shared<TestStageCatalog>();
    auto record     = retired_stage();
    record.state    = ChunkStageState::Reserved;
    record.revision = 1;
    catalog->record = record;
    TestFixedTransport transport(catalog);
    EXPECT_FALSE(reclaim_retired_stage(*catalog, transport, 17, 1, record.id).status.ok());
    catalog->record = retired_stage();
    EXPECT_FALSE(reclaim_retired_stage(*catalog, transport, 17, 0, record.id).status.ok());
    EXPECT_EQ(transport.deletions, 0U);
    EXPECT_TRUE(catalog->record.has_value());
}

TEST(ChunkStageGc, ChangedPhysicalEpochCannotDeleteUsingTheOldStage)
{
    auto catalog    = std::make_shared<TestStageCatalog>();
    catalog->record = retired_stage();
    TestFixedTransport transport(catalog);
    transport.physical_epoch = 2;
    const auto result        = reclaim_retired_stage(*catalog, transport, 17, 2, retired_stage().id);
    EXPECT_EQ(result.status.code(), Code::kUnavailable);
    EXPECT_FALSE(result.deletion_acknowledged);
    EXPECT_FALSE(result.stage_removed);
    EXPECT_EQ(transport.drains, 0U);
    EXPECT_EQ(transport.deletions, 0U);
    ASSERT_TRUE(catalog->record);
    EXPECT_EQ(catalog->record->state, ChunkStageState::Deleting);
}
} // namespace crowdb::tree::detail
