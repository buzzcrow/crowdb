// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_page_store.h"
#include "backend/chunk/chunk_stage.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
ChunkStageRecord reserved()
{
    return {
        .tree_id             = 42,
        .id                  = {.owner_epoch = 7, .operation_high = 8, .operation_low = 9},
        .revision            = 1,
        .chunk_high          = uint64_t{2}
                           << 56,
        .chunk_low           = 123,
        .capacity            = 4096,
        .manifest_generation = 0,
        .purpose             = 2,
        .state               = ChunkStageState::Reserved,
        .packs               = {}
    };
}

TEST(ChunkStage, CodecChecksEveryBoundBeforeAllocatingPackRecords)
{
    auto record     = reserved();
    record.revision = 2;
    record.state    = ChunkStageState::Writing;
    record.packs.push_back({.offset = 0, .length = 4096, .checksum = 0x12345678});
    std::vector<uint8_t> encoded;
    ASSERT_TRUE(encode_stage_record(record, &encoded).ok());
    ASSERT_EQ(encoded.size(), 116U);
    EXPECT_EQ(std::string(encoded.begin(), encoded.begin() + 4), "CTSG");
    ChunkStageRecord decoded;
    ASSERT_TRUE(decode_stage_record(encoded, &decoded).ok());
    EXPECT_EQ(decoded, record);
    for (size_t offset : {8, 40, 64, 84, 88, 96, 112}) {
        auto corrupt = encoded;
        corrupt[offset] ^= 1;
        EXPECT_EQ(decode_stage_record(corrupt, &decoded).code(), Code::kCorruption);
    }
    encoded[4] = 2;
    EXPECT_EQ(decode_stage_record(encoded, &decoded).code(), Code::kNotSupported);
}

TEST(ChunkStage, ProvisionalIdentityAndReferencesNeverChangeDuringTransition)
{
    const auto first   = reserved();
    auto       writing = first;
    writing.revision   = 2;
    writing.state      = ChunkStageState::Writing;
    EXPECT_TRUE(validate_stage_transition({}, first).ok());
    EXPECT_TRUE(validate_stage_transition(first, writing).ok());
    EXPECT_FALSE(validate_stage_transition(writing, {}).ok());
    auto changed = writing;
    ++changed.chunk_low;
    EXPECT_FALSE(validate_stage_transition(first, changed).ok());
    auto ready     = writing;
    ready.revision = 3;
    ready.state    = ChunkStageState::Reloadable;
    EXPECT_FALSE(validate_stage_transition(writing, ready).ok());
    ready.packs.push_back({.offset = 0, .length = 4096, .checksum = 0});
    EXPECT_TRUE(validate_stage_transition(writing, ready).ok());
    ready.packs.push_back({.offset = 1, .length = 1, .checksum = 0});
    EXPECT_FALSE(validate_stage_record(ready).ok());
}

TEST(ChunkStage, OldCatalogKeepsIndependentStagingUnavailable)
{
    MemoryRootCatalog catalog(7);
    EXPECT_FALSE(catalog.stage_schema_supported());
    const auto                      first = reserved();
    std::optional<ChunkStageRecord> loaded;
    EXPECT_EQ(catalog.load_stage(42, first.id, &loaded).code(), Code::kNotSupported);
    EXPECT_EQ(catalog.compare_stage(42, 7, {}, first).code(), Code::kNotSupported);
}
} // namespace
} // namespace crowdb::tree::detail
