// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"

namespace crowdb::tree
{
TEST(ChunkPhysicalCore, SingleIoCheckpointReportsAllPhysicalMirrorAllocationsIncludingIndexPacks)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, std::string(20000, 'x')).ok());
    auto status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    uint64_t expected = 0;
    bool     index    = false;
    for (const auto &[identity, receipt] : test.backend.transport->receipts) {
        (void)identity;
        expected += receipt.physical_bytes;
        index |= detail::chunk_purpose(receipt.chunk) == PagePurpose::kPageIndex;
    }
    ASSERT_TRUE(index);
    ASSERT_GT(expected, 0U);
    EXPECT_EQ(test.tree->tree_state_stats().resources.allocated_backing_bytes, expected);
    const auto first = expected;
    ASSERT_TRUE(test.put(2, "small").ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    expected = 0;
    for (const auto &[identity, receipt] : test.backend.transport->receipts) {
        (void)identity;
        expected += receipt.physical_bytes;
    }
    EXPECT_GT(expected, first);
    EXPECT_EQ(test.tree->tree_state_stats().resources.allocated_backing_bytes, expected);
}
} // namespace crowdb::tree
