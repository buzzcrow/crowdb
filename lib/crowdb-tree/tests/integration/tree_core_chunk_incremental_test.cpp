// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"
#include "snapshot/chunk_root_codec.h"

namespace crowdb::tree
{
TEST(TreeCoreChunkIncremental, UnchangedCheckpointHasZeroWritesAndSameSequenceAfterReopen)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, std::string(20000, 'v')).ok());
    test.store->request_wal_offset(4096);
    uint64_t sequence = 0;
    ASSERT_TRUE(test.tree->snapshot(nullptr, &sequence).ok());
    const auto writes = test.backend.transport->writes;
    const auto begins = test.backend.catalog->begin_calls;
    for (int iteration = 0; iteration < 2; ++iteration) {
        if (iteration != 0) {
            test.tree.reset();
            ASSERT_TRUE(test.open().ok());
        }
        uint64_t   next   = 0;
        const auto status = test.tree->snapshot(nullptr, &next);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(next, sequence);
        EXPECT_EQ(test.backend.transport->writes, writes);
        EXPECT_EQ(test.backend.catalog->begin_calls, iteration == 0 ? begins : 0U);
        EXPECT_EQ(test.store->manifest_generation(), 1U);
        EXPECT_EQ(test.tree->get_view("key").value().to_string(), std::string(20000, 'v'));
    }
    test.store->request_wal_offset(8192);
    ASSERT_TRUE(test.tree->snapshot(nullptr, &sequence).ok());
    EXPECT_EQ(sequence, 2U);
    EXPECT_EQ(test.backend.transport->writes, writes);
    EXPECT_EQ(test.store->durable_wal_offset(), 8192U);
    EXPECT_EQ(test.store->manifest_generation(), 2U);
}

TEST(TreeCoreChunkIncremental, OneLeafChangeReusesOtherIndexPacksAndOldReader)
{
    test_chunk::TestCoreChunk test(1024);
    ASSERT_TRUE(test.open().ok());
    const std::string large(600000, 'v');
    ASSERT_TRUE(test.put(1, large).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    auto held = test.tree->get_view("key");
    ASSERT_TRUE(held.found());
    ChunkRootManifest prior(test.backend.credits.credits.bytes);
    ASSERT_TRUE(decode_chunk_root(test.backend.catalog->committed_manifest, 19, 1, &prior).ok());
    ASSERT_GT(prior.indexes.size(), 1U);
    const auto allocations = test.backend.transport->ids.size();
    ASSERT_TRUE(test.tree->apply(2, Batch{.ops = {{.key = "other", .kind = OpKind::kPut, .value = "small"}}}).ok());
    const auto status = test.tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ChunkRootManifest current(test.backend.credits.credits.bytes);
    ASSERT_TRUE(decode_chunk_root(test.backend.catalog->committed_manifest, 19, 2, &current).ok());
    ASSERT_EQ(current.indexes.size(), prior.indexes.size());
    size_t changed = 0;
    for (size_t i = 0; i < prior.indexes.size(); ++i) {
        changed += current.indexes[i].pack.stage != prior.indexes[i].pack.stage ? 1 : 0;
    }
    EXPECT_EQ(changed, 1U);
    EXPECT_EQ(test.backend.transport->ids.size() - allocations, 2U);
    EXPECT_EQ(held.value().to_string(), large);
    test.tree.reset();
    const auto reopened = test.open();
    ASSERT_TRUE(reopened.ok()) << reopened.to_string();
    EXPECT_EQ(test.tree->get_view("key").value().to_string(), large);
    EXPECT_EQ(test.tree->get_view("other").value().to_string(), "small");
    const auto writes = test.backend.transport->writes;
    ASSERT_TRUE(test.tree->snapshot().ok());
    EXPECT_EQ(test.backend.transport->writes, writes);
    EXPECT_EQ(held.value().to_string(), large);
}
} // namespace crowdb::tree
