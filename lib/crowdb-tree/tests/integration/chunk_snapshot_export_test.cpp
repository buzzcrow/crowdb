// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"
#include "crowdb-tree/snapshot/snapshot_io.h"

namespace crowdb::tree
{
namespace
{
class ChunkSnapshotExport : public testing::TestWithParam<snapshot_format>
{
};

TEST_P(ChunkSnapshotExport, ColdCapturedStreamImportsAfterSourceWriterReplacementAndClose)
{
    test_chunk::TestCoreChunk source;
    ASSERT_TRUE(source.open().ok());
    const std::string overflow(20'000, 'v');
    Batch             batch;
    for (size_t i = 0; i < 32; ++i) {
        batch.ops.push_back({.key = "key" + std::to_string(1000 + i), .kind = OpKind::kPut, .value = "old"});
    }
    batch.ops.push_back({.key = "overflow", .kind = OpKind::kPut, .value = overflow});
    batch.ops.push_back({.key = "gone", .kind = OpKind::kPut, .value = "deleted"});
    ASSERT_TRUE(source.tree->apply(1, batch).ok());
    ASSERT_TRUE(source.tree->apply(2, Batch{.ops = {{.key = "gone", .kind = OpKind::kDelete, .value = ""}}}).ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    source.tree.reset();
    ASSERT_TRUE(source.open().ok());
    std::unique_ptr<SnapshotExport> exporter;
    auto                            status = snapshot_export_begin(*source.tree, GetParam(), 4093, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(exporter->at_slot(), 2U);
    ASSERT_TRUE(source.tree
                    ->apply(3,
                            Batch{
                                .ops = {{.key = "overflow", .kind = OpKind::kPut, .value = "new"},
                                        {.key = "later", .kind = OpKind::kPut, .value = "new"}}
    })
                    .ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    source.tree.reset();

    test_chunk::TestCoreChunk destination;
    ASSERT_TRUE(destination.open().ok());
    SnapshotImport importer(*destination.tree);
    uint64_t       bytes = 0;
    bool           done  = false;
    while (!done) {
        SnapshotChunk chunk;
        status = exporter->next_owned(&chunk, &done);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_LE(chunk.bytes.size(), 4093U);
        if (!chunk.bytes.empty()) {
            ASSERT_TRUE(chunk.owner);
            ASSERT_TRUE(chunk.allocation_budget);
            status =
                importer.feed(std::string_view(reinterpret_cast<const char *>(chunk.bytes.data()), chunk.bytes.size()));
            ASSERT_TRUE(status.ok()) << status.to_string();
            bytes += chunk.bytes.size();
        }
        EXPECT_EQ(source.config.runtime->snapshot().reserved.io_operations, 0U);
    }
    EXPECT_EQ(bytes, exporter->total_bytes());
    uint64_t slot = 0;
    status        = importer.finish(&slot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(slot, 2U);
    exporter.reset();
    ASSERT_TRUE(destination.tree->snapshot().ok());
    destination.tree.reset();
    ASSERT_TRUE(destination.open().ok());
    EXPECT_EQ(destination.tree->durable_snapshot_last_applied_slot(), 2U);
    EXPECT_EQ(destination.tree->get_view("overflow").value().to_string(), overflow);
    EXPECT_FALSE(destination.tree->get_view("gone").found());
    EXPECT_FALSE(destination.tree->get_view("later").found());
    for (size_t i = 0; i < 32; ++i) {
        const auto value = destination.tree->get_view("key" + std::to_string(1000 + i));
        ASSERT_TRUE(value.found());
        EXPECT_EQ(value.value().to_string(), "old");
    }
}

INSTANTIATE_TEST_SUITE_P(PublicFormats, ChunkSnapshotExport,
                         testing::Values(snapshot_format::kPortable, snapshot_format::kNative));
} // namespace
} // namespace crowdb::tree
