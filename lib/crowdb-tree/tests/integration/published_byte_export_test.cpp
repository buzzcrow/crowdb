// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/snapshot/snapshot_io.h"
#include "local_checkpoint_test_support.h"

namespace crowdb::tree
{
namespace
{
struct TestByteBackend
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;

    TestByteBackend()
    {
        store.device = device;
    }
};

struct TestByteExport
{
    std::shared_ptr<TestByteBackend> backend = std::make_shared<TestByteBackend>();
    Config                           config;
    std::unique_ptr<Crowdbtree>      tree;

    TestByteExport()
    {
        EXPECT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.page_store                           = backend->device.get();
        config.async_page_store                     = &backend->store;
        config.backend_owner                        = backend;
        config.frame_bytes                          = 4096;
        config.max_inline_value                     = 64;
        config.runtime_binding.limits.io_operations = 1;
    }

    [[nodiscard]] Status open()
    {
        return Crowdbtree::open(config, &tree);
    }

    static Status collect(SnapshotExport &exporter, std::string *wire, std::vector<SnapshotChunk> *retained = nullptr)
    {
        bool done = false;
        while (!done) {
            SnapshotChunk chunk;
            auto          status = exporter.next_owned(&chunk, &done);
            if (!status.ok()) {
                return status;
            }
            if (!chunk.bytes.empty()) {
                if (!chunk.owner || !chunk.allocation_budget) {
                    return Status::internal_error("runtime export returned unaccounted bytes");
                }
                wire->append(reinterpret_cast<const char *>(chunk.bytes.data()), chunk.bytes.size());
            }
            if (retained != nullptr) {
                retained->push_back(std::move(chunk));
            }
        }
        return Status::Ok();
    }
};

TEST(PublishedByteExport, PortableMatchesExistingWireAndRetainsOneCapturedCoverage)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    const std::string value(12000, 'v');
    ASSERT_TRUE(test.tree
                    ->apply(1,
                            Batch{
                                .ops = {{.key = "a", .kind = OpKind::kPut, .value = value},
                                        {.key = "empty", .kind = OpKind::kPut, .value = ""},
                                        {.key = "gone", .kind = OpKind::kPut, .value = "old"}}
    })
                    .ok());
    ASSERT_TRUE(test.tree->apply(2, Batch{.ops = {{.key = "gone", .kind = OpKind::kDelete, .value = ""}}}).ok());
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kPortable, 7, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(exporter->at_slot(), 2U);
    ASSERT_TRUE(test.tree->apply(3, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "changed"}}}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    std::vector<leaf_entry> expected;
    expected.push_back({.key = "a", .cell = encode_cell_buf(1, OpKind::kPut, value)});
    expected.push_back({.key = "empty", .cell = encode_cell_buf(1, OpKind::kPut, "")});
    expected.push_back({.key = "gone", .cell = encode_cell_buf(2, OpKind::kDelete)});
    SnapshotExport reference(std::make_shared<Snapshot>(2, std::move(expected)), 4096);
    std::string    expected_wire;
    bool           done = false;
    while (!done) {
        std::string piece;
        ASSERT_TRUE(reference.next_chunk(&piece, &done).ok());
        expected_wire += piece;
    }
    EXPECT_EQ(exporter->total_bytes(), expected_wire.size());
    EXPECT_EQ(exporter->final_crc32c(), reference.final_crc32c());
    std::string                wire;
    std::vector<SnapshotChunk> retained;
    status = TestByteExport::collect(*exporter, &wire, &retained);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(wire, expected_wire);
    EXPECT_EQ(exporter->offset(), exporter->total_bytes());
    EXPECT_EQ(test.config.runtime->snapshot().reserved.io_operations, 0U);
    exporter.reset();
    test.tree.reset();
    std::string owned_again;
    for (const auto &chunk : retained) {
        if (!chunk.bytes.empty()) {
            owned_again.append(reinterpret_cast<const char *>(chunk.bytes.data()), chunk.bytes.size());
        }
    }
    EXPECT_EQ(owned_again, wire);
}

TEST(PublishedByteExport, NativeColdOverflowUsesTheSamePidLeaseForBothPasses)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    const std::string value(14000, 'n');
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = value}}}).ok());
    ASSERT_TRUE(test.tree->snapshot().ok());
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kNative, 4093, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto  next = TreeCoreAccess::get(*test.tree)->capture()->next_page_id;
    std::string wire;
    status = TestByteExport::collect(*exporter, &wire);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(TreeCoreAccess::get(*test.tree)->capture()->next_page_id, next);
    EXPECT_EQ(wire.size(), exporter->total_bytes());
    const auto crc = crowdb::common::crc32c(reinterpret_cast<const uint8_t *>(wire.data()), wire.size() - 4);
    EXPECT_EQ(crc, exporter->final_crc32c());
    Config target;
    target.frame_bytes = 4096;
    Crowdbtree     copy(target);
    SnapshotImport importer(copy);
    ASSERT_TRUE(importer.feed(wire).ok());
    uint64_t slot = 0;
    status        = importer.finish(&slot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(slot, 1U);
    std::string restored;
    ASSERT_TRUE(copy.get("a", &slot, &restored));
    EXPECT_EQ(restored, value);
}

TEST(PublishedByteExport, NativeFreshOverflowRewindsWithoutReservingAnotherInterval)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    const std::string value(14000, 'f');
    ASSERT_TRUE(test.tree->put("fresh", value).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    auto                           *core   = TreeCoreAccess::get(*test.tree);
    const auto                      before = core->capture()->next_page_id;
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kNative, 4093, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto leased = core->capture()->next_page_id;
    EXPECT_GT(leased, before);
    std::string wire;
    status = TestByteExport::collect(*exporter, &wire);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->capture()->next_page_id, leased);
    Config target;
    target.frame_bytes = 4096;
    Crowdbtree     copy(target);
    SnapshotImport importer(copy);
    ASSERT_TRUE(importer.feed(wire).ok());
    status = importer.finish(nullptr);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::string restored;
    uint64_t    slot = 0;
    ASSERT_TRUE(copy.get("fresh", &slot, &restored));
    EXPECT_EQ(restored, value);
}

TEST(PublishedByteExport, EmptyPortableSupportsEveryHeaderAndTrailerByteBoundary)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kPortable, 1, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::string wire;
    status = TestByteExport::collect(*exporter, &wire);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(wire.size(), 29U);
    EXPECT_EQ(exporter->total_bytes(), 29U);
    EXPECT_EQ(exporter->offset(), 29U);
}

TEST(PublishedByteExport, OversizeChunkFailsBeforeHandoffOrCapture)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    ASSERT_TRUE(test.tree->put("a", "pending").ok());
    auto                           *core   = TreeCoreAccess::get(*test.tree);
    const auto                      before = core->capture();
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kPortable, kSnapshotChunkBytes + 1, &exporter);
    EXPECT_EQ(status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(exporter);
    EXPECT_EQ(core->capture(), before);
    EXPECT_EQ(core->capture()->coverage, 0U);
}
} // namespace
} // namespace crowdb::tree
