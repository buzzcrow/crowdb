// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_frame.h"
#include "backend/chunk/chunk_runtime_page_store.h"
#include "backend/chunk/legacy_chunk_recovery.h"
#include "c_api_internal.h"
#include "common/test_legacy_chunk.h"
#include "crowdb-tree/chunk_migration_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
Config configuration(test_chunk::TestLegacyChunk &fixture)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    EXPECT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    EXPECT_TRUE(detail::ChunkPinReleaseQueue::create(
                    fixture.credits.credits.bytes, 256, std::make_shared<int>(1), [](void *) noexcept {}, nullptr,
                    &fixture.catalog->queue)
                    .ok());
    auto store = std::make_shared<detail::ChunkRuntimePageStore>(
        detail::ChunkRuntimeOptions{.tree = 42, .owner_epoch = 7, .iu_size = 1, .mirrors = 2}, fixture.catalog,
        fixture.transport, fixture.credits.credits.bytes);
    config.page_store                           = store.get();
    config.backend_owner                        = std::move(store);
    config.frame_bytes                          = 4096;
    config.leaf_split_bytes                     = 3000;
    config.max_inline_value                     = 64;
    config.runtime_binding.limits.io_operations = 1;
    return config;
}

void close_queue(test_chunk::TestLegacyChunk &fixture)
{
    fixture.catalog->queue->stop_admission();
    (void)fixture.catalog->queue->pump(256);
    EXPECT_TRUE(fixture.catalog->queue->drained());
}
} // namespace

TEST(LegacyChunkMigration, ReadOnlyAdapterVerifiesActualLegacyWriterFramesAndPinsGeneration)
{
    test_chunk::TestLegacyChunk fixture;
    auto                        reader = fixture.reader();
    const auto                  status = reader->open(1);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(fixture.catalog->generation_pins.size(), 1U);
    EXPECT_EQ(reader->size(), 110979U);
    for (size_t i = 0; i < fixture.layout.pack_count(); ++i) {
        const auto           pack = fixture.layout.pack(i);
        std::vector<uint8_t> value(pack.ref.length);
        const auto           loaded = reader->read_at(pack.logical_offset, value.data(), value.size());
        ASSERT_TRUE(loaded.ok()) << loaded.to_string();
        const auto &physical = fixture.physical.at({pack.ref.chunk_id.high, pack.ref.chunk_id.low});
        EXPECT_TRUE(
            std::equal(value.begin(), value.end(), physical.begin() + pack.ref.offset + protocol::kFrameV2HeaderBytes));
    }
    const uint8_t byte = 0;
    EXPECT_EQ(reader->write_at(0, &byte, 1).code(), Code::kNotSupported);
    ASSERT_TRUE(reader->release_pin().ok());
    EXPECT_TRUE(fixture.catalog->generation_pins.empty());
}

TEST(LegacyChunkMigration, CorruptedReferenceSegmentAndWrongGenerationDoNotPermitReads)
{
    test_chunk::TestLegacyChunk fixture;
    auto                        reader = fixture.reader();
    EXPECT_EQ(reader->open(2).code(), Code::kUnavailable);
    EXPECT_TRUE(fixture.catalog->generation_pins.empty());
    fixture.catalog->segments.begin()->second.back() ^= 1;
    EXPECT_EQ(reader->open(1).code(), Code::kCorruption);
    EXPECT_EQ(fixture.catalog->generation_pins.size(), 1U);
    uint8_t byte = 0;
    EXPECT_EQ(reader->read_at(0, &byte, 1).code(), Code::kInvalidArgument);
    ASSERT_TRUE(reader->release_pin().ok());
}

TEST(LegacyChunkMigration, ActualLegacyInnerAndOverflowRecoverIntoReadOnlyCapturedGraph)
{
    test_chunk::TestLegacyChunk fixture;
    {
        auto                                          config = configuration(fixture);
        std::shared_ptr<detail::LegacyChunkReadStore> reader;
        std::shared_ptr<TreeCore>                     source;
        const auto status = detail::recover_legacy_chunk_source(config, 1, &reader, &source);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(source->durable_coverage(), 20U);
        GetView view;
        ASSERT_TRUE(source->get("key20", &view).ok());
        EXPECT_EQ(view.slot(), 20U);
        EXPECT_EQ(view.value().to_string(), std::string(8192, 'u'));
        view = {};
        source.reset();
        ASSERT_TRUE(reader->release_pin().ok());
        reader.reset();
    }
    close_queue(fixture);
}

TEST(LegacyChunkMigration, ExplicitMigrationCommitsTheSameRootAndReopensWithNewFormat)
{
    test_chunk::TestLegacyChunk fixture;
    {
        auto                        config = configuration(fixture);
        std::unique_ptr<Crowdbtree> tree;
        const auto                  status = Crowdbtree::migrate_legacy_chunk(config, 1, &tree);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(fixture.catalog->current_generation, 2U);
        EXPECT_TRUE(fixture.catalog->generation_pins.empty());
        std::string value;
        uint64_t    slot = 0;
        ASSERT_TRUE(tree->get("key20", &slot, &value));
        EXPECT_EQ(slot, 20U);
        EXPECT_EQ(value, std::string(8192, 'u'));
        std::vector<scan_entry> rows;
        bool                    truncated = false;
        ASSERT_TRUE(tree->scan({}, {}, {}, 0, 1024 * 1024, false, 0, &rows, &truncated).ok());
        EXPECT_FALSE(truncated);
        ASSERT_EQ(rows.size(), 20U);
        tree.reset();
        (void)fixture.catalog->queue->pump(256);
        auto next                  = fixture.catalog->next_owner();
        fixture.transport->catalog = next;
        auto store                 = std::make_shared<detail::ChunkRuntimePageStore>(
            detail::ChunkRuntimeOptions{.tree = 42, .owner_epoch = 7, .iu_size = 1, .mirrors = 2}, next,
            fixture.transport, fixture.credits.credits.bytes);
        config.page_store    = store.get();
        config.backend_owner = store;
        const auto reopened  = Crowdbtree::open(config, &tree);
        ASSERT_TRUE(reopened.ok()) << reopened.to_string();
        ASSERT_TRUE(tree->get("key20", &slot, &value));
        EXPECT_EQ(slot, 20U);
        EXPECT_EQ(value, std::string(8192, 'u'));
        EXPECT_EQ(store->durable_wal_offset(), 1234U);
    }
    close_queue(fixture);
}

TEST(LegacyChunkMigration, OrdinaryOpenRejectsOldFormatWithoutChangingItsRoot)
{
    test_chunk::TestLegacyChunk fixture;
    {
        auto                        config   = configuration(fixture);
        const auto                  original = fixture.catalog->committed_manifest;
        std::unique_ptr<Crowdbtree> tree;
        const auto                  status = Crowdbtree::open(config, &tree);
        EXPECT_EQ(status.code(), Code::kNotSupported) << status.to_string();
        EXPECT_FALSE(tree);
        EXPECT_EQ(fixture.catalog->committed_manifest, original);
        EXPECT_EQ(fixture.catalog->current_generation, 1U);
        EXPECT_TRUE(fixture.catalog->generation_pins.empty());
        EXPECT_TRUE(fixture.catalog->migrations.empty());
    }
    close_queue(fixture);
}

TEST(LegacyChunkMigration, RejectedCommitKeepsOriginalRootRecoverableAndReleasesItsAttemptPin)
{
    test_chunk::TestLegacyChunk fixture;
    {
        auto       config            = configuration(fixture);
        const auto original          = fixture.catalog->committed_manifest;
        fixture.catalog->fail_commit = true;
        std::unique_ptr<Crowdbtree> tree;
        const auto                  status = Crowdbtree::migrate_legacy_chunk(config, 1, &tree);
        EXPECT_EQ(status.code(), Code::kUnavailable) << status.to_string();
        EXPECT_FALSE(tree);
        EXPECT_EQ(fixture.catalog->committed_manifest, original);
        EXPECT_EQ(fixture.catalog->current_generation, 1U);
        EXPECT_TRUE(fixture.catalog->generation_pins.empty());
        EXPECT_EQ(fixture.catalog->migrations.size(), 1U);
        auto reader = fixture.reader();
        ASSERT_TRUE(reader->open(1).ok());
        uint8_t byte = 0;
        EXPECT_TRUE(reader->read_at(fixture.layout.pack(0).logical_offset, &byte, 1).ok());
        EXPECT_TRUE(reader->release_pin().ok());
    }
    close_queue(fixture);
}

TEST(LegacyChunkMigration, UnknownCommitRetainsOldPinAndRetryResolvesDurableReceipt)
{
    test_chunk::TestLegacyChunk fixture;
    {
        auto config                          = configuration(fixture);
        fixture.catalog->lose_commit_outcome = true;
        std::unique_ptr<Crowdbtree> tree;
        const auto                  status = Crowdbtree::migrate_legacy_chunk(config, 1, &tree);
        EXPECT_EQ(status.code(), Code::kUnavailable) << status.to_string();
        EXPECT_FALSE(tree);
        EXPECT_EQ(fixture.catalog->current_generation, 2U);
        EXPECT_EQ(fixture.catalog->generation_pins.size(), 1U);
        EXPECT_EQ(fixture.catalog->migrations.size(), 1U);
        (void)fixture.catalog->queue->pump(256);
        fixture.catalog            = fixture.catalog->next_legacy_owner();
        fixture.transport->catalog = fixture.catalog;
        auto store                 = std::make_shared<detail::ChunkRuntimePageStore>(
            detail::ChunkRuntimeOptions{.tree = 42, .owner_epoch = 7, .iu_size = 1, .mirrors = 2}, fixture.catalog,
            fixture.transport, fixture.credits.credits.bytes);
        config.page_store    = store.get();
        config.backend_owner = store;
        const auto retried   = Crowdbtree::migrate_legacy_chunk(config, 1, &tree);
        ASSERT_TRUE(retried.ok()) << retried.to_string();
        EXPECT_TRUE(fixture.catalog->generation_pins.empty());
        EXPECT_EQ(fixture.catalog->current_generation, 2U);
        EXPECT_EQ(fixture.catalog->commit_calls, 0U);
        std::string value;
        uint64_t    slot = 0;
        ASSERT_TRUE(tree->get("key20", &slot, &value));
        EXPECT_EQ(value, std::string(8192, 'u'));
        EXPECT_EQ(slot, 20U);
    }
    close_queue(fixture);
}

TEST(LegacyChunkMigration, BoundedChildCopiesOnlyItsLogicalRangeFromSharedLegacyFrames)
{
    test_chunk::TestLegacyChunk fixture;
    {
        auto config      = configuration(fixture);
        config.key_range = KeyRange::bounded("key20", "key3");
        std::unique_ptr<Crowdbtree> tree;
        const auto                  status = Crowdbtree::migrate_legacy_chunk(config, 1, &tree);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(tree->tree_state_stats().logical.kv_known_count, 1U);
        std::string value;
        uint64_t    slot = 0;
        EXPECT_FALSE(tree->get("key2", &slot, &value));
        EXPECT_FALSE(tree->get("key3", &slot, &value));
        ASSERT_TRUE(tree->get("key20", &slot, &value));
        EXPECT_EQ(value, std::string(8192, 'u'));
        EXPECT_EQ(slot, 20U);
    }
    close_queue(fixture);
}

TEST(LegacyChunkMigration, PublicCEntryUsesInjectedRuntimeBackendAndPreservesTheRecoveredValue)
{
    test_chunk::TestLegacyChunk fixture;
    ASSERT_TRUE(detail::ChunkPinReleaseQueue::create(
                    fixture.credits.credits.bytes, 256, std::make_shared<int>(1), [](void *) noexcept {}, nullptr,
                    &fixture.catalog->queue)
                    .ok());
    ct_runtime *raw_runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &raw_runtime), 0);
    const std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(raw_runtime, &ct_runtime_free);
    auto                                                          store = std::make_unique<ct_page_store>();
    store->bundle                                                       = std::make_shared<PageStoreBundle>();
    store->bundle->store = std::make_unique<detail::ChunkRuntimePageStore>(
        detail::ChunkRuntimeOptions{.tree = 42, .owner_epoch = 7, .iu_size = 1, .mirrors = 2}, fixture.catalog,
        fixture.transport, fixture.credits.credits.bytes);
    ct_options options{};
    options.page_store       = store.get();
    options.frame_bytes      = 4096;
    options.max_inline_value = 64;
    ct_tree   *raw_tree      = nullptr;
    const auto status        = ct_migrate_legacy_chunk_with_runtime(&options, runtime.get(), nullptr, 1, &raw_tree);
    ASSERT_EQ(status, 0);
    {
        const std::unique_ptr<ct_tree, decltype(&ct_close)> tree(raw_tree, &ct_close);
        uint64_t                                            slot = 0;
        ct_buf                                              value{};
        int32_t                                             found = 0;
        EXPECT_EQ(ct_get(tree.get(), reinterpret_cast<const uint8_t *>("key20"), 5, &found, &slot, &value), 0);
        EXPECT_EQ(found, 1);
        EXPECT_EQ(slot, 20U);
        EXPECT_EQ(std::string(reinterpret_cast<const char *>(value.data), value.len), std::string(8192, 'u'));
        ct_free_buf(&value);
    }
    close_queue(fixture);
}

} // namespace crowdb::tree
