// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/portable_import.h"
#include "recovered_state_test_support.h"
#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
namespace
{
TEST(PortableImport, SortedCellsBuildABoundedBalancedReadableGraphWithOverflow)
{
    test_read::TestRecoveredGraph test;
    Config                        config;
    config.frame_bytes      = 512;
    config.max_inline_value = 16;
    config.inner_max_keys   = 2;
    PortableImportBuilder builder(config, {}, 300, test.credits.credits.reservation());
    for (size_t i = 0; i != 200; ++i) {
        const auto key    = std::string(3 - std::to_string(i).size(), '0') + std::to_string(i);
        const auto cell   = encode_cell_buf(i + 1, OpKind::kPut, std::string(1000, static_cast<char>('a' + (i % 26))));
        const auto status = builder.append(key, Slice(cell));
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    std::shared_ptr<ReadablePageCatalog> catalog;
    auto                                 status = builder.finish(&catalog);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(catalog, nullptr);
    EXPECT_GT(catalog->frames.size(), 200U);
    ASSERT_TRUE(RecoveredStateOperation::create_readable(catalog, test.loader, test.runtime, test.lane, {},
                                                         test.credits.credits.reservation(), &test.operation)
                    .ok());
    status = detail::wait_snapshot_operation(test.operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(test.operation->result().state->stats.kv_known_count, 200U);
    EXPECT_GT(test.operation->result().state->stats.height, 2U);
    EXPECT_EQ(test.operation->result().state->coverage, 300U);
    EXPECT_EQ(test.store->reads.load(), 0U);
}

TEST(PortableImport, FailurePoisonsStagingAndDoesNotReturnAPartialCatalog)
{
    test_read::LoadCredits credits;
    Config                 config;
    PortableImportBuilder  builder(config, {}, 7, credits.credits.reservation());
    auto                   cell = encode_cell_buf(7, OpKind::kPut, "value");
    ASSERT_TRUE(builder.append("z", Slice(cell)).ok());
    EXPECT_EQ(builder.append("a", Slice(cell)).code(), Code::kInvalidArgument);
    EXPECT_EQ(builder.append("zz", Slice(cell)).code(), Code::kInvalidArgument);
    std::shared_ptr<ReadablePageCatalog> catalog;
    EXPECT_EQ(builder.finish(&catalog).code(), Code::kInvalidArgument);
    EXPECT_FALSE(catalog);
}

TEST(PortableImport, EmptyKeyAndTombstonePreserveSlotsAndEmptyImportPreservesCoverage)
{
    test_read::LoadCredits credits;
    PortableImportBuilder  builder({}, {}, 17, credits.credits.reservation());
    const auto             empty   = encode_cell_buf(5, OpKind::kPut, "empty");
    const auto             deleted = encode_cell_buf(9, OpKind::kDelete);
    ASSERT_TRUE(builder.append("", Slice(empty)).ok());
    ASSERT_TRUE(builder.append("removed", Slice(deleted)).ok());
    std::shared_ptr<ReadablePageCatalog> catalog;
    ASSERT_TRUE(builder.finish(&catalog).ok());
    const auto         &raw = catalog->frames.at(catalog->root_page_id).backing->resident_frame;
    const LeafFrameView leaf(raw.data(), raw.size());
    ASSERT_EQ(leaf.count(), 2U);
    EXPECT_TRUE(leaf.key(0).empty());
    EXPECT_EQ(CellView(leaf.cell(0)).slot(), 5U);
    EXPECT_TRUE(CellView(leaf.cell(1)).is_tombstone());
    EXPECT_EQ(CellView(leaf.cell(1)).slot(), 9U);
    PortableImportBuilder empty_builder({}, {}, 23, credits.credits.reservation());
    catalog.reset();
    ASSERT_TRUE(empty_builder.finish(&catalog).ok());
    EXPECT_TRUE(catalog->frames.empty());
    EXPECT_EQ(catalog->coverage, 23U);
}
} // namespace
} // namespace crowdb::tree
