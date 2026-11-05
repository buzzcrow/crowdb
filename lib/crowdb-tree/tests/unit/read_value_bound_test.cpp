// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/overflow_backing.h"
#include "btree/read_value_bound.h"
#include "published_read_test_support.h"
#include "snapshot/primary_backing.h"

namespace crowdb::tree
{
TEST(ReadValueBound, OnlyExactMatchingDependencyProofsRemoveColdBaseAllowance)
{
    auto base    = test_read::cold(7);
    auto backing = std::make_shared<BackingRef>(*base.backing);
    base.backing = backing;
    auto leaf    = std::make_shared<PageState>();
    leaf->bases.push_back(base);
    auto page                      = test_read::published(leaf);
    auto primary                   = std::make_shared<PrimaryBackingMetadata>();
    page->primary                  = primary;
    auto refs                      = std::make_shared<StoredOverflowReferences>();
    primary->frame.stored_overflow = refs;
    auto bound                     = [&] {
        auto                                 captured = std::make_shared<PublishedTreeState>(*test_read::view(leaf));
        MappingView::CopyStats               copied;
        const std::array<PageReplacement, 1> changes{
            {{.page_id = 0, .expected = captured->mapping.get(0), .replacement = page}}};
        captured->mapping                = captured->mapping.replace(changes, copied);
        captured->stats.kv_unknown_pages = 1;
        return overflow_value_bytes_bound(*captured, 65536);
    };
    EXPECT_EQ(bound(), 65536U);
    primary->frame.backing = backing;
    EXPECT_EQ(bound(), 0U);
    refs->push_back({.head = 17, .bytes = 65536});
    EXPECT_EQ(bound(), 65536U);
    primary->frame.stored_overflow.reset();
    EXPECT_EQ(bound(), 65536U);
    auto dependencies = std::make_shared<OverflowBackingSet>(nullptr);
    backing->overflow = dependencies;
    EXPECT_EQ(bound(), 0U);
    dependencies->pages.emplace(17, OverflowBackingPage{.backing = backing});
    EXPECT_EQ(bound(), 65536U);
    // Avoid an artificial self-owned dependency in this metadata-only fixture.
    dependencies->pages.clear();
}

TEST(ReadValueBound, UnknownLiveCountsDoNotRequireOverflowStorageForBorrowedCells)
{
    auto captured                    = std::make_shared<PublishedTreeState>(*test_read::view(test_read::page({
        {.key = "a", .value = "borrowed"}
    })));
    captured->stats.kv_unknown_pages = 1;
    EXPECT_FALSE(captured->mapping.copy_bounds().may_have_overflow_values);
    EXPECT_EQ(overflow_value_bytes_bound(*captured, 65536), 0U);
}

TEST(ReadValueBound, EncodedOverlayAndBaseInputsRetainTheirOverflowAllowance)
{
    auto           logical = std::make_shared<PageState>();
    OverlayBuilder builder({});
    auto           owner = std::make_shared<CellVersion>(encode_overflow_cell_buf(2, 41, 16384), 2, kFlagOverflow);
    ASSERT_TRUE(builder.append(Slice("a"), std::move(owner)).ok());
    std::shared_ptr<const OverlayBatch> run;
    ASSERT_TRUE(builder.finish(1, &run).ok());
    ASSERT_TRUE(run->has_overflow_values());
    logical->overlays.push_back(std::move(run));
    auto captured                    = std::make_shared<PublishedTreeState>(*test_read::view(logical));
    captured->stats.kv_unknown_pages = 1;
    EXPECT_TRUE(captured->mapping.copy_bounds().may_have_overflow_values);
    EXPECT_EQ(overflow_value_bytes_bound(*captured, 65536), 65536U);
    captured->stats.kv_unknown_pages = 0;
    captured->stats.kv_known_bytes   = 16385;
    EXPECT_EQ(overflow_value_bytes_bound(*captured, 65536), 16385U);
    auto cold = std::make_shared<PageState>();
    cold->bases.push_back(test_read::cold(7));
    auto                                 replacement = test_read::published(cold);
    const std::array<PageReplacement, 1> changes{
        {{.page_id = 0, .expected = captured->mapping.get(0), .replacement = replacement}}};
    MappingView::CopyStats copied;
    captured->mapping = captured->mapping.replace(changes, copied);
    EXPECT_TRUE(captured->mapping.copy_bounds().may_have_overflow_values);
    const std::array<PageReplacement, 1> clear{
        {{.page_id = 0, .expected = replacement, .replacement = test_read::published(test_read::page({}))}}};
    captured->mapping = captured->mapping.replace(clear, copied);
    EXPECT_FALSE(captured->mapping.copy_bounds().may_have_overflow_values);
    EXPECT_EQ(overflow_value_bytes_bound(*captured, 65536), 0U);
}
} // namespace crowdb::tree
