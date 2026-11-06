// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_maintenance.h"

#include <gtest/gtest.h>

using namespace crowdb::tree;

namespace
{
std::shared_ptr<const CellVersion> cell(uint64_t slot, Slice value, OpKind kind = OpKind::kPut)
{
    return std::make_shared<CellVersion>(encode_cell_buf(slot, kind, value), slot,
                                         kind == OpKind::kDelete ? kFlagTombstone : 0);
}

std::shared_ptr<const OverlayBatch>
run(uint64_t id, std::initializer_list<std::pair<std::string, std::shared_ptr<const CellVersion>>> entries)
{
    OverlayBuilder builder({});
    for (const auto &[key, owner] : entries) {
        EXPECT_TRUE(builder.append(Slice(key), owner).ok());
    }
    std::shared_ptr<const OverlayBatch> output;
    EXPECT_TRUE(builder.finish(id, &output).ok());
    return output;
}

std::shared_ptr<const BackingRef> cold_backing()
{
    auto backing    = std::make_shared<BackingRef>();
    backing->length = 4096;
    backing->owner  = std::make_shared<int>(1);
    return backing;
}

PageMaintenanceLimits small_layout()
{
    PageMaintenanceLimits limits;
    limits.primary_page_bytes  = 256;
    limits.target_page_bytes   = 220;
    limits.merge_trigger_bytes = 190;
    limits.merge_target_bytes  = 210;
    limits.inline_value_bytes  = 64;
    return limits;
}

std::shared_ptr<PageState> dense_page()
{
    auto page              = std::make_shared<PageState>();
    page->route_generation = 1;
    page->content_id       = 1;
    page->right_sibling    = 99;
    page->overlays.push_back(run(1, {
                                        {"a", cell(1, std::string(30, 'a'))},
                                        {"b", cell(2, std::string(30, 'b'))},
                                        {"c", cell(3, std::string(30, 'c'))},
                                        {"d", cell(4, std::string(30, 'd'))}
    }));
    return page;
}
} // namespace

TEST(PageMaintenance, ColdOverlayCoalescePreservesTombstoneAndDoesNotLoadBase)
{
    auto page        = std::make_shared<PageState>();
    page->content_id = 9;
    page->bases.push_back({.identity = 1, .max_slot = 2, .backing = cold_backing()});
    page->overlays.push_back(run(1, {
                                        {"key", cell(2, "old")}
    }));
    const auto tombstone = cell(3, {}, OpKind::kDelete);
    page->overlays.push_back(run(2, {
                                        {"key", tombstone}
    }));
    PageMaintenanceLimits limits;
    limits.gc_floor = 3;
    auto result     = coalesce_overlays(page, limits, 7, 100);
    ASSERT_TRUE(result.status.ok());
    ASSERT_NE(result.page, nullptr);
    EXPECT_EQ(result.readiness, MaintenanceReadiness::kReady);
    EXPECT_TRUE(result.loads.empty());
    ASSERT_EQ(result.page->overlays.size(), 1U);
    EXPECT_EQ(result.page->overlays[0]->find("key")->owner, tombstone);
    EXPECT_EQ(result.page->lookup("key").state, LogicalLookupState::kTombstone);
    EXPECT_EQ(result.page->bases[0].backing, page->bases[0].backing);
    EXPECT_EQ(result.page->content_id, 9U);
    EXPECT_EQ(result.page->representation_id, 7U);
    EXPECT_EQ(page->overlays.size(), 2U);
    EXPECT_EQ(result.removed_primary_bytes, 0U);
}

TEST(PageMaintenance, OverlayOnlyGcKeepsNewerDeletesAndPreservesCapturedPages)
{
    auto page = std::make_shared<PageState>();
    page->overlays.push_back(run(1, {
                                        {"a", cell(1, "old")},
                                        {"b", cell(2, "old")}
    }));
    page->overlays.push_back(run(2, {
                                        {"a", cell(3, {}, OpKind::kDelete)},
                                        {"b", cell(4, {}, OpKind::kDelete)}
    }));
    PageMaintenanceLimits limits;
    limits.gc_floor = 3;
    auto result     = coalesce_overlays(page, limits, 7, 100);
    ASSERT_TRUE(result.status.ok());
    ASSERT_NE(result.page, nullptr);
    EXPECT_EQ(result.page->lookup("a").state, LogicalLookupState::kMissing);
    EXPECT_EQ(result.page->lookup("b").state, LogicalLookupState::kTombstone);
    EXPECT_EQ(page->lookup("a").state, LogicalLookupState::kTombstone);
    EXPECT_EQ(page->overlays.front()->find("a")->value().to_string(), "old");
    EXPECT_EQ(result.removed_primary_bytes, kCellHeaderSize + kLeafSlotSize + 1);
}

TEST(PageMaintenance, CoalesceKeepsStableTiesAndProducesBoundedRuns)
{
    auto       page  = std::make_shared<PageState>();
    const auto first = cell(7, "first");
    page->overlays.push_back(run(1, {
                                        {"a", first},
                                        {"c", cell(3, "c")}
    }));
    page->overlays.push_back(run(2, {
                                        {"a", cell(7, "second")},
                                        {"b", cell(2, "b")     }
    }));
    PageMaintenanceLimits limits;
    limits.run.max_entries = 1;
    auto result            = coalesce_overlays(page, limits, 10, 100);
    ASSERT_TRUE(result.status.ok());
    ASSERT_EQ(result.page->overlays.size(), 3U);
    EXPECT_EQ(result.page->overlays[0]->entries()[0].owner, first);
    EXPECT_EQ(result.page->overlays[0]->id(), 100U);
    EXPECT_EQ(result.page->overlays[2]->id(), 102U);
    EXPECT_EQ(result.page->lookup("b").cell.value.to_string(), "b");
}

TEST(PageMaintenance, CoalesceBudgetFailureDoesNotPublishPartialOutput)
{
    auto                  page = dense_page();
    PageMaintenanceLimits limits;
    limits.max_work_entries = 2;
    auto result             = coalesce_overlays(page, limits, 2, 10);
    EXPECT_EQ(result.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(result.readiness, MaintenanceReadiness::kDeferred);
    EXPECT_EQ(result.page, nullptr);
    EXPECT_EQ(page->overlays[0]->entries().size(), 4U);
}

TEST(PageMaintenance, ColdLayoutRequestsOneBaseWithoutInventingPageSize)
{
    auto page = dense_page();
    page->bases.push_back({.identity = 1, .backing = cold_backing()});
    page->bases.push_back({.identity = 2, .backing = cold_backing()});
    auto layout = plan_leaf_layout(page, small_layout());
    EXPECT_TRUE(layout.status.ok());
    EXPECT_EQ(layout.readiness, MaintenanceReadiness::kNeedsBase);
    ASSERT_EQ(layout.loads.size(), 1U);
    EXPECT_EQ(layout.loads[0].base_index, 0U);
    EXPECT_TRUE(layout.primary_bytes.empty());
    EXPECT_TRUE(layout.separators.empty());
}

TEST(PageMaintenance, ByteLayoutSplitsManyWaysWithoutCopyingValues)
{
    auto       page   = dense_page();
    const auto limits = small_layout();
    auto       layout = plan_leaf_layout(page, limits);
    ASSERT_TRUE(layout.status.ok());
    ASSERT_EQ(layout.readiness, MaintenanceReadiness::kReady);
    EXPECT_TRUE(std::ranges::equal(layout.separators, std::array{"b", "c", "d"},
                                   [](const auto &actual, const char *expected) { return actual == expected; }));
    ASSERT_EQ(layout.primary_bytes.size(), 4U);
    std::vector<SplitTarget> targets;
    targets.reserve(layout.primary_bytes.size());
    for (size_t i = 0; i < layout.primary_bytes.size(); ++i) {
        EXPECT_LE(layout.primary_bytes[i], limits.primary_page_bytes);
        targets.push_back({
            .page_id = 10 + i, .version = {.route_generation = 2, .content_id = 10 + i, .representation_id = 10 + i}
        });
    }
    auto split = split_leaf_ranges(layout, targets, limits);
    ASSERT_TRUE(split.status.ok());
    ASSERT_EQ(split.pages.size(), 4U);
    for (size_t i = 0; i < split.pages.size(); ++i) {
        const std::string key(1, static_cast<char>('a' + i));
        auto              before = page->lookup(Slice(key));
        auto              after  = split.pages[i]->lookup(Slice(key));
        EXPECT_EQ(before.cell.owner, after.cell.owner);
        EXPECT_EQ(before.cell.value.data(), after.cell.value.data());
        EXPECT_EQ(split.pages[i]->right_sibling, i == 3 ? 99U : 11 + i);
        for (size_t j = 0; j < split.pages.size(); ++j) {
            if (j != i) {
                EXPECT_EQ(split.pages[j]->lookup(Slice(key)).state, LogicalLookupState::kMissing);
            }
        }
    }
    EXPECT_EQ(page->right_sibling, 99U);
    EXPECT_EQ(page->overlays[0]->entries().size(), 4U);
}

TEST(PageMaintenance, SplitSharesResidentBaseAndClipsEveryView)
{
    auto page              = std::make_shared<PageState>();
    page->route_generation = 1;
    std::vector<leaf_entry> entries;
    for (const auto *key : {"a", "b", "c", "d"}) {
        entries.push_back({.key = key, .cell = encode_cell_buf(1, OpKind::kPut, std::string(30, 'v'))});
    }
    auto base = std::shared_ptr<const LeafBase>(LeafBase::build(entries));
    page->bases.push_back({.identity = 42, .resident = base});
    auto layout = plan_leaf_layout(page, small_layout());
    ASSERT_TRUE(layout.status.ok());
    std::vector<SplitTarget> targets;
    targets.reserve(layout.primary_bytes.size());
    for (size_t i = 0; i < layout.primary_bytes.size(); ++i) {
        targets.push_back({
            .page_id = i + 1, .version = {.route_generation = 2, .content_id = i + 1, .representation_id = i + 1}
        });
    }
    auto split = split_leaf_ranges(layout, targets, small_layout());
    ASSERT_TRUE(split.status.ok());
    ASSERT_EQ(split.pages.size(), 4U);
    for (size_t i = 0; i < split.pages.size(); ++i) {
        ASSERT_EQ(split.pages[i]->bases.size(), 1U);
        EXPECT_EQ(split.pages[i]->bases[0].resident, base);
        EXPECT_EQ(split.pages[i]->bases[0].identity, 42U);
        LogicalPageCursor cursor(split.pages[i], {}, UINT64_MAX, 1);
        ASSERT_TRUE(cursor.valid());
        EXPECT_EQ(cursor.key().to_string(), std::string(1, static_cast<char>('a' + i)));
        cursor.next();
        EXPECT_FALSE(cursor.valid());
    }
}

TEST(PageMaintenance, LargeValuePlansOverflowAndExceptionalKeyRemainsOnePage)
{
    auto page = std::make_shared<PageState>();
    page->overlays.push_back(run(1, {
                                        {"a", cell(1, std::string(100000, 'v'))}
    }));
    auto layout = plan_leaf_layout(page, small_layout());
    ASSERT_TRUE(layout.status.ok());
    EXPECT_EQ(layout.overflow_values, 1U);
    EXPECT_EQ(layout.primary_bytes.size(), 1U);
    EXPECT_EQ(layout.total_primary_bytes, kFrameHeaderSize + kFrameTrailerSize + kLeafSlotSize + 1 + kOverflowCellSize);
    page->overlays = {
        run(2, {{std::string(200, 'z'), cell(2, "x")}}
         )
    };
    layout = plan_leaf_layout(page, small_layout());
    ASSERT_TRUE(layout.status.ok()) << layout.status.to_string();
    ASSERT_EQ(layout.primary_bytes.size(), 1U);
    EXPECT_GT(layout.primary_bytes.front(), small_layout().primary_page_bytes);
    EXPECT_LE(layout.primary_bytes.front(), kMaxPhysicalPageBytes);
    EXPECT_TRUE(layout.separators.empty());
}

TEST(PageMaintenance, SplitTargetBudgetDoesNotRejectOnePageThatAlreadyFits)
{
    auto page = std::make_shared<PageState>();
    page->overlays.push_back(run(1, {
                                        {"a", cell(1, std::string(30, 'a'))},
                                        {"b", cell(2, std::string(30, 'b'))}
    }));
    auto limits             = small_layout();
    limits.max_output_pages = 1;
    auto layout             = plan_leaf_layout(page, limits);
    ASSERT_TRUE(layout.status.ok());
    EXPECT_TRUE(std::ranges::equal(layout.primary_bytes, std::array<size_t, 1>{240}));
    EXPECT_TRUE(layout.separators.empty());
    layout = plan_leaf_layout(dense_page(), limits);
    EXPECT_EQ(layout.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(layout.readiness, MaintenanceReadiness::kDeferred);
    EXPECT_TRUE(layout.primary_bytes.empty());
}

TEST(PageMaintenance, MergeRequiresSameParentAdjacentRangesAndHysteresis)
{
    auto left            = std::make_shared<PageState>();
    auto right           = std::make_shared<PageState>();
    left->range          = {.upper = "b"};
    right->range         = {.lower = "b"};
    left->right_sibling  = 2;
    right->right_sibling = 8;
    left->overlays       = {
        run(1, {{"a", cell(1, "x")}}
         )
    };
    right->overlays = {
        run(2, {{"b", cell(2, "y")}}
         )
    };
    LeafMergeInput input{
        .left = left, .right = right, .left_page_id = 1, .right_page_id = 2, .left_parent = 5, .right_parent = 5};
    auto merged =
        plan_leaf_merge(input, small_layout(), {.route_generation = 1, .content_id = 3, .representation_id = 3}, 100);
    ASSERT_TRUE(merged.status.ok());
    ASSERT_NE(merged.page, nullptr);
    EXPECT_EQ(merged.page->lookup("a").cell.value.to_string(), "x");
    EXPECT_EQ(merged.page->lookup("b").cell.value.to_string(), "y");
    EXPECT_EQ(merged.page->right_sibling, 8U);
    input.right_parent = 6;
    EXPECT_EQ(plan_leaf_merge(input, small_layout(), {1, 3, 3}, 100).status.code(), Code::kInvalidArgument);
    input.right_parent  = 5;
    left->right_sibling = 3;
    EXPECT_EQ(plan_leaf_merge(input, small_layout(), {1, 3, 3}, 100).status.code(), Code::kInvalidArgument);
}

TEST(PageMaintenance, MergeClipsHiddenBaseKeysBeforeCombiningRanges)
{
    std::vector<leaf_entry> entries;
    entries.push_back({.key = "a", .cell = encode_cell_buf(1, OpKind::kPut, "left")});
    entries.push_back({.key = "b", .cell = encode_cell_buf(99, OpKind::kPut, "hidden")});
    auto base           = std::shared_ptr<const LeafBase>(LeafBase::build(entries));
    auto left           = std::make_shared<PageState>();
    left->range         = {.upper = "b"};
    left->right_sibling = 2;
    left->bases.push_back({.identity = 1, .resident = base});
    auto right   = std::make_shared<PageState>();
    right->range = {.lower = "b"};
    right->overlays.push_back(run(2, {
                                         {"b", cell(2, "visible")}
    }));
    auto merged = plan_leaf_merge(
        {.left = left, .right = right, .left_page_id = 1, .right_page_id = 2, .left_parent = 5, .right_parent = 5},
        small_layout(), {.route_generation = 1, .content_id = 3, .representation_id = 3}, 100);
    ASSERT_TRUE(merged.status.ok());
    ASSERT_NE(merged.page, nullptr);
    EXPECT_EQ(merged.page->lookup("b").cell.value.to_string(), "visible");
}

TEST(PageMaintenance, PartialSourceWindowPreservesTombstonesMaskingUntouchedRuns)
{
    auto page = std::make_shared<PageState>();
    page->overlays.push_back(run(1, {
                                        {"key", cell(1, "older")}
    }));
    page->overlays.push_back(run(2, {
                                        {"key", cell(3, {}, OpKind::kDelete)}
    }));
    page->overlays.push_back(run(3, {
                                        {"other", cell(2, "value")}
    }));
    PageMaintenanceLimits limits;
    limits.max_sources = 2;
    limits.gc_floor    = 3;
    auto partial       = coalesce_overlays(page, limits, 2, 4);
    ASSERT_TRUE(partial.status.ok()) << partial.status.to_string();
    ASSERT_TRUE(partial.page);
    EXPECT_EQ(partial.page->overlays.size(), 2U);
    EXPECT_EQ(partial.page->lookup("key").state, LogicalLookupState::kTombstone);
    EXPECT_EQ(partial.page->lookup("other").cell.value.to_string(), "value");
    auto complete = coalesce_overlays(partial.page, limits, 3, 5);
    ASSERT_TRUE(complete.status.ok()) << complete.status.to_string();
    EXPECT_EQ(complete.page->lookup("key").state, LogicalLookupState::kMissing);
    EXPECT_EQ(complete.page->lookup("other").cell.value.to_string(), "value");
}
