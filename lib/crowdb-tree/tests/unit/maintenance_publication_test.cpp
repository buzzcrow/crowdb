// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_publication.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <future>
#include <string>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<InstallRecord> maintenance_record(uint64_t sequence)
{
    return std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 9, .range = 0, .sequence = sequence});
}

PublicationResult publish_maintenance(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> completed;
    auto                            result = completed.get_future();
    request->on_done                       = [&](PublicationResult value) { completed.set_value(std::move(value)); };
    if (!publisher.try_submit(request)) {
        ADD_FAILURE() << "publication rejected by ingress";
        return {.status = Status::unavailable()};
    }
    return result.get();
}

std::shared_ptr<PublishedPage> maintenance_leaf(std::vector<std::string> keys, size_t value_bytes,
                                                PageKeyRange range = {}, uint64_t sibling = kInvalidPageId)
{
    auto logical               = std::make_shared<PageState>();
    logical->range             = std::move(range);
    logical->right_sibling     = sibling;
    logical->route_generation  = 1;
    logical->content_id        = 7;
    logical->representation_id = 4;
    OverlayBuilder builder({});
    for (const auto &key : keys) {
        auto cell =
            std::make_shared<CellVersion>(encode_cell_buf(1, OpKind::kPut, std::string(value_bytes, 'v')), 1, 0);
        EXPECT_TRUE(builder.append(Slice(key), std::move(cell)).ok());
    }
    std::shared_ptr<const OverlayBatch> run;
    EXPECT_TRUE(builder.finish(1, &run).ok());
    logical->overlays.push_back(std::move(run));
    auto page                     = std::make_shared<PublishedPage>();
    page->revision                = 1;
    page->leaf                    = std::move(logical);
    page->metrics.dirty           = true;
    page->metrics.overlay_runs    = 1;
    page->metrics.overlay_entries = keys.size();
    return page;
}

std::shared_ptr<PublishedPage> maintenance_inner(std::vector<uint64_t> children, std::vector<std::string> separators,
                                                 uint32_t level = 1)
{
    auto inner = std::make_shared<InnerPageState>();
    inner->children.assign(children.begin(), children.end());
    inner->separators.assign(separators.begin(), separators.end());
    auto page          = std::make_shared<PublishedPage>();
    page->revision     = 1;
    page->level        = level;
    page->inner        = std::move(inner);
    page->metrics.leaf = false;
    return page;
}

MaintenancePublicationOptions maintenance_limits()
{
    MaintenancePublicationOptions options;
    options.pages.primary_page_bytes  = 256;
    options.pages.target_page_bytes   = 220;
    options.pages.merge_trigger_bytes = 190;
    options.pages.merge_target_bytes  = 210;
    options.pages.inline_value_bytes  = 64;
    options.max_inner_children        = 2;
    return options;
}

std::shared_ptr<const PublishedPage> route(const PublishedTreeState &view, const std::string &key)
{
    auto page = view.mapping.get(view.root_page_id);
    for (size_t depth = 0; depth < 64 && page && page->inner; ++depth) {
        EXPECT_EQ(page->inner->children.size(), page->inner->separators.size() + 1);
        const auto index =
            static_cast<size_t>(std::upper_bound(page->inner->separators.begin(), page->inner->separators.end(), key) -
                                page->inner->separators.begin());
        const auto next = view.mapping.get(page->inner->children[index]);
        EXPECT_NE(next, nullptr);
        if (!next) {
            return {};
        }
        EXPECT_EQ(next->level + 1, page->level);
        page = next;
    }
    return page;
}

void seed_leaf(PublicationCoordinator &publisher, const std::shared_ptr<PublishedPage> &page)
{
    EXPECT_TRUE(publish_maintenance(publisher, {.record       = maintenance_record(0),
                                                .pages        = {{.page_id = 0, .expected = {}, .replacement = page}},
                                                .root_page_id = 0})
                    .status.ok());
}

TEST(MaintenancePublication, MultiwayRootSplitPublishesAllLevelsAndKeepsOldView)
{
    PublicationCoordinator publisher;
    auto                   original = maintenance_leaf({"a", "b", "c", "d"}, 30);
    seed_leaf(publisher, original);
    const auto before = publisher.capture();
    auto       plan   = build_split_publication(before, 0, maintenance_record(1), maintenance_limits());
    ASSERT_TRUE(plan.status.ok()) << plan.status.message();
    ASSERT_TRUE(plan.changed());
    EXPECT_TRUE(std::ranges::equal(plan.output_leaf_pids, std::array<uint64_t, 4>{0, 1, 2, 3}));
    ASSERT_TRUE(publish_maintenance(publisher, std::move(plan.transaction)).status.ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->stats.leaf_pages, 4U);
    EXPECT_EQ(after->stats.inner_pages, 3U);
    EXPECT_EQ(after->stats.height, 3U);
    EXPECT_EQ(after->stats.dirty_pages, 7U);
    EXPECT_EQ(after->stats.writeback_candidate_pages, 7U);
    EXPECT_EQ(after->stats.checkpoint_pending_pages, 7U);
    EXPECT_TRUE(after->stats.checkpoint_mapping_pending);
    EXPECT_EQ(after->stats.page_size_unknown_pages, 0U);
    EXPECT_EQ(after->stats.kv_known_count, 4U);
    EXPECT_EQ(after->stats.kv_known_bytes, 124U);
    for (size_t i = 0; i < 4; ++i) {
        const std::string key(1, static_cast<char>('a' + i));
        const auto        leaf = route(*after, key);
        ASSERT_TRUE(leaf && leaf->leaf);
        EXPECT_EQ(leaf->leaf->lookup(Slice(key)).cell.owner, original->leaf->lookup(Slice(key)).cell.owner);
        EXPECT_EQ(after->mapping.get(i)->leaf->right_sibling, i == 3 ? kInvalidPageId : i + 1);
    }
    publisher.reclaim();
    EXPECT_EQ(before->mapping.get(0), original);
    EXPECT_EQ(before->stats.height, 1U);
    EXPECT_EQ(before->mapping.get(0)->leaf->lookup("d").state, LogicalLookupState::kValue);
}

TEST(MaintenancePublication, SplittingExistingChildRecursivelySplitsParentAndPreservesRightNeighbor)
{
    PublicationCoordinator publisher;
    auto                   left   = maintenance_leaf({"a", "b", "c", "d"}, 30, {.upper = "m"}, 1);
    auto                   right  = maintenance_leaf({"m", "n"}, 30, {.lower = "m"});
    auto                   parent = maintenance_inner({0, 1}, {"m"});
    ASSERT_TRUE(publish_maintenance(publisher,
                                    {
                                        .record       = maintenance_record(0),
                                        .pages        = {{.page_id = 0, .expected = {}, .replacement = left},
                                                         {.page_id = 1, .expected = {}, .replacement = right},
                                                         {.page_id = 2, .expected = {}, .replacement = parent}},
                                        .root_page_id = 2
    })
                    .status.ok());
    const auto before = publisher.capture();
    auto       plan   = build_split_publication(before, 0, maintenance_record(1), maintenance_limits());
    ASSERT_TRUE(plan.status.ok()) << plan.status.message();
    const auto last = plan.output_leaf_pids.back();
    ASSERT_TRUE(publish_maintenance(publisher, std::move(plan.transaction)).status.ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->stats.leaf_pages, 5U);
    EXPECT_EQ(after->stats.height, 4U);
    EXPECT_EQ(after->mapping.get(last)->leaf->right_sibling, 1U);
    EXPECT_EQ(after->mapping.get(1), right);
    for (const auto *key : {"a", "b", "c", "d", "m", "n"}) {
        auto page = route(*after, key);
        ASSERT_TRUE(page && page->leaf);
        EXPECT_EQ(page->leaf->lookup(key).state, LogicalLookupState::kValue);
    }
    EXPECT_EQ(before->mapping.get(2)->inner->children, (InnerPageState::Children{0, 1}));
}

TEST(MaintenancePublication, SameParentMergeRemovesRightAndContractsRootAtomically)
{
    PublicationCoordinator publisher;
    auto                   left   = maintenance_leaf({"a"}, 1, {.upper = "b"}, 1);
    auto                   right  = maintenance_leaf({"b"}, 1, {.lower = "b"});
    auto                   parent = maintenance_inner({0, 1}, {"b"});
    ASSERT_TRUE(publish_maintenance(publisher,
                                    {
                                        .record       = maintenance_record(0),
                                        .pages        = {{.page_id = 0, .expected = {}, .replacement = left},
                                                         {.page_id = 1, .expected = {}, .replacement = right},
                                                         {.page_id = 2, .expected = {}, .replacement = parent}},
                                        .root_page_id = 2
    })
                    .status.ok());
    const auto before = publisher.capture();
    auto       plan   = build_merge_publication(before, 0, 1, maintenance_record(1), 100, maintenance_limits());
    ASSERT_TRUE(plan.status.ok()) << plan.status.message();
    ASSERT_TRUE(plan.changed());
    ASSERT_TRUE(publish_maintenance(publisher, std::move(plan.transaction)).status.ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->root_page_id, 0U);
    EXPECT_EQ(after->next_page_id, 3U);
    EXPECT_EQ(after->stats.leaf_pages, 1U);
    EXPECT_EQ(after->stats.inner_pages, 0U);
    EXPECT_EQ(after->stats.height, 1U);
    EXPECT_EQ(after->stats.dirty_pages, 1U);
    EXPECT_EQ(after->stats.checkpoint_pending_pages, 1U);
    EXPECT_TRUE(after->stats.checkpoint_mapping_pending);
    EXPECT_EQ(after->stats.kv_known_count, 2U);
    EXPECT_EQ(after->stats.kv_known_bytes, 4U);
    EXPECT_EQ(after->mapping.get(1), nullptr);
    EXPECT_EQ(after->mapping.get(2), nullptr);
    EXPECT_EQ(after->mapping.get(0)->leaf->right_sibling, kInvalidPageId);
    EXPECT_EQ(route(*after, "b")->leaf->lookup("b").cell.owner, right->leaf->lookup("b").cell.owner);
    EXPECT_EQ(before->mapping.get(2), parent);
    EXPECT_EQ(before->mapping.get(1), right);
}

TEST(MaintenancePublication, CoalesceUpdatesExactPrimarySizeWhenItRemovesAnEligibleTombstone)
{
    PublicationCoordinator publisher;
    auto                   leaf    = maintenance_leaf({"a"}, 1);
    auto                   logical = std::make_shared<PageState>(*leaf->leaf);
    OverlayBuilder         builder({});
    auto tombstone = std::make_shared<CellVersion>(encode_cell_buf(2, OpKind::kDelete, {}), 2, kFlagTombstone);
    ASSERT_TRUE(builder.append("b", std::move(tombstone)).ok());
    std::shared_ptr<const OverlayBatch> run;
    ASSERT_TRUE(builder.finish(2, &run).ok());
    logical->overlays.push_back(std::move(run));
    leaf->leaf        = logical;
    const auto layout = plan_leaf_layout(logical, maintenance_limits().pages);
    ASSERT_TRUE(layout.status.ok());
    leaf->metrics.page_size_known          = true;
    leaf->metrics.page_size_bytes          = layout.total_primary_bytes;
    leaf->metrics.page_size_estimate_bytes = layout.total_primary_bytes;
    leaf->metrics.kv_known                 = true;
    leaf->metrics.kv_count                 = 1;
    leaf->metrics.kv_bytes                 = 2;
    auto sources                           = std::make_shared<PublishedSources>();
    sources->floor                         = 2;
    publisher.request_gc_floor(2);
    ASSERT_TRUE(publish_maintenance(publisher, {.record       = maintenance_record(0),
                                                .pages        = {{.page_id = 0, .expected = {}, .replacement = leaf}},
                                                .root_page_id = 0,
                                                .coverage     = 2,
                                                .expected_sources = publisher.capture()->sources,
                                                .sources          = std::move(sources)})
                    .status.ok());
    const auto before = publisher.capture();
    auto       plan   = build_coalesce_publication(before, 0, maintenance_record(1), 100, maintenance_limits());
    ASSERT_TRUE(plan.status.ok()) << plan.status.to_string();
    ASSERT_TRUE(publish_maintenance(publisher, std::move(plan.transaction)).status.ok());
    const auto after = publisher.capture()->mapping.get(0);
    EXPECT_TRUE(after->metrics.page_size_known);
    EXPECT_EQ(after->metrics.page_size_bytes,
              kFrameHeaderSize + kFrameTrailerSize + kLeafSlotSize + kCellHeaderSize + 2);
    EXPECT_EQ(after->metrics.kv_count, 1U);
    EXPECT_EQ(after->metrics.kv_bytes, 2U);
    EXPECT_EQ(after->leaf->lookup("b").state, LogicalLookupState::kMissing);
    EXPECT_EQ(before->mapping.get(0)->leaf->lookup("b").state, LogicalLookupState::kTombstone);
}

TEST(MaintenancePublication, CrossParentMergeAndMetadataExhaustionLeaveTreeUnchanged)
{
    PublicationCoordinator publisher;
    seed_leaf(publisher, maintenance_leaf({"a", "b", "c", "d"}, 30));
    auto       options        = maintenance_limits();
    const auto original       = publisher.capture();
    options.max_changed_pages = 1;
    auto denied               = build_split_publication(original, 0, maintenance_record(1), options);
    EXPECT_EQ(denied.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(denied.changed());
    EXPECT_EQ(publisher.capture(), original);
    options.max_changed_pages = 256;
    auto split                = build_split_publication(original, 0, maintenance_record(2), options);
    ASSERT_TRUE(split.status.ok());
    ASSERT_TRUE(publish_maintenance(publisher, std::move(split.transaction)).status.ok());
    const auto before = publisher.capture();
    auto       merged = build_merge_publication(before, 1, 2, maintenance_record(3), 100, options);
    EXPECT_EQ(merged.status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(merged.changed());
    EXPECT_EQ(publisher.capture(), before);
}

TEST(MaintenancePublication, StaleLeafPlanCannotPublishAnyOfItsStructuralChanges)
{
    PublicationCoordinator publisher;
    auto                   original = maintenance_leaf({"a", "b", "c", "d"}, 30);
    seed_leaf(publisher, original);
    auto plan = build_split_publication(publisher.capture(), 0, maintenance_record(1), maintenance_limits());
    ASSERT_TRUE(plan.status.ok());
    auto newer = std::make_shared<PublishedPage>(*original);
    ++newer->revision;
    ASSERT_TRUE(publish_maintenance(publisher, {.record = maintenance_record(2),
                                                .pages  = {{.page_id = 0, .expected = original, .replacement = newer}}})
                    .status.ok());
    const auto before = publisher.capture();
    EXPECT_EQ(publish_maintenance(publisher, std::move(plan.transaction)).status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture(), before);
    EXPECT_EQ(before->stats.leaf_pages, 1U);
    EXPECT_EQ(before->next_page_id, 1U);
}

TEST(MaintenancePublication, RootPreconditionRejectsReparentingEvenWhenTargetLeafIsUnchanged)
{
    PublicationCoordinator publisher;
    auto                   original = maintenance_leaf({"a"}, 1);
    seed_leaf(publisher, original);
    auto plan = build_coalesce_publication(publisher.capture(), 0, maintenance_record(1), 100);
    ASSERT_TRUE(plan.status.ok());
    ASSERT_TRUE(plan.changed());
    auto parent = maintenance_inner({0}, {});
    ASSERT_TRUE(publish_maintenance(publisher, {.record       = maintenance_record(2),
                                                .pages        = {{.page_id = 1, .expected = {}, .replacement = parent}},
                                                .root_page_id = 1})
                    .status.ok());
    const auto before = publisher.capture();
    EXPECT_EQ(publish_maintenance(publisher, std::move(plan.transaction)).status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture(), before);
    EXPECT_EQ(before->root_page_id, 1U);
}

TEST(MaintenancePublication, CoalescePreservesUnknownSummaryAndLogicalIdentity)
{
    PublicationCoordinator publisher;
    auto                   original = maintenance_leaf({"a", "b"}, 1);
    auto                   logical  = std::make_shared<PageState>(*original->leaf);
    logical->overlays.push_back(logical->overlays.front());
    original->leaf                          = logical;
    original->metrics.overlay_runs          = 2;
    original->metrics.overlay_entries       = 4;
    original->metrics.consolidate_candidate = true;
    seed_leaf(publisher, original);
    ASSERT_TRUE(
        publish_maintenance(publisher,
                            {
                                .record            = maintenance_record(1000),
                                .expected_coverage = 0,
                                .checkpoint = CheckpointPublication{.action = CheckpointAction::Capture, .token = 1}
    })
            .status.ok());
    ASSERT_TRUE(
        publish_maintenance(publisher,
                            {
                                .record     = maintenance_record(1001),
                                .checkpoint = CheckpointPublication{.action = CheckpointAction::Commit, .token = 1}
    })
            .status.ok());
    auto plan = build_coalesce_publication(publisher.capture(), 0, maintenance_record(1), 100);
    ASSERT_TRUE(plan.status.ok());
    ASSERT_TRUE(publish_maintenance(publisher, std::move(plan.transaction)).status.ok());
    const auto after = publisher.capture();
    const auto page  = after->mapping.get(0);
    EXPECT_EQ(page->leaf->content_id, logical->content_id);
    EXPECT_EQ(page->leaf->route_generation, logical->route_generation);
    EXPECT_EQ(page->leaf->representation_id, logical->representation_id + 1);
    EXPECT_EQ(after->stats.overlay_runs, 1U);
    EXPECT_EQ(after->stats.overlay_entries, 2U);
    EXPECT_EQ(after->stats.consolidate_candidate_pages, 0U);
    EXPECT_EQ(after->stats.page_size_unknown_pages, 1U);
    EXPECT_EQ(after->stats.kv_unknown_pages, 1U);
    EXPECT_EQ(after->stats.checkpoint_pending_pages, 0U);
    EXPECT_FALSE(after->stats.checkpoint_mapping_pending);
}

TEST(MaintenancePublication, ExceptionalSeparatorStillBuildsACompleteBoundedRoot)
{
    PublicationCoordinator publisher;
    seed_leaf(
        publisher,
        maintenance_leaf({std::string(20, 'a'), std::string(20, 'b'), std::string(20, 'c'), std::string(20, 'd')}, 30));
    const auto before       = publisher.capture();
    auto       options      = maintenance_limits();
    options.max_inner_bytes = kFrameHeaderSize + kFrameTrailerSize + (2 * sizeof(uint64_t)) + kInnerSlotSize + 1;
    auto plan               = build_split_publication(before, 0, maintenance_record(1), options);
    ASSERT_TRUE(plan.status.ok()) << plan.status.to_string();
    EXPECT_TRUE(plan.changed());
    EXPECT_EQ(publisher.capture(), before);
    ASSERT_TRUE(publish_maintenance(publisher, std::move(plan.transaction)).status.ok());
    const auto after = publisher.capture();
    EXPECT_GT(after->mapping.get(after->root_page_id)->level, 0U);
}
} // namespace
} // namespace crowdb::tree
