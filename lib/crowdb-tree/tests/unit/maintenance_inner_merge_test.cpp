// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_operation.h"
#include "btree/maintenance_publication.h"
#include "btree/page_load.h"

#include <gtest/gtest.h>

#include <future>
#include <map>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<InstallRecord> token(uint64_t sequence)
{
    return std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 10, .sequence = sequence});
}

PublicationResult publish(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> result;
    auto                            future = result.get_future();
    request->on_done                       = [&](auto value) { result.set_value(std::move(value)); };
    EXPECT_TRUE(publisher.try_submit(request));
    return future.get();
}

std::shared_ptr<PublishedPage> inner(uint32_t level, std::vector<uint64_t> children,
                                     std::vector<std::string> separators)
{
    auto route = std::make_shared<InnerPageState>();
    route->children.assign(children.begin(), children.end());
    route->separators.assign(separators.begin(), separators.end());
    auto page          = std::make_shared<PublishedPage>();
    page->revision     = 1;
    page->level        = level;
    page->inner        = std::move(route);
    page->metrics.leaf = false;
    return page;
}

struct MergeTree
{
    PublicationCoordinator        publisher;
    MaintenancePublicationOptions options;

    MergeTree()
    {
        options.max_inner_children        = 4;
        options.inner_merge_keys          = 2;
        options.pages.primary_page_bytes  = 256;
        options.pages.target_page_bytes   = 220;
        options.pages.merge_trigger_bytes = 190;
        options.pages.merge_target_bytes  = 210;
        PublicationTransaction           seed{.record = token(0)};
        const std::array<std::string, 4> keys{"a", "b", "m", "n"};
        for (size_t i = 0; i < keys.size(); ++i) {
            auto leaf              = std::make_shared<PageState>();
            leaf->route_generation = leaf->content_id = leaf->representation_id = 1;
            if (i != 0) {
                leaf->range.lower = OwnedKey(keys[i]);
            }
            if (i + 1 < keys.size()) {
                leaf->range.upper = OwnedKey(keys[i + 1]);
            }
            leaf->right_sibling = i + 1 < keys.size() ? i + 1 : kInvalidPageId;
            OverlayBuilder builder({});
            auto           cell = std::make_shared<CellVersion>(encode_cell_buf(1, OpKind::kPut, "v"), 1, 0);
            EXPECT_TRUE(builder.append(keys[i], cell).ok());
            std::shared_ptr<const OverlayBatch> run;
            EXPECT_TRUE(builder.finish(i + 1, &run).ok());
            leaf->overlays.push_back(std::move(run));
            auto page      = std::make_shared<PublishedPage>();
            page->revision = 1;
            page->leaf     = std::move(leaf);
            seed.pages.push_back({.page_id = i, .expected = {}, .replacement = page});
        }
        seed.pages.push_back({.page_id = 10, .expected = {}, .replacement = inner(1, {0, 1}, {"b"})});
        seed.pages.push_back({.page_id = 11, .expected = {}, .replacement = inner(1, {2, 3}, {"n"})});
        seed.pages.push_back({.page_id = 20, .expected = {}, .replacement = inner(2, {10, 11}, {"m"})});
        seed.root_page_id = 20;
        EXPECT_TRUE(publish(publisher, std::move(seed)).status.ok());
    }
};

TEST(MaintenanceInnerMerge, RepairsUntouchedUnderfullRouteAndKeepsOldView)
{
    MergeTree  tree;
    const auto before = tree.publisher.capture();
    auto       plan   = build_inner_merge_publication(before, 2, token(1), tree.options);
    ASSERT_TRUE(plan.status.ok()) << plan.status.to_string();
    ASSERT_TRUE(plan.changed());
    EXPECT_EQ(plan.merged_inner_pairs, 1U);
    ASSERT_TRUE(publish(tree.publisher, std::move(plan.transaction)).status.ok());
    const auto after = tree.publisher.capture();
    EXPECT_EQ(before->mapping.copy_bounds().max_level, 2U);
    EXPECT_EQ(after->mapping.copy_bounds().max_level, 1U);
    EXPECT_EQ(after->root_page_id, 10U);
    EXPECT_FALSE(after->mapping.get(11));
    EXPECT_FALSE(after->mapping.get(20));
    const auto route = after->mapping.get(10)->inner;
    EXPECT_EQ(route->children.size(), 4U);
    EXPECT_EQ(route->separators[1], OwnedKey("m"));
    EXPECT_EQ(before->mapping.get(20)->inner->children.size(), 2U);
    EXPECT_EQ(before->mapping.get(11)->inner->children.size(), 2U);
    EXPECT_EQ(before->mapping.get(2)->leaf->lookup("m").cell.value.to_string(), "v");
}

TEST(MaintenanceInnerMerge, LeafMergeRepairsItsParentInTheSameTransaction)
{
    MergeTree tree;
    auto      plan = build_merge_publication(tree.publisher.capture(), 2, 3, token(1), 10, tree.options);
    ASSERT_TRUE(plan.status.ok()) << plan.status.to_string();
    ASSERT_TRUE(plan.changed());
    EXPECT_EQ(plan.merged_inner_pairs, 1U);
    ASSERT_TRUE(publish(tree.publisher, std::move(plan.transaction)).status.ok());
    const auto after = tree.publisher.capture();
    EXPECT_EQ(after->root_page_id, 10U);
    EXPECT_EQ(after->mapping.get(10)->inner->children.size(), 3U);
    EXPECT_FALSE(after->mapping.get(3));
    EXPECT_EQ(after->mapping.get(2)->leaf->lookup("n").cell.value.to_string(), "v");
}

TEST(MaintenanceInnerMerge, FanoutAndEncodedBytesPreventOversizedMerge)
{
    MergeTree tree;
    tree.options.max_inner_children = 3;
    auto plan = build_inner_merge_publication(tree.publisher.capture(), 2, token(1), tree.options);
    ASSERT_TRUE(plan.status.ok());
    EXPECT_FALSE(plan.changed());
    tree.options.max_inner_children = 4;
    tree.options.max_inner_bytes = kFrameHeaderSize + kFrameTrailerSize + (2 * sizeof(uint64_t)) + kInnerSlotSize + 1;
    plan                         = build_inner_merge_publication(tree.publisher.capture(), 2, token(2), tree.options);
    ASSERT_TRUE(plan.status.ok());
    EXPECT_FALSE(plan.changed());
}

TEST(MaintenanceInnerMerge, ChangedPageBoundRejectsWithoutPublication)
{
    MergeTree  tree;
    const auto before              = tree.publisher.capture();
    tree.options.max_changed_pages = 1;
    const auto plan                = build_inner_merge_publication(before, 2, token(1), tree.options);
    EXPECT_EQ(plan.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(plan.changed());
    EXPECT_EQ(tree.publisher.capture(), before);
}

TEST(MaintenanceInnerMerge, ChangedSiblingInvalidatesWholePreparedTransaction)
{
    MergeTree  tree;
    const auto before = tree.publisher.capture();
    auto       plan   = build_inner_merge_publication(before, 2, token(1), tree.options);
    ASSERT_TRUE(plan.status.ok());
    auto changed = std::make_shared<PublishedPage>(*before->mapping.get(10));
    ++changed->revision;
    ASSERT_TRUE(publish(tree.publisher,
                        {.record = token(2),
                         .pages  = {{.page_id = 10, .expected = before->mapping.get(10), .replacement = changed}}})
                    .status.ok());
    const auto intervening = tree.publisher.capture();
    EXPECT_EQ(publish(tree.publisher, std::move(plan.transaction)).status.code(), Code::kUnavailable);
    EXPECT_EQ(tree.publisher.capture(), intervening);
}

class TestInnerLoader final : public PageLoader
{
  public:
    std::map<uint64_t, std::shared_ptr<const InnerPageState>> frames;
    size_t                                                    calls        = 0;
    bool                                                      fail_sibling = false;

    void load_leaf(BaseRef /*base*/, LeafComplete complete) override
    {
        ADD_FAILURE() << "inner repair must not load leaf payload";
        complete(Status::internal_error(), {});
    }

    void load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete) override
    {
        ++calls;
        if (fail_sibling && backing->address == 11) {
            complete(Status::io_error("injected sibling failure"), {});
            return;
        }
        complete(Status::Ok(), frames.at(backing->address));
    }
};

void cold_repair(bool fail)
{
    MergeTree tree;
    auto      loader     = std::make_shared<TestInnerLoader>();
    loader->fail_sibling = fail;
    PublicationTransaction evict{.record = token(3)};
    const auto             before = tree.publisher.capture();
    for (const uint64_t pid : {10, 11, 20}) {
        const auto old = before->mapping.get(pid);
        loader->frames.emplace(pid, old->inner);
        auto page = std::make_shared<PublishedPage>(*old);
        page->inner.reset();
        auto backing        = std::make_shared<BackingRef>();
        backing->address    = pid;
        backing->length     = 512;
        backing->owner      = std::make_shared<int>(1);
        page->inner_backing = std::move(backing);
        evict.pages.push_back({.page_id = pid, .expected = old, .replacement = std::move(page)});
    }
    ASSERT_TRUE(publish(tree.publisher, std::move(evict)).status.ok());
    const auto captured = tree.publisher.capture();
    EXPECT_EQ(captured->mapping.copy_bounds().max_level, 2U);
    detail::ResourceBudget::Limits limits{};
    limits.fill(16 * 1024 * 1024);
    detail::ResourceBudget          budget(limits, 1);
    auto                            account = budget.make_account(limits);
    detail::FlushRuntime            runtime(1, 4);
    auto                            lane = std::make_shared<detail::FlushRuntime::Lane>(4);
    PageMaintenanceOperationOptions options;
    options.publication = tree.options;
    options.max_steps = options.max_candidates = 1;
    options.acquire                            = [&](auto *out) {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, 16 * 1024 * 1024);
        if (!credit) {
            return Status::resource_exhausted();
        }
        *out = detail::AllocationReservation::create(std::move(*credit));
        return Status::Ok();
    };
    auto maintenance = make_page_maintenance(tree.publisher, runtime, lane, std::move(options), loader, {});
    std::promise<Status> completion;
    auto                 result = completion.get_future();
    maintenance(captured, [&](Status status, uint64_t) { completion.set_value(std::move(status)); });
    const auto status = result.get();
    runtime.drain();
    EXPECT_EQ(loader->calls, 3U);
    if (fail) {
        EXPECT_EQ(status.code(), Code::kIoError);
        EXPECT_EQ(tree.publisher.capture(), captured);
    }
    else {
        EXPECT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(tree.publisher.capture()->root_page_id, 10U);
        EXPECT_FALSE(captured->mapping.get(20)->inner);
        EXPECT_EQ(tree.publisher.capture()->mapping.get(10)->inner->children.size(), 4U);
    }
}

TEST(MaintenanceInnerMerge, ColdSiblingAndDescendingRoutePublishOnOneWorker)
{
    cold_repair(false);
}

TEST(MaintenanceInnerMerge, ColdSiblingFailureKeepsCapturedTree)
{
    cold_repair(true);
}
} // namespace
} // namespace crowdb::tree
