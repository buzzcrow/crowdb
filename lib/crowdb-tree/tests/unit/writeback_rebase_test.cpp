// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/writeback_rebase.h"
#include "load_budget_test_support.h"
#include "published_read_test_support.h"
#include "snapshot/captured_rebase.h"

namespace crowdb::tree
{
namespace
{
using namespace test_read;

DurableCapturedFrame receipt()
{
    auto backing        = std::make_shared<BackingRef>();
    backing->backend_id = 1;
    backing->content_id = 33;
    backing->generation = 7;
    backing->length     = 4096;
    backing->owner      = std::make_shared<int>(1);
    DurableCapturedFrame result{.page_id = 0, .backing = std::move(backing)};
    result.summary.emplace();
    result.summary->primary_used_bytes = 128;
    result.summary->live_counts_known  = true;
    result.summary->live_keys          = 2;
    result.summary->live_key_bytes     = 2;
    result.summary->live_value_bytes   = 6;
    return result;
}

std::shared_ptr<PublishedPage> dirty_page(std::shared_ptr<const PageState> leaf)
{
    auto result                         = published(std::move(leaf));
    result->metrics.overlay_runs        = result->leaf->overlays.size();
    result->metrics.overlay_entries     = 2;
    result->metrics.writeback_candidate = true;
    return result;
}

TEST(WritebackRebase, ReplacesCapturedSuffixAndKeepsNewValuesAndTombstones)
{
    LoadCredits credits;
    auto        captured     = dirty_page(page({
        {.key = "a", .value = "old"},
        {.key = "b", .value = "old"}
    }));
    auto        current_leaf = std::make_shared<PageState>(*captured->leaf);
    auto        later        = page({
        {.key = "a", .slot = 11, .value = "new"},
        {.key = "b", .slot = 11, .value = "", .tombstone = true}
    });
    current_leaf->overlays.insert(current_leaf->overlays.begin(), later->overlays.front());
    current_leaf->content_id                     = 9;
    auto                                 current = dirty_page(current_leaf);
    std::shared_ptr<const PublishedPage> replacement;
    ASSERT_TRUE(
        build_writeback_rebase(captured, current, receipt(), 10, credits.credits.reservation(), &replacement).ok());
    ASSERT_EQ(replacement->leaf->overlays.size(), 1U);
    EXPECT_EQ(replacement->leaf->overlays.front(), later->overlays.front());
    EXPECT_EQ(replacement->leaf->content_id, 9U);
    EXPECT_TRUE(replacement->metrics.dirty);
    EXPECT_EQ(replacement->metrics.overlay_entries, 2U);
    EXPECT_EQ(captured->leaf->overlays.size(), 1U);
    EXPECT_EQ(current->leaf->overlays.size(), 2U);
    auto loader                         = std::make_shared<TestLoader>();
    loader->frames[33]                  = base({
        {.key = "a", .value = "old"},
        {.key = "b", .value = "old"}
    });
    auto                          fixed = view(replacement->leaf, {}, 10);
    std::shared_ptr<PublishedGet> get;
    ASSERT_TRUE(PublishedGet::create(fixed, loader, Slice("a"), {}, &get).ok());
    complete_inline(get->sender());
    ASSERT_TRUE(get->result().found);
    EXPECT_EQ(get->result().cell.value.to_string(), "new");
    get.reset();
    ASSERT_TRUE(PublishedGet::create(fixed, loader, Slice("b"), {}, &get).ok());
    complete_inline(get->sender());
    EXPECT_FALSE(get->result().found);
}

TEST(WritebackRebase, CleanReceiptMakesColdPageExactWithoutRetainingCapturedRun)
{
    auto                                 captured = dirty_page(page({
        {.key = "a", .value = "old"},
        {.key = "b", .value = "old"}
    }));
    std::weak_ptr<const OverlayBatch>    old      = captured->leaf->overlays.front();
    std::shared_ptr<const PublishedPage> replacement;
    ASSERT_TRUE(build_writeback_rebase(captured, captured, receipt(), 10, {}, &replacement).ok());
    EXPECT_FALSE(replacement->metrics.dirty);
    EXPECT_FALSE(replacement->metrics.writeback_candidate);
    EXPECT_TRUE(replacement->metrics.page_size_known);
    EXPECT_EQ(replacement->metrics.page_size_bytes, 128U);
    EXPECT_EQ(replacement->metrics.kv_count, 2U);
    EXPECT_EQ(replacement->metrics.kv_bytes, 8U);
    EXPECT_TRUE(replacement->leaf->overlays.empty());
    ASSERT_EQ(replacement->leaf->bases.size(), 1U);
    EXPECT_FALSE(replacement->leaf->bases.front().resident);
    captured.reset();
    EXPECT_TRUE(old.expired());
}

TEST(WritebackRebase, ChangedRouteBaseAndRunRejectWithoutPublishingOutput)
{
    auto captured = dirty_page(page({
        {.key = "a", .value = "old"}
    }));
    for (int change = 0; change < 4; ++change) {
        auto leaf = std::make_shared<PageState>(*captured->leaf);
        if (change == 0) {
            ++leaf->route_generation;
        }
        if (change == 1) {
            leaf->right_sibling = 5;
        }
        if (change == 2) {
            leaf->bases.push_back(cold(2));
        }
        if (change == 3) {
            leaf->overlays.front() = page({
                                              {.key = "a", .value = "different"}
            })
                                         ->overlays.front();
        }
        auto                                 current = dirty_page(leaf);
        std::shared_ptr<const PublishedPage> out;
        EXPECT_EQ(build_writeback_rebase(captured, current, receipt(), 10, {}, &out).code(), Code::kUnavailable);
        EXPECT_FALSE(out);
    }
}

TEST(WritebackRebase, CapturedSourcesDoNotConsumeTheRetainedSuffixWindow)
{
    auto leaf = std::make_shared<PageState>(*page({
        {.key = "a", .value = "old"}
    }));
    leaf->overlays.assign(129, leaf->overlays.front());
    auto captured     = dirty_page(leaf);
    auto current_leaf = std::make_shared<PageState>(*leaf);
    auto later        = page({
                                 {.key = "b", .slot = 11, .value = "new"}
    })
                            ->overlays.front();
    current_leaf->overlays.insert(current_leaf->overlays.begin(), later);
    auto                                 current = dirty_page(current_leaf);
    std::shared_ptr<const PublishedPage> replacement;
    auto status = build_writeback_rebase(captured, current, receipt(), 10, {}, &replacement, 1);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(replacement->leaf->overlays.size(), 1U);
    EXPECT_EQ(replacement->leaf->overlays.front(), later);
    EXPECT_EQ(replacement->leaf->bases.size(), 1U);
}

TEST(WritebackRebase, BudgetFailurePreservesCurrentAndOutput)
{
    LoadCredits credits;
    auto        metadata  = credits.credits.reservation();
    const auto  remaining = metadata->available();
    ASSERT_TRUE(metadata->claim(remaining));
    auto                                 captured = dirty_page(page({
        {.key = "a", .value = "old"}
    }));
    std::shared_ptr<const PublishedPage> out;
    EXPECT_EQ(build_writeback_rebase(captured, captured, receipt(), 10, metadata, &out).code(),
              Code::kResourceExhausted);
    EXPECT_FALSE(out);
    EXPECT_EQ(captured->leaf->overlays.size(), 1U);
    metadata->release(remaining);
}

TEST(WritebackRebase, BoundedOperationPublishesOnlyRepresentationAndDefersChangedPage)
{
    for (bool conflict : {false, true}) {
        LoadCredits credits;
        auto        initial = view(page({
                                       {.key = "a", .value = "old"}
        }),
                                   {}, 10);
        auto        current = std::make_shared<PublishedTreeState>(*initial);
        ASSERT_TRUE(apply_page_metrics(current->stats, nullptr, &current->mapping.get(0)->metrics));
        if (conflict) {
            auto changed = std::make_shared<PageState>(*initial->mapping.get(0)->leaf);
            ++changed->route_generation;
            MappingView::CopyStats         copied;
            std::array<PageReplacement, 1> changes{
                {{.page_id = 0, .expected = initial->mapping.get(0), .replacement = published(changed)}}};
            current->mapping = current->mapping.replace(changes, copied);
        }
        auto                   runtime = std::make_shared<detail::FlushRuntime>(1, 8);
        auto                   lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
        PublicationCoordinator publisher(4, 1, {}, std::pmr::get_default_resource(), {},
                                         {.metadata = {}, .initial = current, .source_catalog = {}});
        auto                   pages = std::make_shared<CapturedDurablePages>(nullptr);
        pages->coverage              = 10;
        pages->generation            = 1;
        pages->frames.emplace(0, receipt());
        std::shared_ptr<CapturedRebaseOperation> operation;
        ASSERT_TRUE(CapturedRebaseOperation::create(publisher, initial, pages, runtime, lane,
                                                    credits.credits.reservation(), &operation)
                        .ok());
        Completion done;
        auto       connected = stdexec::connect(operation->sender(), Receiver{.done = &done});
        stdexec::start(connected);
        runtime->drain();
        ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
        EXPECT_EQ(operation->result().installed, conflict ? 0U : 1U);
        EXPECT_EQ(operation->result().deferred, conflict ? 1U : 0U);
        EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, current->stats.checkpoint_pending_pages);
        EXPECT_EQ(publisher.capture()->stats.checkpoint_mapping_pending, current->stats.checkpoint_mapping_pending);
        if (!conflict) {
            EXPECT_FALSE(publisher.capture()->mapping.get(0)->metrics.dirty);
        }
    }
}

TEST(WritebackRebase, InnerKeepsRoutingOwnerAndInstallsDurableBacking)
{
    auto inner = std::make_shared<InnerPageState>();
    inner->children.push_back(1);
    auto captured           = std::make_shared<PublishedPage>();
    captured->level         = 1;
    captured->inner         = inner;
    captured->metrics.leaf  = false;
    captured->metrics.dirty = true;
    auto written            = receipt();
    written.type            = page_type::kInnerBase;
    std::shared_ptr<const PublishedPage> out;
    ASSERT_TRUE(build_writeback_rebase(captured, captured, written, 10, {}, &out).ok());
    EXPECT_EQ(out->inner, inner);
    EXPECT_EQ(out->inner_backing, written.backing);
    EXPECT_FALSE(out->metrics.dirty);
    EXPECT_TRUE(out->metrics.evict_candidate);
}
} // namespace
} // namespace crowdb::tree
