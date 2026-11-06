// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "load_budget_test_support.h"
#include "published_read_test_support.h"
#include "snapshot/captured_plan.h"
#include "snapshot/captured_rebase.h"
#include "snapshot/primary_backing.h"

namespace crowdb::tree
{
namespace
{
using namespace test_read;

TEST(CapturedRebaseReuse, LayoutCountsOnlyExactPrimariesWithoutRelocation)
{
    for (bool relocation : {false, true}) {
        CapturedWriteLimits limits;
        limits.frames.exclusion.length = relocation ? 4096 : 0;
        CapturedLayoutPlanner planner(2, 1, limits, 1, {}, {});
        for (uint64_t pid = 0; pid < 2; ++pid) {
            ASSERT_TRUE(
                planner
                    .accept(
                        {.page_id = pid, .summary = {}, .reused_overflow = {}, .owner = {}, .reused_primary = pid == 0})
                    .ok());
            ASSERT_TRUE(planner.step().ok());
        }
        ASSERT_TRUE(planner.finish().ok());
        EXPECT_EQ(planner.result().primary_frames, 2U);
        EXPECT_EQ(planner.result().reused_primary_frames, relocation ? 0U : 1U);
        EXPECT_EQ(planner.result().frame_count_upper, 2U);
    }
}

TEST(CapturedRebaseReuse, LayoutSeparatesRadixBatchesFromDurableSegments)
{
    CapturedLayoutPlanner planner(1025, 1, {}, 2, {}, {});
    for (uint64_t pid : {0U, 63U, 64U, 128U, 1023U, 1024U}) {
        ASSERT_TRUE(
            planner
                .accept(
                    {.page_id = pid, .summary = {}, .reused_overflow = {}, .owner = {}, .reused_primary = pid == 128})
                .ok());
        ASSERT_TRUE(planner.step().ok());
    }
    ASSERT_TRUE(planner.finish().ok());
    EXPECT_EQ(planner.result().primary_frames, 6U);
    EXPECT_EQ(planner.result().reused_primary_frames, 1U);
    EXPECT_EQ(planner.result().segments, 2U);
    EXPECT_EQ(planner.result().rebase_batches, 4U);
}

struct TestReuse
{
    explicit TestReuse(bool later_overlay)
    {
        auto base_ref = cold(33, 10);
        auto leaf     = std::make_shared<PageState>();
        leaf->bases.push_back(base_ref);
        frame = {.page_id       = 0,
                 .type          = page_type::kLeafBase,
                 .frame_bytes   = 4096,
                 .logical_bytes = 4096,
                 .backing       = base_ref.backing};
        frame.summary.emplace();
        frame.summary->live_counts_known = true;
        frame.summary->live_keys         = 1;
        frame.summary->live_key_bytes    = 1;
        frame.summary->live_value_bytes  = 3;
        frame.stored_overflow            = std::make_shared<StoredOverflowReferences>();
        auto page                        = published(leaf);
        page->metrics.dirty              = false;
        page->primary                    = make_primary_backing(frame, *leaf, 10, 0, {});
        EXPECT_TRUE(page->primary);
        initial               = std::make_shared<PublishedTreeState>(nullptr, 10);
        initial->coverage     = 10;
        initial->root_page_id = 0;
        initial->next_page_id = 1;
        MappingView::CopyStats copied;
        initial->mapping = initial->mapping.replace(
            std::array<PageReplacement, 1>{{{.page_id = 0, .expected = {}, .replacement = page}}}, copied);
        EXPECT_TRUE(apply_page_metrics(initial->stats, nullptr, &page->metrics));
        current = std::make_shared<PublishedTreeState>(*initial);
        if (later_overlay) {
            auto changed_leaf = std::make_shared<PageState>(*leaf);
            auto run          = test_read::page({
                {.key = "a", .slot = 11, .value = "new"}
            });
            changed_leaf->overlays.push_back(run->overlays.front());
            auto changed           = std::make_shared<PublishedPage>(*page);
            changed->leaf          = changed_leaf;
            changed->metrics.dirty = true;
            current->mapping       = current->mapping.replace(
                std::array<PageReplacement, 1>{{{.page_id = 0, .expected = page, .replacement = changed}}}, copied);
        }
    }

    DurableCapturedFrame                frame;
    std::shared_ptr<PublishedTreeState> initial;
    std::shared_ptr<PublishedTreeState> current;
};

TEST(CapturedRebaseReuse, ZeroReplacementCreditPreservesExactPageAndLaterOverlay)
{
    for (bool later_overlay : {false, true}) {
        TestReuse   fixture(later_overlay);
        LoadCredits credits;
        const auto  bound = CapturedRebaseOperation::allocation_bound(0);
        if (!bound) {
            FAIL() << "zero-replacement rebase bound overflow";
        }
        auto paid = credits.credits.reservation()->take_reserved(
            *bound + detail::shared_allocation_bound<detail::AllocationReservation>());
        if (!paid) {
            FAIL() << "zero-replacement rebase credit unavailable";
        }
        auto                   metadata = detail::AllocationReservation::create(std::move(*paid));
        auto                   runtime  = std::make_shared<detail::FlushRuntime>(1, 8);
        auto                   lane     = std::make_shared<detail::FlushRuntime::Lane>(8);
        PublicationCoordinator publisher(4, 1, {}, std::pmr::get_default_resource(), {},
                                         {.metadata = {}, .initial = fixture.current, .source_catalog = {}});
        auto                   pages = std::make_shared<CapturedDurablePages>(nullptr);
        pages->coverage              = 10;
        pages->generation            = fixture.initial->generation;
        pages->root_page_id          = 0;
        pages->frames.emplace(0, fixture.frame);
        std::shared_ptr<CapturedRebaseOperation> operation;
        ASSERT_TRUE(
            CapturedRebaseOperation::create(publisher, fixture.initial, pages, runtime, lane, metadata, &operation)
                .ok());
        Completion done;
        auto       connected = stdexec::connect(operation->sender(), Receiver{.done = &done});
        stdexec::start(connected);
        runtime->drain();
        ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
        EXPECT_EQ(operation->result().installed, 0U);
        EXPECT_EQ(operation->result().deferred, 0U);
        EXPECT_EQ(operation->result().reused, 1U);
        const auto actual = publisher.capture()->mapping.get(0);
        EXPECT_EQ(actual, fixture.current->mapping.get(0));
        EXPECT_EQ(actual->leaf->overlays.size(), later_overlay ? 1U : 0U);
        if (later_overlay) {
            auto loader        = std::make_shared<TestLoader>();
            loader->frames[33] = base({
                {.key = "a", .value = "old"}
            });
            std::shared_ptr<PublishedGet> get;
            ASSERT_TRUE(PublishedGet::create(publisher.capture(), loader, Slice("a"), {}, &get).ok());
            complete_inline(get->sender());
            ASSERT_TRUE(get->result().found);
            EXPECT_EQ(get->result().cell.slot, 11U);
            EXPECT_EQ(get->result().cell.value.to_string(), "new");
        }
    }
}
} // namespace
} // namespace crowdb::tree
