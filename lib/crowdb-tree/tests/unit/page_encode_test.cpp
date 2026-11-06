// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_encode.h"
#include "btree/tree_metrics.h"

#include <gtest/gtest.h>

#include <array>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<const CellVersion> encode_cell_owner(uint64_t slot, std::string value, bool deleted = false)
{
    return std::make_shared<CellVersion>(encode_cell_buf(slot, deleted ? OpKind::kDelete : OpKind::kPut, value), slot,
                                         deleted ? kFlagTombstone : 0);
}

std::shared_ptr<PublishedPage>
encoding_leaf(const std::vector<std::pair<std::string, std::shared_ptr<const CellVersion>>> &entries,
              PageKeyRange                                                                   range = {})
{
    OverlayBuilder builder({});
    for (const auto &[key, cell] : entries) {
        EXPECT_TRUE(builder.append(Slice(key), cell).ok());
    }
    std::shared_ptr<const OverlayBatch> run;
    EXPECT_TRUE(builder.finish(1, &run).ok());
    auto logical   = std::make_shared<PageState>();
    logical->range = std::move(range);
    if (run) {
        logical->overlays.push_back(std::move(run));
    }
    auto page  = std::make_shared<PublishedPage>();
    page->leaf = std::move(logical);
    return page;
}

std::shared_ptr<PublishedTreeState> encoding_capture(std::vector<std::shared_ptr<PublishedPage>> pages,
                                                     uint64_t root = 0, uint64_t coverage = 10, uint64_t gc_floor = 0)
{
    auto                         view = std::make_shared<PublishedTreeState>();
    std::vector<PageReplacement> replacements;
    replacements.reserve(pages.size());
    for (size_t index = 0; index < pages.size(); ++index) {
        replacements.push_back({.page_id = index, .expected = {}, .replacement = std::move(pages[index])});
    }
    MappingView::CopyStats copied;
    view->mapping      = view->mapping.replace(replacements, copied);
    view->root_page_id = root;
    view->next_page_id = pages.size();
    view->coverage     = coverage;
    view->gc_floor     = gc_floor;
    return view;
}

TEST(FinalPageEncoder, LeafResolvesCapturedWinnersAndGcWithoutIntermediateValueCopies)
{
    auto page   = encoding_leaf({
        {"a", encode_cell_owner(3, "new")},
        {"b", encode_cell_owner(2, "", true)},
        {"c", encode_cell_owner(4, "", true)},
        {"d", encode_cell_owner(11, "future")}
    });
    auto old    = encoding_leaf({
        {"a", encode_cell_owner(1, "old")     },
        {"d", encode_cell_owner(5, "captured")}
    });
    auto merged = std::make_shared<PageState>(*page->leaf);
    merged->overlays.push_back(old->leaf->overlays.front());
    page->leaf    = merged;
    auto captured = encoding_capture({page}, 0, 10, 2);
    auto prepared = prepare_final_page(captured, 0, {}, {.frame_bytes = 512});
    ASSERT_TRUE(prepared.status.ok());
    ASSERT_NE(prepared.encoder, nullptr);
    EXPECT_EQ(prepared.encoder->summary().encoded_entries, 3U);
    EXPECT_TRUE(prepared.encoder->summary().live_counts_known);
    EXPECT_EQ(prepared.encoder->summary().dropped_tombstones, 1U);
    EXPECT_EQ(prepared.encoder->counters().inline_value_copy_bytes, 0U);
    std::array<uint8_t, 512> frame{};
    auto                     step = prepared.encoder->next(frame);
    ASSERT_TRUE(step.status.ok());
    ASSERT_TRUE(step.complete);
    EXPECT_TRUE(frame_validate(frame.data(), frame.size()));
    LeafFrameView leaf(frame.data(), frame.size());
    ASSERT_EQ(leaf.count(), 3U);
    EXPECT_EQ(CellView(leaf.cell(0)).value().to_string(), "new");
    EXPECT_TRUE(CellView(leaf.cell(1)).is_tombstone());
    EXPECT_EQ(CellView(leaf.cell(2)).value().to_string(), "captured");
    EXPECT_EQ(frame_lower_fence(frame.data()).to_string(), "a");
    EXPECT_EQ(frame_upper_fence(frame.data()).to_string(), "d");
    EXPECT_EQ(prepared.encoder->counters().inline_value_copy_bytes, 11U);
    EXPECT_EQ(prepared.encoder->counters().key_copy_bytes, 3U);
    EXPECT_FALSE(prepared.encoder->next(frame).frame.has_value());
}

TEST(FinalPageEncoder, LargeExternalValueStreamsThroughOneCallerFrameAndRetainsOwner)
{
    auto payload = std::make_shared<std::string>(16 * 1024, 'v');
    auto cell    = std::make_shared<CellVersion>(
        buffer::wrap_external(reinterpret_cast<const uint8_t *>(payload->data()), payload->size(),
                              new std::shared_ptr<std::string>(payload),
                              [](void *owner) { delete static_cast<std::shared_ptr<std::string> *>(owner); }),
        7, 0);
    std::weak_ptr<const CellVersion> lifetime = cell;
    auto                             captured = encoding_capture({
        encoding_leaf({{"k", cell}}
        )
    });
    auto prepared = prepare_final_page(captured, 0, {}, {.frame_bytes = 256, .inline_value_bytes = 16});
    ASSERT_TRUE(prepared.status.ok());
    ASSERT_NE(prepared.encoder, nullptr);
    captured.reset();
    cell.reset();
    EXPECT_FALSE(lifetime.expired());
    const auto count = prepared.encoder->summary().new_overflow_frames;
    EXPECT_EQ(count, (payload->size() + overflow_chunk_cap(256) - 1) / overflow_chunk_cap(256));
    std::array<uint8_t, 257> frame{};
    frame.back() = 0xAA;
    EXPECT_EQ(prepared.encoder->next(frame).status.code(), Code::kUnavailable);
    EXPECT_EQ(prepared.encoder->bind_overflow({.first = 0, .count = count, .owner = payload}).code(),
              Code::kInvalidArgument);
    ASSERT_TRUE(prepared.encoder->bind_overflow({.first = 100, .count = count, .owner = payload}).ok());
    EXPECT_EQ(prepared.encoder->next(std::span<uint8_t>(frame.data(), 255)).status.code(), Code::kInvalidArgument);
    size_t offset = 0;
    for (uint64_t index = 0; index < count; ++index) {
        auto step = prepared.encoder->next(frame);
        ASSERT_TRUE(step.status.ok());
        if (!step.frame.has_value()) {
            ADD_FAILURE() << "overflow stream did not emit a frame";
            return;
        }
        EXPECT_FALSE(step.complete);
        EXPECT_EQ(step.frame->page_id, 100 + index);
        EXPECT_TRUE(frame_validate(frame.data(), 256));
        OverflowFrameView overflow(frame.data(), 256);
        EXPECT_EQ(overflow.payload(), Slice(payload->data() + offset, overflow.chunk_len()));
        EXPECT_EQ(overflow.next_page_id(), index + 1 == count ? kInvalidPageId : 101 + index);
        offset += overflow.chunk_len();
        EXPECT_EQ(frame.back(), 0xAA);
    }
    EXPECT_EQ(offset, payload->size());
    auto primary = prepared.encoder->next(frame);
    ASSERT_TRUE(primary.status.ok());
    EXPECT_TRUE(primary.complete);
    EXPECT_TRUE(frame_validate(frame.data(), 256));
    LeafFrameView leaf(frame.data(), 256);
    EXPECT_EQ(CellView(leaf.cell(0)).overflow_head(), 100U);
    EXPECT_EQ(CellView(leaf.cell(0)).overflow_len(), payload->size());
    EXPECT_EQ(prepared.encoder->counters().overflow_value_copy_bytes, payload->size());
    EXPECT_EQ(prepared.encoder->counters().inline_value_copy_bytes, 0U);
    EXPECT_EQ(prepared.encoder->counters().primary_frames, 1U);
    prepared.encoder.reset();
    EXPECT_FALSE(lifetime.expired());
    primary.frame.reset();
    EXPECT_TRUE(lifetime.expired());
}

TEST(FinalPageEncoder, ExistingOverflowRequiresCapturedProtectionAndCopiesNoPayload)
{
    auto cell              = std::make_shared<CellVersion>(encode_overflow_cell_buf(2, 40, 99999), 2, kFlagOverflow);
    auto captured          = encoding_capture({
        encoding_leaf({{"k", cell}}
        )
    });
    captured->next_page_id = 100;
    EXPECT_EQ(prepare_final_page(captured, 0, {}).status.code(), Code::kInvalidArgument);
    auto protection = std::make_shared<int>(1);
    auto prepared   = prepare_final_page(captured, 0, {.overflow_dependencies = protection}, {.frame_bytes = 256});
    ASSERT_TRUE(prepared.status.ok());
    ASSERT_NE(prepared.encoder, nullptr);
    ASSERT_EQ(prepared.encoder->reused_overflow().size(), 1U);
    EXPECT_EQ(prepared.encoder->reused_overflow()[0].owner, protection);
    EXPECT_EQ(prepared.encoder->summary().new_overflow_frames, 0U);
    EXPECT_EQ(prepared.encoder->summary().live_value_bytes, 99999U);
    std::array<uint8_t, 256> frame{};
    EXPECT_TRUE(prepared.encoder->next(frame).complete);
    LeafFrameView leaf(frame.data(), frame.size());
    EXPECT_EQ(CellView(leaf.cell(0)).overflow_head(), 40U);
    EXPECT_EQ(prepared.encoder->counters().overflow_value_copy_bytes, 0U);
    EXPECT_EQ(prepared.encoder->counters().inline_value_copy_bytes, 0U);
}

TEST(FinalPageEncoder, OversizedPrimaryAndBoundedWorkFailBeforeOutput)
{
    auto captured  = encoding_capture({
        encoding_leaf({{"a", encode_cell_owner(1, std::string(30, 'x'))},
                       {"b", encode_cell_owner(1, std::string(30, 'x'))},
                       {"c", encode_cell_owner(1, std::string(30, 'x'))}}
        )
    });
    auto oversized = prepare_final_page(captured, 0, {}, {.frame_bytes = 256});
    EXPECT_EQ(oversized.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(oversized.encoder, nullptr);
    EXPECT_TRUE(oversized.needs_split);
    const auto work = prepare_final_page(captured, 0, {}, {.max_work_entries = 1});
    EXPECT_EQ(work.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(work.needs_split);
    const auto metadata = prepare_final_page(captured, 0, {}, {.max_metadata_bytes = 1});
    EXPECT_EQ(metadata.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(metadata.needs_split);
    EXPECT_EQ(prepare_final_page(captured, 0, {.range = {.lower = "a"}}).status.code(), Code::kInvalidArgument);
}

TEST(FinalPageEncoder, ColdLeafAndInnerReturnOwnedLoadRequestsWithoutEncoding)
{
    auto backing    = std::make_shared<BackingRef>();
    backing->length = 512;
    backing->owner  = std::make_shared<int>(1);
    auto logical    = std::make_shared<PageState>();
    logical->bases.push_back({.identity = 1, .generation = 1, .backing = backing});
    auto leaf      = std::make_shared<PublishedPage>();
    leaf->leaf     = logical;
    auto captured  = encoding_capture({leaf});
    auto leaf_plan = prepare_final_page(captured, 0, {});
    ASSERT_TRUE(leaf_plan.status.ok());
    if (!leaf_plan.needs_base.has_value()) {
        ADD_FAILURE() << "cold leaf did not return its load request";
        return;
    }
    EXPECT_EQ(leaf_plan.needs_base->page, logical);
    EXPECT_EQ(leaf_plan.encoder, nullptr);
    auto inner           = std::make_shared<PublishedPage>();
    inner->level         = 1;
    inner->inner_backing = backing;
    captured             = encoding_capture({inner});
    auto inner_plan      = prepare_final_page(captured, 0, {});
    ASSERT_TRUE(inner_plan.status.ok());
    if (!inner_plan.needs_inner.has_value()) {
        ADD_FAILURE() << "cold inner did not return its load request";
        return;
    }
    EXPECT_EQ(inner_plan.needs_inner->backing, backing);
    EXPECT_EQ(inner_plan.encoder, nullptr);
}

TEST(FinalPageEncoder, InnerUsesOnlyCapturedRoutingAndExplicitDescendantFences)
{
    auto inner        = std::make_shared<InnerPageState>();
    inner->children   = {0, 1};
    inner->separators = {"m"};
    auto root         = std::make_shared<PublishedPage>();
    root->level       = 1;
    root->inner       = inner;
    auto captured     = encoding_capture(
        {
            encoding_leaf({{"a", encode_cell_owner(1, "v")}},
            {.upper = "m"                    }
            ),
            encoding_leaf({{"m", encode_cell_owner(1, "v")}},
            {.lower = "m"                    }
            ), root
    },
        2);
    EXPECT_EQ(prepare_final_page(captured, 2, {}).status.code(), Code::kInvalidArgument);
    auto prepared = prepare_final_page(captured, 2,
                                       {
                                           .inner_fences = InnerEncodeFences{.lower_leaf = 0, .upper_leaf = 1}
    },
                                       {.frame_bytes = 256});
    ASSERT_TRUE(prepared.status.ok());
    ASSERT_NE(prepared.encoder, nullptr);
    captured.reset();
    std::array<uint8_t, 256> frame{};
    EXPECT_TRUE(prepared.encoder->next(frame).complete);
    EXPECT_TRUE(frame_validate(frame.data(), frame.size()));
    InnerFrameView encoded(frame.data(), frame.size());
    EXPECT_EQ(encoded.child_at(0), 0U);
    EXPECT_EQ(encoded.child_at(1), 1U);
    EXPECT_EQ(encoded.separator_at(0).to_string(), "m");
    EXPECT_FALSE(prepared.encoder->summary().live_counts_known);
    EXPECT_EQ(frame_lower_fence_page_id(frame.data()), 0U);
    EXPECT_EQ(frame_upper_fence_page_id(frame.data()), 1U);
}

TEST(FinalPageEncoder, GcCanProduceAnEmptyValidLeafFrame)
{
    auto captured = encoding_capture(
        {
            encoding_leaf({{"a", encode_cell_owner(1, "", true)}}
            )
    },
        0, 10, 1);
    auto prepared = prepare_final_page(captured, 0, {}, {.frame_bytes = 256});
    ASSERT_TRUE(prepared.status.ok());
    ASSERT_NE(prepared.encoder, nullptr);
    std::array<uint8_t, 256> frame{};
    EXPECT_TRUE(prepared.encoder->next(frame).complete);
    EXPECT_TRUE(frame_validate(frame.data(), frame.size()));
    EXPECT_EQ(LeafFrameView(frame.data(), frame.size()).count(), 0U);
    EXPECT_FALSE(frame_has_lower_fence(frame.data()));
}

TEST(PageEncodeMetrics, CountsActualFrameWorkAndErrorsButNotCompletedPolls)
{
    auto             metrics  = std::make_shared<TreeWorkMetrics>();
    auto             captured = encoding_capture({
        encoding_leaf({{"k", encode_cell_owner(1, "value")}}
        )
    });
    PageEncodeLimits limits;
    limits.frame_bytes  = 512;
    limits.work_metrics = metrics;
    auto prepared       = prepare_final_page(captured, 0, {}, limits);
    ASSERT_TRUE(prepared.status.ok());
    ASSERT_NE(prepared.encoder, nullptr);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].attempts, 0U);
    std::array<uint8_t, 512> frame{};
    EXPECT_EQ(prepared.encoder->next(std::span<uint8_t>(frame.data(), 511)).status.code(), Code::kInvalidArgument);
    ASSERT_TRUE(prepared.encoder->next(frame).status.ok());
    EXPECT_FALSE(prepared.encoder->next(frame).frame.has_value());
    const auto result = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)];
    EXPECT_EQ(result.attempts, 2U);
    EXPECT_EQ(result.count, 2U);
    EXPECT_EQ(result.inflight, 0U);
    EXPECT_EQ(result.outcomes[static_cast<size_t>(TreeOutcome::Error)], 1U);
    EXPECT_EQ(result.outcomes[static_cast<size_t>(TreeOutcome::Success)], 1U);
}

TEST(PageEncodeMetrics, OverflowAndPrimaryEachRecordTheirOwnEncodingWork)
{
    auto             metrics  = std::make_shared<TreeWorkMetrics>();
    auto             captured = encoding_capture({
        encoding_leaf({{"k", encode_cell_owner(1, std::string(1024, 'v'))}}
        )
    });
    PageEncodeLimits limits;
    limits.frame_bytes        = 256;
    limits.inline_value_bytes = 16;
    limits.work_metrics       = metrics;
    auto prepared             = prepare_final_page(captured, 0, {}, limits);
    ASSERT_TRUE(prepared.status.ok());
    ASSERT_NE(prepared.encoder, nullptr);
    const auto frames = prepared.encoder->summary().new_overflow_frames;
    ASSERT_GT(frames, 1U);
    ASSERT_TRUE(prepared.encoder->bind_overflow({.first = 100, .count = frames, .owner = captured}).ok());
    std::array<uint8_t, 256> output{};
    for (uint64_t i = 0; i <= frames; ++i) {
        auto step = prepared.encoder->next(output);
        ASSERT_TRUE(step.status.ok());
        ASSERT_TRUE(step.frame.has_value());
        EXPECT_EQ(step.complete, i == frames);
    }
    const auto result = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)];
    EXPECT_EQ(result.attempts, frames + 1);
    EXPECT_EQ(result.count, frames + 1);
    EXPECT_EQ(result.outcomes[static_cast<size_t>(TreeOutcome::Success)], frames + 1);
    EXPECT_EQ(result.inflight, 0U);
}
} // namespace
} // namespace crowdb::tree
