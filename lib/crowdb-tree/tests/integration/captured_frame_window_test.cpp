// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "load_budget_test_support.h"
#include "published_read_test_support.h"
#include "snapshot/captured_frames.h"

namespace crowdb::tree
{
namespace
{
using namespace test_read;

TEST(CapturedFrameWindow, WarmAndColdOversizedLeavesUseExactPrepaidDescriptorBound)
{
    for (bool cold_base : {false, true}) {
        auto logical = std::make_shared<PageState>();
        if (cold_base) {
            logical->bases.push_back(cold(1));
        }
        const auto run = page({
                                  {.key = "z", .value = "overlay"}
        })
                             ->overlays.front();
        logical->overlays.assign(8192, run);
        auto captured          = std::make_shared<PublishedTreeState>(*view(logical, {}, 10));
        captured->next_page_id = 1;
        LoadCredits credits;
        credits.credits.reservation()->seal();
        CapturedFrameLimits limits;
        limits.max_depth                 = 1;
        limits.page_metadata_bytes       = captured->mapping.copy_bounds().max_leaf_bytes;
        limits.encode.frame_bytes        = 512;
        limits.encode.max_sources        = 1;
        limits.encode.max_entries        = 2;
        limits.encode.max_work_entries   = 8;
        limits.encode.max_metadata_bytes = 4096;
        const auto bound                 = CapturedFrameStream::allocation_bound(limits);
        if (!bound) {
            FAIL() << "captured descriptor allocation bound overflow";
        }
        limits.encode.metadata                       = credits.reserve(detail::BudgetClass::kRouting, *bound);
        auto loader                                  = std::make_shared<TestLoader>();
        loader->frames[1]                            = base({
            {.key = "a", .value = "cold"}
        });
        auto                                 runtime = std::make_shared<detail::FlushRuntime>(1, 8);
        auto                                 lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
        std::shared_ptr<CapturedFrameStream> stream;
        ASSERT_TRUE(CapturedFrameStream::create(captured, loader, runtime, lane, limits, {}, &stream).ok());
        Completion done;
        auto       operation = stdexec::connect(stream->sender(), Receiver{.done = &done});
        stdexec::start(operation);
        runtime->drain();
        ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
        const auto &frame = stream->result().frame;
        if (!frame) {
            FAIL() << "snapshot did not emit a primary frame";
        }
        const auto &summary = frame->summary;
        if (!summary) {
            FAIL() << "primary frame lacks its final summary";
        }
        EXPECT_EQ(summary->encoded_entries, cold_base ? 2U : 1U);
        EXPECT_EQ(loader->calls, cold_base ? 1U : 0U);
        EXPECT_TRUE(stream->result().complete);
    }
}

TEST(CapturedFrameWindow, DescriptorGeometryOverflowRejectsBeforeAllocation)
{
    CapturedFrameLimits limits;
    limits.page_metadata_bytes = SIZE_MAX;
    EXPECT_FALSE(CapturedFrameStream::allocation_bound(limits));
    limits.page_metadata_bytes = 0;
    limits.encode.max_sources  = SIZE_MAX;
    EXPECT_FALSE(CapturedFrameStream::allocation_bound(limits));
}
} // namespace
} // namespace crowdb::tree
