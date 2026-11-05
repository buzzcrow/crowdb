// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/maptable/compressor.h"
#include "snapshot/native_layout.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestNativeLayout
{
    std::shared_ptr<PublishedTreeState>   view         = std::make_shared<PublishedTreeState>();
    std::shared_ptr<OverflowBackingSet>   dependencies = std::make_shared<OverflowBackingSet>(nullptr);
    std::vector<ReusedOverflowDependency> references;

    TestNativeLayout()
    {
        view->next_page_id = 8;
        add(1, kInvalidPageId, 100);
        references.push_back({.head = 1, .bytes = 100, .owner = dependencies, .backing = dependencies});
    }

    void add(uint64_t pid, uint64_t next, uint32_t bytes)
    {
        auto backing             = std::make_shared<BackingRef>();
        backing->backend_id      = 7;
        backing->generation      = 9;
        backing->address         = 8192 * pid;
        backing->length          = 8192;
        backing->owner           = std::make_shared<int>(0);
        dependencies->pages[pid] = {.backing       = std::move(backing),
                                    .next          = next,
                                    .bytes         = bytes,
                                    .frame_bytes   = 4096,
                                    .logical_bytes = 4096 + kDurableBlobHeader};
    }

    CapturedFrameResult::Plan page(uint64_t pid = 0, uint64_t fresh = 0)
    {
        return {.page_id         = pid,
                .summary         = {.new_overflow_frames = fresh},
                .reused_overflow = references,
                .owner           = dependencies};
    }

    static Status drain(NativeLayoutPlanner &planner)
    {
        while (planner.pending()) {
            auto status = planner.step();
            if (!status.ok()) {
                return status;
            }
        }
        return planner.finish();
    }
};

TEST(NativeLayout, DeduplicatesPhysicalChainsAndCountsNewFramesWithoutPayloads)
{
    TestNativeLayout test;
    test.references.push_back(test.references.front());
    NativeLayoutPlanner planner(test.view, {}, {}, 7, 9);
    ASSERT_TRUE(planner.accept(test.page(0, 3)).ok());
    auto status = TestNativeLayout::drain(planner);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(planner.frame_count(), 5U);
    EXPECT_EQ(planner.new_overflow_frames(), 3U);
    EXPECT_EQ(planner.old_frames().size(), 1U);
    EXPECT_EQ(planner.old_frames().at(1).backing, test.dependencies->pages.at(1).backing);
}

TEST(NativeLayout, RejectsMissingCycleLengthAndForeignDomainDescriptors)
{
    for (unsigned fault = 0; fault != 4; ++fault) {
        TestNativeLayout test;
        if (fault == 0) {
            test.dependencies->pages.clear();
        }
        if (fault == 1) {
            test.dependencies->pages.at(1).next = 1;
        }
        if (fault == 2) {
            test.references.front().bytes = 101;
        }
        if (fault == 3) {
            auto foreign = std::make_shared<BackingRef>(*test.dependencies->pages.at(1).backing);
            ++foreign->generation;
            test.dependencies->pages.at(1).backing = std::move(foreign);
        }
        NativeLayoutPlanner planner(test.view, {}, {}, 7, 9);
        ASSERT_TRUE(planner.accept(test.page()).ok());
        EXPECT_EQ(planner.step().code(), Code::kCorruption) << "fault=" << fault;
    }
}

TEST(NativeLayout, RejectsConflictingAliasesAndStopsAtTheDependencyStepLimit)
{
    TestNativeLayout test;
    test.add(2, kInvalidPageId, 50);
    test.dependencies->pages.at(1).next  = 2;
    test.dependencies->pages.at(1).bytes = 50;
    NativeLayoutPlanner bounded(test.view, {.max_frames = 8, .max_dependency_steps = 1}, {}, 7, 9);
    ASSERT_TRUE(bounded.accept(test.page()).ok());
    ASSERT_TRUE(bounded.step().ok());
    EXPECT_EQ(bounded.step().code(), Code::kResourceExhausted);

    NativeLayoutPlanner conflicting(test.view, {}, {}, 7, 9);
    ASSERT_TRUE(conflicting.accept(test.page()).ok());
    ASSERT_TRUE(TestNativeLayout::drain(conflicting).ok());
    auto changed = std::make_shared<BackingRef>(*test.dependencies->pages.at(1).backing);
    changed->address += 4096;
    test.dependencies->pages.at(1).backing = std::move(changed);
    ASSERT_TRUE(conflicting.accept(test.page(3)).ok());
    EXPECT_EQ(conflicting.step().code(), Code::kCorruption);
}

TEST(NativeLayout, BoundRejectsCountAndNamespaceOverflow)
{
    TestNativeLayout    test;
    NativeLayoutPlanner planner(test.view, {.max_frames = 1, .max_dependency_steps = 1}, {}, 7, 9);
    EXPECT_EQ(planner.accept(test.page(0, 1)).code(), Code::kResourceExhausted);
    EXPECT_EQ(planner.frame_count(), 0U);
    EXPECT_FALSE(NativeLayoutPlanner::allocation_bound(UINT64_MAX, {}));
    EXPECT_FALSE(NativeLayoutPlanner::allocation_bound(1, {.max_frames = UINT64_MAX}));
    EXPECT_FALSE(NativeLayoutPlanner::allocation_bound(1, {.max_frames = 0}));
}
} // namespace
} // namespace crowdb::tree
