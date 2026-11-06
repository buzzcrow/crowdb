// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/crowdb-tree.h"
#include "snapshot/published_native_collect.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestNativeCollect
{
    Config                      config;
    std::unique_ptr<Crowdbtree> tree;

    TestNativeCollect()
    {
        EXPECT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.frame_bytes      = 4096;
        config.max_inline_value = 64;
    }

    [[nodiscard]] Status open()
    {
        tree = std::make_unique<Crowdbtree>(config);
        return Status::Ok();
    }
};

TEST(PublishedNativeCollect, PublicCompatibilityPayloadsRemainChargedAfterTreeClose)
{
    TestNativeCollect test;
    auto              status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(12000, 'c');
    ASSERT_TRUE(test.tree->put("a", value).ok());
    std::vector<NativeFrame> frames;
    uint64_t                 root = 0;
    uint64_t                 slot = 0;
    uint64_t                 next = 0;
    status                        = test.tree->collect_native_frames(&frames, &root, &slot, &next);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(frames.empty());
    size_t capacity = 0;
    for (const auto &frame : frames) {
        EXPECT_TRUE(frame.allocation_owner);
        EXPECT_EQ(frame.durable_addr, kNoAddr);
        capacity += frame.frame.capacity();
    }
    test.tree.reset();
    EXPECT_GE(test.config.runtime->snapshot().allocated.routing_bytes, capacity);
    Config destination;
    destination.frame_bytes = 4096;
    Crowdbtree copy(destination);
    status = copy.install_snapshot_native(std::move(frames), root, slot, next);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::string actual;
    ASSERT_TRUE(copy.get("a", nullptr, &actual));
    EXPECT_EQ(actual, value);
}

TEST(PublishedNativeCollect, CapacityFailurePreservesEveryCallerOutput)
{
    TestNativeCollect test;
    auto              status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.tree->put("a", std::string(12000, 'b')).ok());
    auto                     core = TreeCoreAccess::get(*test.tree)->shared_from_this();
    std::vector<NativeFrame> frames;
    frames.push_back({
        .page_id = 987, .frame = {1, 2, 3},
             .durable_addr = kNoAddr, .durable_plen = 0, .inherited = false
    });
    NativeStreamInfo info{
        .root_page_id = 111, .at_slot = 222, .next_page_id = 333, .frame_count = 444, .frame_count_known = true};
    status = collect_published_frames(core, 1024, &frames, &info);
    EXPECT_EQ(status.code(), Code::kResourceExhausted);
    ASSERT_EQ(frames.size(), 1U);
    EXPECT_EQ(frames[0].page_id, 987U);
    EXPECT_EQ(frames[0].frame, (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_EQ(info.root_page_id, 111U);
    EXPECT_EQ(info.at_slot, 222U);
    EXPECT_EQ(info.next_page_id, 333U);
    EXPECT_EQ(info.frame_count, 444U);
}
} // namespace
} // namespace crowdb::tree
