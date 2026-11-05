// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/maptable/frame_page.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(NativeImportExport, ImportedResidentOverflowExportsAfterTreeClosesWithoutABackend)
{
    Config source_config;
    source_config.frame_bytes      = 4096;
    source_config.max_inline_value = 64;
    Crowdbtree        source(source_config);
    const std::string value(16000, 'i');
    ASSERT_TRUE(source.put("a", value).ok());
    ASSERT_TRUE(source.flush().ok());
    std::vector<NativeFrame> original;
    uint64_t                 root   = 0;
    uint64_t                 slot   = 0;
    uint64_t                 next   = 0;
    auto                     status = source.collect_native_frames(&original, &root, &slot, &next);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_GT(original.size(), 1U);
    ASSERT_EQ(frame_page_type(original[1].frame.data()), page_type::kOverflowFrame);
    Config target_config;
    target_config.frame_bytes      = 4096;
    target_config.max_inline_value = 64;
    ASSERT_TRUE(TreeRuntime::create({}, &target_config.runtime).ok());
    auto target = std::make_unique<Crowdbtree>(target_config);
    status      = target->install_snapshot_native(std::move(original), root, slot, next);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::unique_ptr<NativeFrameIterator> cursor;
    status = target->open_native_frame_iterator(nullptr, &cursor);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto info = cursor->stream_info();
    target.reset();
    std::vector<NativeFrame>     frames;
    std::vector<NativeFrameView> retained;
    bool                         done = false;
    while (!done) {
        std::optional<NativeFrameView> frame;
        status = cursor->next_owned(&frame, &done);
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (!frame) {
            continue;
        }
        EXPECT_EQ(frame->durable_addr, kNoAddr);
        EXPECT_FALSE(frame->inherited);
        frames.push_back({.page_id      = frame->page_id,
                          .frame        = std::vector<uint8_t>(frame->bytes.begin(), frame->bytes.end()),
                          .durable_addr = kNoAddr,
                          .durable_plen = 0,
                          .inherited    = false});
        retained.push_back(*frame);
    }
    EXPECT_EQ(frames.size(), info.frame_count);
    cursor.reset();
    for (const auto &frame : retained) {
        EXPECT_TRUE(frame_validate(frame.bytes.data(), frame.bytes.size()));
    }
    Crowdbtree roundtrip(source_config);
    status = roundtrip.install_snapshot_native(std::move(frames), info.root_page_id, info.at_slot, info.next_page_id);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::string actual;
    ASSERT_TRUE(roundtrip.get("a", nullptr, &actual));
    EXPECT_EQ(actual, value);
}
} // namespace crowdb::tree
