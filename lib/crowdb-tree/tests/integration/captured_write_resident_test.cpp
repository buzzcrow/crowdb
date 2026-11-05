// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/overflow_backing.h"
#include "captured_write_test_support.h"

namespace crowdb::tree
{
namespace
{
std::shared_ptr<PublishedTreeState> shared_resident_chain()
{
    auto overflow = std::make_shared<OverflowBackingSet>(nullptr);
    for (uint64_t pid = 1; pid <= 2; ++pid) {
        auto              bytes = std::make_shared<std::vector<uint8_t>>(512);
        const std::string value(300, 'v');
        const auto        next = pid == 1 ? 2 : kInvalidPageId;
        overflow_frame_build(bytes->data(), bytes->size(), pid, next, reinterpret_cast<const uint8_t *>(value.data()),
                             value.size());
        auto backing            = std::make_shared<BackingRef>();
        backing->owner          = bytes;
        backing->storage        = BackingStorage::kResidentFrame;
        backing->resident_frame = *bytes;
        overflow->pages.emplace(
            pid,
            OverflowBackingPage{
                .backing = std::move(backing), .next = next, .bytes = 300, .frame_bytes = 512, .logical_bytes = 0});
    }
    auto             raw  = std::make_shared<std::vector<uint8_t>>(512);
    const auto       cell = encode_overflow_cell_buf(10, 1, 600);
    LeafFrameBuilder builder(raw->data(), raw->size());
    EXPECT_TRUE(builder.try_append_sorted("a", Slice(cell)));
    EXPECT_TRUE(builder.try_append_sorted("b", Slice(cell)));
    builder.finish(0, kInvalidPageId);
    auto backing            = std::make_shared<BackingRef>();
    backing->owner          = raw;
    backing->storage        = BackingStorage::kResidentFrame;
    backing->resident_frame = *raw;
    backing->overflow       = std::move(overflow);
    auto leaf               = std::make_shared<PageState>();
    leaf->bases.push_back(
        {.identity   = 1,
         .generation = 1,
         .max_slot   = 10,
         .resident   = std::shared_ptr<const LeafBase>(LeafBase::from_frame_copy(raw->data(), raw->size())),
         .backing    = std::move(backing)});
    auto captured          = test_write::captured_leaf(std::move(leaf));
    captured->next_page_id = 3;
    return captured;
}

TEST(CapturedWriteResident, DuplicateReferencesShareWritesAndReleaseEveryExtent)
{
    test_write::TestCapturedWrite test;
    auto                          captured = shared_resident_chain();
    std::weak_ptr<const void>     raw_owner =
        captured->mapping.get(0)->leaf->bases.front().backing->overflow->pages.at(1).backing->owner;
    ASSERT_TRUE(test.prepare(captured).ok());
    test_write::Completion done;
    test.run(&done);
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    auto pages = test.operation->result().pages;
    ASSERT_NE(pages, nullptr);
    EXPECT_EQ(test.operation->result().written_frames, 3U);
    EXPECT_EQ(pages->frames.size(), 3U);
    EXPECT_EQ(pages->frames.at(0).backing->overflow->pages.size(), 2U);
    auto retained = pages->frames.at(0).backing;
    test.operation.reset();
    captured.reset();
    pages.reset();
    EXPECT_TRUE(raw_owner.expired());
    EXPECT_EQ(test.ledger->stats().protected_ranges, 3U);
    retained.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[test_write::LoadCredits::kBytes], 0U);
}
} // namespace
} // namespace crowdb::tree
