// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(DurablePageSegments, ChangingOneSegmentPreservesTheOtherSegmentAndOldReader)
{
    ReadablePageFrames original;
    original.emplace(0, {.page_id = 0, .backing = nullptr});
    original.emplace(1, {.page_id = 1, .backing = nullptr});
    original.emplace(kMappingSegmentSize,
                     {.page_id = kMappingSegmentSize, .type = page_type::kInnerBase, .backing = nullptr});
    ReadablePageFrames current;
    current.inherit(original);
    ASSERT_TRUE(current.same_segment(original, 0));
    ASSERT_TRUE(current.same_segment(original, 1));
    current.erase(1);
    current.emplace(2, {.page_id = 2, .backing = nullptr});
    EXPECT_FALSE(current.same_segment(original, 0));
    EXPECT_TRUE(current.same_segment(original, 1));
    EXPECT_TRUE(original.contains(1));
    EXPECT_FALSE(original.contains(2));
    EXPECT_FALSE(current.contains(1));
    EXPECT_TRUE(current.contains(2));
    EXPECT_EQ(current.primary_counts(0), std::make_pair(size_t{2}, size_t{0}));
    EXPECT_EQ(current.primary_counts(1), std::make_pair(size_t{0}, size_t{1}));
    EXPECT_EQ(current.lower_bound(3)->first, kMappingSegmentSize);
    EXPECT_THROW(current.at(3), std::out_of_range);
}

std::shared_ptr<const StoredOverflowReferences> heads(uint64_t pid)
{
    auto result = std::make_shared<StoredOverflowReferences>();
    result->push_back({.head = pid, .bytes = 10});
    return result;
}

TEST(DurablePageSegments, ReplacingSharedOverflowDropsOnlyUnreachableSuffixes)
{
    CapturedDurablePages original(nullptr);
    original.frames.emplace(0, {.page_id = 0, .backing = nullptr, .stored_overflow = heads(3)});
    original.frames.emplace(1, {.page_id = 1, .backing = nullptr, .stored_overflow = heads(4)});
    original.frames.emplace(3,
                            {.page_id = 3, .type = page_type::kOverflowFrame, .next_overflow = 5, .backing = nullptr});
    original.frames.emplace(4,
                            {.page_id = 4, .type = page_type::kOverflowFrame, .next_overflow = 5, .backing = nullptr});
    original.frames.emplace(5, {.page_id = 5, .type = page_type::kOverflowFrame, .backing = nullptr});
    ASSERT_TRUE(original.initialize_overflow_counts().ok());
    CapturedDurablePages current(nullptr);
    current.frames.inherit(original.frames);
    current.overflow_counts_known  = true;
    current.stored_reference_count = original.stored_reference_count;
    CapturedDurablePages replacement(nullptr);
    replacement.frames.emplace(0, {.page_id = 0, .backing = nullptr});
    ASSERT_TRUE(current.replace_primary(replacement).ok());
    EXPECT_FALSE(current.frames.contains(3));
    EXPECT_TRUE(current.frames.contains(4));
    EXPECT_TRUE(current.frames.contains(5));
    EXPECT_EQ(std::as_const(current.frames).at(5).incoming_references, 1U);
    ASSERT_TRUE(current.erase_primary(1).ok());
    EXPECT_EQ(current.frames.size(), 1U);
    EXPECT_TRUE(current.frames.contains(0));
    EXPECT_EQ(original.frames.size(), 5U);
    EXPECT_EQ(std::as_const(original.frames).at(5).incoming_references, 2U);
}

TEST(DurablePageSegments, SharingAnExistingHeadKeepsItsDependencyOwner)
{
    CapturedDurablePages pages(nullptr);
    pages.frames.emplace(0, {.page_id = 0, .backing = nullptr, .stored_overflow = heads(2)});
    pages.frames.emplace(2, {.page_id = 2, .type = page_type::kOverflowFrame, .backing = nullptr});
    ASSERT_TRUE(pages.initialize_overflow_counts().ok());
    CapturedDurablePages replacement(nullptr);
    replacement.frames.emplace(1, {.page_id = 1, .backing = nullptr, .stored_overflow = heads(2)});
    replacement.frames.emplace(2, {.page_id = 2, .type = page_type::kOverflowFrame, .backing = nullptr});
    ASSERT_TRUE(pages.replace_primary(replacement).ok());
    EXPECT_EQ(std::as_const(pages.frames).at(2).incoming_references, 2U);
    ASSERT_TRUE(pages.erase_primary(0).ok());
    EXPECT_EQ(std::as_const(pages.frames).at(2).incoming_references, 1U);
    EXPECT_TRUE(pages.frames.contains(1));
}
} // namespace
} // namespace crowdb::tree
