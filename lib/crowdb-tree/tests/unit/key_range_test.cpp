// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/btree/key_range.h"
#include "crowdb-tree/maptable/frame_page.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

using namespace crowdb::tree;

TEST(KeyRange, EndpointFormsUseHalfOpenOrdering)
{
    KeyRange all = KeyRange::unbounded();
    EXPECT_TRUE(all.contains(Slice("")));
    EXPECT_TRUE(all.contains(Slice("z")));

    KeyRange bounded = KeyRange::bounded(std::string("b"), std::string("d"));
    ASSERT_TRUE(bounded.validate().ok());
    EXPECT_FALSE(bounded.contains(Slice("a")));
    EXPECT_TRUE(bounded.contains(Slice("b")));
    EXPECT_TRUE(bounded.contains(Slice("c")));
    EXPECT_FALSE(bounded.contains(Slice("d")));

    KeyRange min_open = KeyRange::bounded(std::nullopt, std::string("b"));
    EXPECT_TRUE(min_open.contains(Slice("")));
    EXPECT_FALSE(min_open.contains(Slice("b")));
    KeyRange max_open = KeyRange::bounded(std::string("b"), std::nullopt);
    EXPECT_TRUE(max_open.contains(Slice("z")));
}

TEST(KeyRange, EmptyAndInvalidRangesAreDistinct)
{
    KeyRange empty = KeyRange::bounded(std::string("m"), std::string("m"));
    ASSERT_TRUE(empty.validate().ok());
    EXPECT_FALSE(empty.contains(Slice("m")));
    EXPECT_FALSE(empty.contains(Slice("l")));

    KeyRange invalid = KeyRange::bounded(std::string("z"), std::string("a"));
    EXPECT_EQ(invalid.validate().code(), Code::kInvalidArgument);
}

TEST(KeyRange, ChecksummedFramesMustFitTheConfiguredRange)
{
    std::vector<uint8_t> frame(4096);
    LeafFrameBuilder     leaf(frame.data(), frame.size());
    buffer               inside  = encode_cell_buf(1, OpKind::kPut, Slice("v"));
    buffer               outside = encode_cell_buf(2, OpKind::kPut, Slice("v"));
    ASSERT_TRUE(leaf.try_append_sorted(Slice("b"), inside.slice()));
    ASSERT_TRUE(leaf.try_append_sorted(Slice("z"), outside.slice()));
    leaf.finish(1, kInvalidPageId);
    ASSERT_TRUE(frame_validate(frame.data(), frame.size()));
    EXPECT_FALSE(
        frame_validate_key_range(frame.data(), frame.size(), KeyRange::bounded(std::string("a"), std::string("m"))));
    EXPECT_TRUE(frame_validate_key_range(frame.data(), frame.size(), KeyRange::unbounded()));
}
