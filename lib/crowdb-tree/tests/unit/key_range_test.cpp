// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/key_range.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>

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
