// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_route.h"
#include "published_read_test_support.h"

namespace crowdb::tree
{
namespace
{
std::shared_ptr<PublishedTreeState> routing_tree()
{
    auto tree         = std::make_shared<PublishedTreeState>();
    auto root         = std::make_shared<PublishedPage>();
    auto inner        = std::make_shared<InnerPageState>();
    inner->children   = {1, 2, 3};
    inner->separators = {"m", "t"};
    root->level       = 1;
    root->inner       = inner;
    auto middle       = test_read::published(test_read::page(
        {
            {.key = "n", .value = "kept"}
    },
        {.lower = "m", .upper = "t"}));
    // Omitted children deliberately have no mapping descriptor: a bounded
    // route must skip their roots without loading or inspecting descendants.
    const std::array<PageReplacement, 2> replacements{
        {{.page_id = 0, .expected = {}, .replacement = root}, {.page_id = 2, .expected = {}, .replacement = middle}}
    };
    MappingView::CopyStats copied;
    tree->mapping      = tree->mapping.replace(replacements, copied);
    tree->root_page_id = 0;
    return tree;
}

TEST(PublishedRoutePruning, CountsExcludedRootsWithoutTouchingTheirDescendants)
{
    const auto tree = routing_tree();
    for (bool reverse : {false, true}) {
        PublishedRouteCursor cursor(0, tree->mapping, 2, reverse, {}, {.lower = "m", .upper = "t"});
        const auto           status = cursor.seek(reverse ? Slice("t") : Slice("m"), reverse);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(cursor.leaf_page_id(), 2U);
        EXPECT_EQ(cursor.skipped_subtrees(), 1U);
        ASSERT_TRUE(cursor.next().ok());
        EXPECT_FALSE(cursor.leaf());
        EXPECT_EQ(cursor.skipped_subtrees(), 2U);
    }
}

TEST(PublishedRoutePruning, EmptyRangeSkipsTheRootWithoutReadingItsIndex)
{
    const auto           tree = routing_tree();
    PublishedRouteCursor cursor(0, tree->mapping, 2, false, {}, {.lower = "m", .upper = "m"});
    ASSERT_TRUE(cursor.seek(Slice("m"), false).ok());
    EXPECT_FALSE(cursor.leaf());
    EXPECT_FALSE(cursor.needs_inner());
    EXPECT_EQ(cursor.skipped_subtrees(), 1U);
}
} // namespace
} // namespace crowdb::tree
