// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "published_read_test_support.h"
#include "snapshot/primary_backing.h"

namespace crowdb::tree
{
namespace
{
struct TestPrimary
{
    std::shared_ptr<PageState> leaf = std::make_shared<PageState>();
    PublishedPage              page;
    DurableCapturedFrame       frame;

    TestPrimary()
    {
        auto backing        = std::make_shared<BackingRef>();
        backing->backend_id = 7;
        backing->content_id = 5;
        backing->generation = 1;
        backing->length     = 4096;
        backing->owner      = std::make_shared<int>(1);
        leaf->bases.push_back({.identity = 5, .generation = 1, .max_slot = 10, .backing = backing});
        frame.page_id       = 3;
        frame.frame_bytes   = 4096;
        frame.logical_bytes = 4096;
        frame.backing       = std::move(backing);
        frame.summary.emplace();
        frame.summary->encoded_entries     = 1;
        frame.summary->new_overflow_frames = 2;
        frame.summary->dropped_tombstones  = 4;
        frame.stored_overflow              = std::make_shared<StoredOverflowReferences>();
        page.leaf                          = leaf;
        page.primary                       = make_primary_backing(frame, *leaf, 10, 2, {});
    }

    [[nodiscard]] bool eligible() const
    {
        return can_reuse_primary(page, 3, {}, 2, 7, 4096);
    }
};

TEST(PrimaryBacking, ReceiptForgetsOldWorkCountersAndRetainsOnlyItsPage)
{
    TestPrimary test;
    ASSERT_TRUE(test.page.primary);
    EXPECT_TRUE(test.eligible());
    const auto &reused   = test.page.primary->frame.summary;
    const auto &original = test.frame.summary;
    if (!reused || !original) {
        FAIL() << "primary summary is absent";
    }
    EXPECT_EQ(reused->new_overflow_frames, 0U);
    EXPECT_EQ(reused->dropped_tombstones, 0U);
    EXPECT_EQ(original->new_overflow_frames, 2U);
    EXPECT_EQ(test.page.primary->frame.backing, test.frame.backing);
}

TEST(PrimaryBacking, IdentityGeometryFloorAndSlotFilterMustMatch)
{
    TestPrimary test;
    EXPECT_FALSE(can_reuse_primary(test.page, 4, {}, 2, 7, 4096));
    EXPECT_FALSE(can_reuse_primary(test.page, 3, {}, 2, 8, 4096));
    EXPECT_FALSE(can_reuse_primary(test.page, 3, {}, 2, 0, 4096));
    EXPECT_FALSE(can_reuse_primary(test.page, 3, {}, 2, 7, 8192));
    EXPECT_FALSE(can_reuse_primary(test.page, 3, {}, 3, 7, 4096));
    test.leaf->bases.front().max_slot = 9;
    EXPECT_FALSE(test.eligible());
    test.leaf->bases.front().max_slot = 11;
    EXPECT_TRUE(test.eligible());
    test.leaf->bases.front().max_slot.reset();
    EXPECT_TRUE(test.eligible());
    test.leaf->bases.front().backing = std::make_shared<BackingRef>(*test.frame.backing);
    EXPECT_FALSE(test.eligible());
}

TEST(PrimaryBacking, LaterOverlaySiblingAndRoutingSliceInvalidateReuse)
{
    TestPrimary test;
    test.leaf->overlays = test_read::page({
                                              {.key = "a", .value = "later"}
    })
                              ->overlays;
    EXPECT_FALSE(test.eligible());
    test.leaf->overlays.clear();
    test.leaf->right_sibling = 8;
    EXPECT_FALSE(test.eligible());
    test.leaf->right_sibling             = kInvalidPageId;
    test.leaf->bases.front().range.lower = OwnedKey("a");
    EXPECT_FALSE(test.eligible());
    test.leaf->bases.front().range.lower.reset();
    PageKeyRange narrowed;
    narrowed.upper = OwnedKey("z");
    EXPECT_FALSE(can_reuse_primary(test.page, 3, narrowed, 2, 7, 4096));
    EXPECT_TRUE(test.eligible());
}

TEST(PrimaryBacking, AdvancedFloorReusesOnlyACompleteLiveOnlyReceipt)
{
    TestPrimary test;
    if (!test.frame.summary) {
        FAIL() << "missing frame summary";
    }
    test.frame.summary->live_counts_known = true;
    test.frame.summary->live_keys         = test.frame.summary->encoded_entries;
    test.page.primary                     = make_primary_backing(test.frame, *test.leaf, 10, 2, {});
    EXPECT_TRUE(can_reuse_primary(test.page, 3, {}, 10, 7, 4096));
    EXPECT_FALSE(can_reuse_primary(test.page, 3, {}, 1, 7, 4096));

    test.frame.summary->encoded_entries++;
    test.page.primary = make_primary_backing(test.frame, *test.leaf, 10, 2, {});
    EXPECT_FALSE(can_reuse_primary(test.page, 3, {}, 10, 7, 4096));
    EXPECT_TRUE(test.eligible());

    test.frame.summary->encoded_entries--;
    test.frame.summary->live_counts_known = false;
    test.page.primary                     = make_primary_backing(test.frame, *test.leaf, 10, 2, {});
    EXPECT_FALSE(can_reuse_primary(test.page, 3, {}, 10, 7, 4096));
}

TEST(PrimaryBacking, MissingStoredReferenceProofCannotBeInvented)
{
    TestPrimary test;
    test.frame.stored_overflow.reset();
    EXPECT_FALSE(make_primary_backing(test.frame, *test.leaf, 10, 2, {}));
    test.frame.stored_overflow = std::make_shared<StoredOverflowReferences>();
    test.frame.summary.reset();
    EXPECT_FALSE(make_primary_backing(test.frame, *test.leaf, 10, 2, {}));
}

TEST(PrimaryBacking, InnerRequiresUnchangedRouteAndCapturedDescendantFences)
{
    TestPrimary test;
    auto        inner = std::make_shared<InnerPageState>();
    inner->children   = {1, 2};
    inner->separators.emplace_back("middle");
    test.page.leaf.reset();
    test.page.level         = 1;
    test.page.inner         = inner;
    test.page.inner_backing = test.frame.backing;
    test.frame.type         = page_type::kInnerBase;
    test.frame.stored_overflow.reset();
    auto &summary = test.frame.summary;
    if (!summary) {
        FAIL() << "primary summary is absent";
    }
    summary->inner_fences = {.lower_leaf = 1, .upper_leaf = 2};
    test.page.primary     = make_inner_primary_backing(test.frame, test.page, 10, 2, {});
    ASSERT_TRUE(test.page.primary);
    EXPECT_TRUE(can_reuse_inner_primary(test.page, 3, {1, 2}, 7, 4096));
    EXPECT_FALSE(can_reuse_inner_primary(test.page, 3, {2, 2}, 7, 4096));
    EXPECT_FALSE(can_reuse_inner_primary(test.page, 3, {}, 7, 4096));
    test.page.inner = std::make_shared<InnerPageState>(*inner);
    EXPECT_FALSE(can_reuse_inner_primary(test.page, 3, {1, 2}, 7, 4096));
    auto decoded         = std::make_shared<InnerPageState>(*inner);
    decoded->frame_proof = {.self_page_id   = 3,
                            .frame_bytes    = 4096,
                            .has_fences     = true,
                            .page_id_fences = true,
                            .lower          = {},
                            .upper          = {},
                            .lower_leaf     = 1,
                            .upper_leaf     = 2};
    test.page.inner      = decoded;
    inner.reset();
    EXPECT_TRUE(test.page.primary->inner_route.expired());
    EXPECT_TRUE(can_reuse_inner_primary(test.page, 3, {1, 2}, 7, 4096));
    decoded->frame_proof->self_page_id = 4;
    EXPECT_FALSE(can_reuse_inner_primary(test.page, 3, {1, 2}, 7, 4096));
}
} // namespace
} // namespace crowdb::tree
