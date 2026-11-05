// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_cache.h"
#include "inner_load_test_support.h"

#include <array>

namespace crowdb::tree
{
namespace
{
using namespace test_read;

struct TestTrim
{
    size_t        count = SIZE_MAX;
    PageCacheTrim request;

    TestTrim(PageCacheKind kind, size_t retained)
        : request{.kind      = kind,
                  .max_pages = retained,
                  .complete  = [](void  *context,
                                  size_t evicted) noexcept { static_cast<TestTrim *>(context)->count = evicted; },
                  .context   = this}
    {
    }
};

class TestTrimLoader final : public PageLoader
{
  public:
    std::shared_ptr<const LeafBase> leaf    = base({
        {.key = "key", .value = "value"}
    });
    std::shared_ptr<InnerPageState> inner   = std::make_shared<InnerPageState>();
    size_t                          leaves  = 0;
    size_t                          indexes = 0;

    TestTrimLoader()
    {
        inner->children.push_back(10);
    }

    void load_leaf(BaseRef /*base*/, LeafComplete complete) override
    {
        ++leaves;
        complete(Status::Ok(), leaf);
    }

    void load_inner(std::shared_ptr<const BackingRef> /*backing*/, InnerComplete complete) override
    {
        ++indexes;
        complete(Status::Ok(), inner);
    }
};

TEST(PageCacheTrim, LeafAndInnerBudgetsAreIndependentAndKeepRecentPinsValid)
{
    SharedPageCache                 cache({.max_pages = 8, .max_waiters = 4});
    auto                            upstream = std::make_shared<TestTrimLoader>();
    auto                            loader   = cache.bind(upstream);
    const std::array<BaseRef, 4>    frames{cold(1), cold(2), cold(3), cold(4)};
    std::shared_ptr<const LeafBase> pinned;
    auto                            leaf = [&](uint64_t pid) {
        loader->load_leaf(frames.at(pid - 1), [&](Status status, auto value) {
            ASSERT_TRUE(status.ok());
            pinned = std::move(value);
        });
    };
    auto inner = [&](uint64_t pid) {
        loader->load_inner(frames.at(pid - 1).backing, [](Status status, auto value) {
            ASSERT_TRUE(status.ok());
            ASSERT_TRUE(value);
        });
    };
    leaf(1);
    leaf(2);
    inner(3);
    inner(4);
    leaf(1);
    ASSERT_EQ(cache.snapshot().resident_pages, 4U);
    TestTrim leaves(PageCacheKind::Leaf, 1);
    cache.trim(leaves.request);
    EXPECT_EQ(leaves.count, 1U);
    EXPECT_EQ(cache.snapshot().resident_pages, 3U);
    leaf(1);
    inner(3);
    inner(4);
    EXPECT_EQ(upstream->leaves, 2U);
    EXPECT_EQ(upstream->indexes, 2U);
    TestTrim indexes(PageCacheKind::Inner, 0);
    cache.trim(indexes.request);
    EXPECT_EQ(indexes.count, 2U);
    EXPECT_EQ(cache.snapshot().resident_pages, 1U);
    EXPECT_EQ(CellView(pinned->cell(0)).value().to_string(), "value");
    leaf(2);
    EXPECT_EQ(upstream->leaves, 3U);
}

TEST(PageCacheTrim, ReentrantCompletionQueuesTrimWithoutBlockingTheLoadOwner)
{
    SharedPageCache cache;
    auto            upstream = std::make_shared<TestTrimLoader>();
    auto            loader   = cache.bind(upstream);
    TestTrim        trim(PageCacheKind::Leaf, 0);
    loader->load_leaf(cold(1), [&](Status status, auto leaf) {
        ASSERT_TRUE(status.ok());
        cache.trim(trim.request);
        EXPECT_EQ(trim.count, SIZE_MAX);
        EXPECT_EQ(CellView(leaf->cell(0)).value().to_string(), "value");
    });
    EXPECT_EQ(trim.count, 1U);
    EXPECT_EQ(cache.snapshot().resident_pages, 0U);
    EXPECT_EQ(cache.snapshot().waiters, 0U);
}
} // namespace
} // namespace crowdb::tree
