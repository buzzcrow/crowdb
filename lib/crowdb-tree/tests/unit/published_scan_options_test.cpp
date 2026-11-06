// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "published_read_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

TEST(PublishedScanOptions, InclusiveReverseEmptyKeyIsARealBoundary)
{
    auto captured = view(page({
        {.key = "",  .value = "empty"},
        {.key = "a", .value = "a"    }
    }));
    for (bool inclusive : {false, true}) {
        std::shared_ptr<PublishedScan> scan;
        ASSERT_TRUE(PublishedScan::create(captured, {}, {}, true, {}, &scan,
                                          {.start = OwnedKey(""), .start_inclusive = inclusive})
                        .ok());
        const auto entries = collect(scan);
        EXPECT_EQ(entries.size(), inclusive ? 1U : 0U);
    }
}

TEST(PublishedScanOptions, KeysOnlyIncludesRequestedTombstoneWithinStrictBytes)
{
    auto                           captured = view(page({
        {.key = "a", .value = std::string(100, 'x')             },
        {.key = "b", .value = "",              .tombstone = true}
    }));
    std::shared_ptr<PublishedScan> scan;
    ASSERT_TRUE(PublishedScan::create(captured, {}, {}, false, {.max_result_bytes = 2}, &scan,
                                      {.keys_only = true, .include_tombstones = true})
                    .ok());
    const auto entries = collect(scan);
    ASSERT_EQ(entries.size(), 2U);
    EXPECT_TRUE(entries[0].cell.value.empty());
    EXPECT_TRUE(entries[1].cell.tombstone());
}

TEST(PublishedScanOptions, ExactEntryLimitReportsEndWithoutAnExtraWindow)
{
    auto                           captured = view(page({
        {.key = "a", .value = "value"}
    }));
    std::shared_ptr<PublishedScan> scan;
    ASSERT_TRUE(PublishedScan::create(captured, {}, {}, false, {.max_entries = 1}, &scan).ok());
    complete_inline(scan->next_sender());
    ASSERT_EQ(scan->result().entries.size(), 1U);
    EXPECT_TRUE(scan->result().done);
}

TEST(PublishedScanOptions, FullWindowProbeNeverLoadsTheNextOverflowValue)
{
    auto table = std::make_shared<MemTable>();
    ASSERT_TRUE(table->upsert("z", 2, encode_overflow_cell_buf(2, 10, 1024)));
    auto                           captured = view(page({
                                                       {.key = "a", .value = "value"}
    }),
                                                   {table});
    std::shared_ptr<PublishedScan> scan;
    // No loader is installed: materializing the lookahead value would fail.
    ASSERT_TRUE(PublishedScan::create(captured, {}, {}, false, {.max_entries = 1}, &scan).ok());
    complete_inline(scan->next_sender());
    ASSERT_EQ(scan->result().entries.size(), 1U);
    EXPECT_EQ(Slice(scan->result().entries[0].key).to_string(), "a");
    EXPECT_FALSE(scan->result().done);
}

TEST(PublishedScanOptions, StrictFirstOverflowIsTruncatedWithoutLoadingOrConsumingIt)
{
    auto table = std::make_shared<MemTable>();
    ASSERT_TRUE(table->upsert("a", 2, encode_overflow_cell_buf(2, 10, 128 * 1024)));
    auto                           captured = view(page({}), {table});
    std::shared_ptr<PublishedScan> scan;
    // A missing loader makes any attempted overflow materialization fail.
    ASSERT_TRUE(PublishedScan::create(captured, {}, {}, false, {.max_result_bytes = 1024}, &scan).ok());
    for (size_t attempt = 0; attempt < 2; ++attempt) {
        complete_inline(scan->next_sender());
        EXPECT_TRUE(scan->result().status.ok());
        EXPECT_TRUE(scan->result().entries.empty());
        EXPECT_TRUE(scan->result().byte_limit_reached);
        EXPECT_FALSE(scan->result().done);
    }
}

TEST(PublishedScanOptions, PublicBudgetSpansBoundedWindowsAndAllowsLargerSingleEntry)
{
    auto                           captured = view(page({
        {.key = "a", .value = "1234"},
        {.key = "b", .value = "1234"},
        {.key = "c", .value = std::string(20, 'v')},
        {.key = "d", .value = "1234"}
    }));
    std::shared_ptr<PublishedScan> scan;
    ASSERT_TRUE(PublishedScan::create(captured, {}, {}, false, {.max_entries = 2, .max_result_bytes = 10}, &scan,
                                      {.byte_budget = 36})
                    .ok());
    for (const size_t count : {size_t{2}, size_t{1}, size_t{1}}) {
        complete_inline(scan->next_sender());
        ASSERT_TRUE(scan->result().status.ok());
        EXPECT_EQ(scan->result().entries.size(), count);
        EXPECT_FALSE(scan->result().byte_limit_reached);
    }
    EXPECT_TRUE(scan->result().done);
}

TEST(PublishedReadBounds, RejectsOverflowedLimitsAndFundsDefaultOperations)
{
    PublishedReadLimits limits;
    ASSERT_TRUE(PublishedGet::allocation_bound(limits, 4));
    ASSERT_TRUE(PublishedScan::allocation_bound(limits, 4));
    limits.max_entries = SIZE_MAX;
    EXPECT_FALSE(PublishedScan::allocation_bound(limits, 4));
    PageLoadLimits loads;
    EXPECT_TRUE(AsyncStorePageLoader::read_reserve_bound(loads, 1, 4, 4, 1, loads.max_value_bytes));
    EXPECT_FALSE(AsyncStorePageLoader::read_reserve_bound(loads, 1, SIZE_MAX, 4, 1, 0));
}
} // namespace crowdb::tree
