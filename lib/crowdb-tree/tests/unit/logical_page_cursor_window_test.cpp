// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_publication.h"
#include "btree/page_maintenance.h"
#include "published_read_test_support.h"

namespace crowdb::tree
{
namespace
{
TEST(LogicalPageCursorWindow, ManyRunsKeepConstantCursorStorageAndMatchForwardReverseFrontiers)
{
    auto page = std::make_shared<PageState>();
    for (uint64_t slot = 1; slot <= 129; ++slot) {
        const auto run = test_read::page({
            {.key = "b", .slot = slot, .value = std::to_string(slot)},
            {.key = "c", .slot = slot, .value = "later"             }
        });
        page->overlays.push_back(run->overlays.front());
    }
    page->bases.push_back({
        .identity = 1,
        .resident =
            test_read::base({{.key = "a", .slot = 1, .value = "base"}, {.key = "d", .slot = 1, .value = "base"}}
            )
    });
    detail::ResourceBudget::Limits limits;
    const auto                     bytes = LogicalPageCursor::allocation_bound(1).value_or(0) +
                                           detail::shared_allocation_bound<detail::AllocationReservation>();
    limits.fill(bytes);
    detail::ResourceBudget budget(limits, 1);
    auto                   account = budget.make_account(limits);
    for (bool reverse : {false, true}) {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, bytes);
        if (!credit) {
            FAIL() << "cursor credit unavailable";
        }
        auto              metadata = detail::AllocationReservation::create(std::move(*credit));
        LogicalPageCursor cursor(page, {}, 64, 1, reverse, metadata);
        ASSERT_TRUE(cursor.status().ok()) << cursor.status().to_string();
        std::vector<std::string> keys;
        while (cursor.valid()) {
            keys.push_back(cursor.key().to_string());
            if (cursor.key() == Slice("b")) {
                EXPECT_EQ(cursor.cell().slot, 64U);
                EXPECT_EQ(cursor.cell().value.to_string(), "64");
            }
            cursor.next();
        }
        EXPECT_EQ(keys, (reverse ? std::vector<std::string>{"d", "c", "b", "a"}
                                 : std::vector<std::string>{"a", "b", "c", "d"}));
    }
}

TEST(LogicalPageCursorWindow, PartialSplitRetainsAnOversizedQueryableRemainder)
{
    auto page = std::make_shared<PageState>();
    for (int index = 0; index < 40; ++index) {
        const auto run = test_read::page({
            {.key = "key" + std::to_string(100 + index), .slot = 1, .value = std::string(40, 'v')}
        });
        page->overlays.push_back(run->overlays.front());
    }
    auto published          = std::make_shared<PublishedTreeState>(*test_read::view(page));
    published->next_page_id = 1;
    MaintenancePublicationOptions options;
    options.pages.max_sources        = 2;
    options.pages.max_work_entries   = 8;
    options.pages.primary_page_bytes = 256;
    options.pages.target_page_bytes  = 220;
    options.pages.inline_value_bytes = 64;
    auto record = std::make_shared<InstallRecord>(InstallToken{.generation = published->generation});
    auto result = build_split_step_publication(published, 0, record, options);
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_GT(result.output_leaf_pids.size(), 1U);
    size_t keys = 0;
    for (const auto &change : result.transaction.pages) {
        if (!change.replacement || !change.replacement->leaf) {
            continue;
        }
        LogicalPageCursor cursor(change.replacement->leaf, {}, UINT64_MAX, 2);
        ASSERT_TRUE(cursor.status().ok());
        while (cursor.valid()) {
            ++keys;
            EXPECT_EQ(cursor.cell().value.to_string(), std::string(40, 'v'));
            cursor.next();
        }
    }
    EXPECT_EQ(keys, 40U);
}
} // namespace
} // namespace crowdb::tree
