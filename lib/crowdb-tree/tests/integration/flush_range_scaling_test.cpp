// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_batch.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
void seed_wide_tree(PublicationCoordinator &publisher, const std::shared_ptr<MemTable> &source, size_t leaves)
{
    auto                                                      request = std::make_shared<PublicationRequest>();
    std::vector<std::pair<uint64_t, std::optional<OwnedKey>>> level;
    for (size_t index = 0; index < leaves; ++index) {
        const auto key = "k" + std::to_string(10000 + index);
        ASSERT_TRUE(source->upsert(key, 1, encode_cell_buf(1, OpKind::kPut, "value")));
        auto leaf = std::make_shared<PageState>();
        if (index != 0) {
            leaf->range.lower = key;
        }
        if (index + 1 != leaves) {
            leaf->range.upper   = "k" + std::to_string(10001 + index);
            leaf->right_sibling = index + 1;
        }
        auto page  = std::make_shared<PublishedPage>();
        page->leaf = leaf;
        level.emplace_back(index, leaf->range.lower);
        request->transaction.pages.push_back({.page_id = index, .replacement = page});
    }
    uint64_t next_pid = leaves;
    uint32_t depth    = 0;
    while (level.size() > 1) {
        ++depth;
        std::vector<std::pair<uint64_t, std::optional<OwnedKey>>> parents;
        for (size_t first = 0; first < level.size(); first += 64) {
            auto inner = std::make_shared<InnerPageState>();
            for (size_t child = first; child < std::min(first + 64, level.size()); ++child) {
                inner->children.push_back(level[child].first);
                if (child != first) {
                    const auto &lower = level[child].second;
                    if (!lower) {
                        FAIL() << "a nonfirst child needs a lower separator";
                    }
                    inner->separators.push_back(*lower);
                }
            }
            auto page          = std::make_shared<PublishedPage>();
            page->level        = depth;
            page->metrics.leaf = false;
            page->inner        = std::move(inner);
            parents.emplace_back(next_pid, level[first].second);
            request->transaction.pages.push_back({.page_id = next_pid++, .replacement = page});
        }
        level = std::move(parents);
    }
    source->close();
    auto inputs = std::make_shared<PublishedSources>();
    inputs->tables.push_back(source);
    request->transaction.record                = std::make_shared<InstallRecord>(InstallToken{});
    request->transaction.root_page_id          = level.front().first;
    request->transaction.expected_next_page_id = publisher.capture()->next_page_id;
    request->transaction.next_page_id          = next_pid;
    request->transaction.expected_sources      = publisher.capture()->sources;
    request->transaction.sources               = inputs;
    request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()) << result.status.to_string(); };
    ASSERT_TRUE(publisher.try_submit(request));
}

TEST(FlushRangeScaling, PrepaidHandoffCrosses4096LeavesWithoutATreeSizeLimit)
{
    constexpr size_t       leaves = 4097;
    detail::FlushRuntime   runtime(4, 32);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(32);
    PublicationCoordinator publisher;
    auto                   source = std::make_shared<MemTable>();
    seed_wide_tree(publisher, source, leaves);
    const auto view = publisher.capture();
    ASSERT_EQ(view->mapping.copy_bounds().leaf_pages, leaves);
    FlushBatchOptions options;
    // Admission includes future writers, so its range bound may exceed the
    // currently linked entries as well as the old 4096-range ceiling.
    const auto bound = FlushBatch::metadata_reserve_bound(view->mapping, 1, 8192, 8192 * 6, 4, options);
    if (!bound) {
        FAIL() << "the finite range metadata bound must fit size_t";
    }
    detail::ResourceBudget::Limits limits{};
    limits[static_cast<size_t>(detail::BudgetClass::kHandoff)] = *bound;
    detail::ResourceBudget budget(limits, 4);
    auto                   account = budget.make_account(limits);
    auto                   credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, *bound);
    if (!credit) {
        FAIL() << "the exact prepaid handoff grant must fit: " << *bound;
    }
    auto                        metadata = detail::AllocationReservation::create(std::move(*credit));
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch, {}, metadata).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    ASSERT_TRUE(batch->result().handoff_status.ok()) << batch->result().handoff_status.to_string();
    EXPECT_EQ(publisher.capture()->coverage, 1U);
    EXPECT_TRUE(publisher.capture()->sources->tables.empty());
    for (size_t index = 0; index < leaves; ++index) {
        const auto key = "k" + std::to_string(10000 + index);
        EXPECT_EQ(publisher.capture()->mapping.get(index)->leaf->lookup(key).cell.value.to_string(), "value");
    }
}
} // namespace
} // namespace crowdb::tree
