// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(CAbiReadBudget, LazySnapshotIterationReturnsPressureInsteadOfThrowingAcrossTheAbi)
{
    ct_page_store *raw_store = nullptr;
    ASSERT_EQ(ct_page_store_open_mem(1, &raw_store), 0);
    std::unique_ptr<ct_page_store, decltype(&ct_page_store_free)> store(raw_store, &ct_page_store_free);
    ct_options                                                    options{};
    options.page_store = store.get();
    ct_tree *raw       = nullptr;
    ASSERT_EQ(ct_open(&options, &raw), 0);
    std::unique_ptr<ct_tree, decltype(&ct_close)> tree(raw, &ct_close);
    const uint8_t                                 key   = 'k';
    const uint8_t                                 value = 'v';
    ASSERT_EQ(ct_apply_put(tree.get(), 1, &key, 1, &value, 1), 0);
    ASSERT_EQ(ct_flush(tree.get()), 0);
    ct_view *raw_view = nullptr;
    ASSERT_EQ(ct_snapshot_view(tree.get(), &raw_view), 0);
    std::unique_ptr<ct_view, decltype(&ct_view_release)> view(raw_view, &ct_view_release);
    ct_iter                                             *raw_iterator = nullptr;
    ASSERT_EQ(ct_view_iter(view.get(), &raw_iterator), 0);
    std::unique_ptr<ct_iter, decltype(&ct_iter_release)> iterator(raw_iterator, &ct_iter_release);
    auto                                                *core = TreeCoreAccess::get(*c_api_tree(tree.get()));
    core->binding()->drain();
    const auto account = core->binding()->account();
    auto       blocker =
        detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting,
                                            detail::ResourceBudget::available(account, detail::BudgetClass::kRouting));
    ASSERT_TRUE(blocker);
    int32_t   valid  = 7;
    ct_status status = 0;
    EXPECT_NO_THROW(status = ct_iter_next(iterator.get(), nullptr, nullptr, nullptr, nullptr, &valid));
    EXPECT_EQ(status, static_cast<ct_status>(Code::kResourceExhausted));
    EXPECT_EQ(valid, 0);
    blocker.reset();
    ct_buf returned_key{};
    ct_buf returned_value{};
    ASSERT_EQ(ct_iter_next(iterator.get(), &returned_key, nullptr, nullptr, &returned_value, &valid), 0);
    ASSERT_EQ(valid, 1);
    EXPECT_EQ(returned_key.len, 1U);
    EXPECT_EQ(returned_value.len, 1U);
    EXPECT_EQ(returned_key.data[0], key);
    EXPECT_EQ(returned_value.data[0], value);
    ct_free_buf(&returned_key);
    ct_free_buf(&returned_value);
}

TEST(CAbiReadBudget, PressureReturnsResourceExhaustedAndDoesNotLatchIoFailure)
{
    ct_options options{};
    ct_tree   *raw = nullptr;
    ASSERT_EQ(ct_open(&options, &raw), 0);
    std::unique_ptr<ct_tree, decltype(&ct_close)> tree(raw, &ct_close);
    const uint8_t                                 key   = 'k';
    const uint8_t                                 value = 'v';
    ASSERT_EQ(ct_apply_put(tree.get(), 1, &key, 1, &value, 1), 0);
    const auto account = TreeCoreAccess::get(*c_api_tree(tree.get()))->binding()->account();
    auto       blocker =
        detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting,
                                            detail::ResourceBudget::available(account, detail::BudgetClass::kRouting));
    ASSERT_TRUE(blocker);
    int32_t  found = 7;
    uint64_t slot  = 7;
    ct_buf   result{};
    EXPECT_EQ(ct_get(tree.get(), &key, 1, &found, &slot, &result), static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(found, 0);
    EXPECT_EQ(slot, 0U);
    EXPECT_EQ(result.data, nullptr);
    EXPECT_EQ(result.len, 0U);
    EXPECT_FALSE(c_api_tree(tree.get())->get_view("k").found());
    ct_stats stats{};
    ct_get_stats(tree.get(), &stats);
    EXPECT_EQ(stats.io_failed, 0);
    blocker.reset();
    ASSERT_EQ(ct_get(tree.get(), &key, 1, &found, &slot, &result), 0);
    EXPECT_EQ(found, 1);
    EXPECT_EQ(slot, 1U);
    ASSERT_EQ(result.len, 1U);
    EXPECT_EQ(result.data[0], value);
    ct_free_buf(&result);
}
} // namespace crowdb::tree
