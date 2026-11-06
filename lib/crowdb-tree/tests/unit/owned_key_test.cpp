// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/owned_key.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(OwnedKey, ShortKeysNeedNoHeapCreditBeyondTheirContainingObject)
{
    detail::ResourceBudget::Limits limits{};
    limits.fill(1024 * 1024);
    detail::ResourceBudget budget(limits, 1);
    auto                   account = budget.make_account(limits);
    auto                   credit  = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, 4096);
    if (!credit) {
        FAIL() << "short-key test control admission failed";
    }
    auto metadata = detail::AllocationReservation::create(std::move(*credit));
    metadata->seal();
    const auto        before = account->snapshot().physical;
    const std::string value(24, 'k');
    ASSERT_NO_THROW({
        OwnedKey key(Slice(value), metadata);
        EXPECT_EQ(Slice(key).to_string(), value);
    });
    EXPECT_EQ(account->snapshot().physical, before);
}

TEST(OwnedKey, CopiesAndMovesOwnBinaryBytesAcrossStorageTransitions)
{
    for (const size_t size : {0U, 1U, 23U, 24U, 25U, 64U, 4096U}) {
        std::string value(size, '\0');
        for (size_t i = 0; i < size; ++i) {
            value[i] = static_cast<char>(i % 256);
        }
        OwnedKey original{Slice(value)};
        auto     copy  = original;
        auto     moved = std::exchange(original, OwnedKey{});
        EXPECT_EQ(original.size(), 0U);
        original = "replacement";
        EXPECT_EQ(Slice(copy).to_string(), value);
        EXPECT_EQ(Slice(moved).to_string(), value);
        OwnedKey assigned(std::string(4096, 'a'));
        assigned = copy;
        EXPECT_EQ(Slice(assigned).to_string(), value);
        assigned = std::exchange(moved, OwnedKey{});
        EXPECT_EQ(moved.size(), 0U);
        EXPECT_EQ(Slice(assigned).to_string(), value);
        assigned = "short";
        EXPECT_EQ(assigned, "short");
        assigned = copy;
        EXPECT_EQ(Slice(assigned).to_string(), value);
        EXPECT_EQ(assigned <=> copy, std::strong_ordering::equal);
    }
}

TEST(OwnedKey, ContainerRelocationKeepsEachOwnedKeyValid)
{
    std::vector<OwnedKey> keys;
    // Keep growth deliberate: this regression must relocate existing keys.
    keys.reserve(1);
    for (size_t i = 0; i < 256; ++i) {
        keys.emplace_back(std::string(i, static_cast<char>(i)));
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_EQ(Slice(keys[i]).to_string(), std::string(i, static_cast<char>(i)));
    }
}
} // namespace crowdb::tree
