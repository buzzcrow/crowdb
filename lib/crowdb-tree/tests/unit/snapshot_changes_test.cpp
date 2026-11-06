// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/snapshot_changes.h"

#include <gtest/gtest.h>

#include <map>

namespace crowdb::tree
{
namespace
{
SnapshotPageChange change(uint64_t pid, uint64_t version)
{
    return {
        .page_id         = pid,
        .publication_seq = version,
        .before          = {.content = version - 1, .present = true},
        .after           = {.content = version,     .present = true}
    };
}

SnapshotChanges populated()
{
    SnapshotChanges result;
    for (uint64_t i = 0; i < 1024; ++i) {
        result = result.replace(change(i * 1024, 1));
    }
    return result;
}

TEST(SnapshotChanges, SparseUpdatesMatchOrderedReferenceAndKeepCapturedRoots)
{
    SnapshotChanges              selected;
    std::map<uint64_t, uint64_t> expected;
    for (size_t i = 0; i < 3000; ++i) {
        const uint64_t pid = i % 3 == 0 ? (i * 2654435761ULL) % SnapshotChanges::kPageLimit : (i * 37) % 64;
        selected           = selected.replace(change(pid, i + 1));
        expected[pid]      = i + 1;
    }
    selected                                  = selected.replace(change(SnapshotChanges::kPageLimit - 1, 3001));
    expected[SnapshotChanges::kPageLimit - 1] = 3001;
    EXPECT_EQ(selected.size(), expected.size());
    std::map<uint64_t, uint64_t> observed;
    uint64_t                     next    = 0;
    auto                         collect = [](void *raw, const SnapshotPageChange &entry) {
        static_cast<std::map<uint64_t, uint64_t> *>(raw)->emplace(entry.page_id, entry.after.content);
    };
    while (!selected.visit_from(next, 7, &observed, collect)) {
    }
    EXPECT_EQ(observed, expected);
    const auto captured = selected;
    ASSERT_NE(captured.get(0), nullptr);
    selected = selected.replace(change(0, 4000));
    EXPECT_FALSE(selected.same_root(captured));
    EXPECT_NE(captured.get(0)->after.content, selected.get(0)->after.content);
    EXPECT_EQ(selected.get(0)->after.content, 4000U);
    EXPECT_EQ(selected.get(SnapshotChanges::kPageLimit), nullptr);
    EXPECT_THROW((void)selected.replace(change(SnapshotChanges::kPageLimit, 1)), std::invalid_argument);
}

TEST(SnapshotChanges, DeletionRetainsOriginalRangeWithoutChangingCapturedVersion)
{
    auto initial         = change(7, 2);
    initial.before.range = {.lower = OwnedKey(""), .upper = OwnedKey("old")};
    initial.after.range  = {.lower = OwnedKey("new"), .upper = std::nullopt};
    auto       selected  = SnapshotChanges{}.replace(initial);
    const auto captured  = selected;
    auto       removed   = change(7, 3);
    removed.after        = {};
    selected             = selected.replace(removed);
    ASSERT_TRUE(selected.get(7));
    EXPECT_FALSE(selected.get(7)->after.present);
    EXPECT_EQ(selected.get(7)->before.content, 1U);
    EXPECT_EQ(selected.get(7)->before.range.lower, OwnedKey(""));
    EXPECT_EQ(selected.get(7)->before.range.upper, OwnedKey("old"));
    EXPECT_TRUE(captured.get(7)->after.present);
    EXPECT_EQ(captured.get(7)->after.range.lower, OwnedKey("new"));
}

TEST(SnapshotChanges, EffectsAccumulateWithoutLosingEarlierContentOrCapturedClassification)
{
    auto initial        = change(7, 1);
    initial.effect      = SnapshotChangeEffect::SourceStructure;
    auto       selected = SnapshotChanges{}.replace(initial);
    const auto captured = selected;
    auto       accepted = change(7, 2);
    accepted.effect     = SnapshotChangeEffect::AcceptedPrefix;
    selected            = selected.replace(accepted);
    auto structure      = change(7, 3);
    structure.effect    = SnapshotChangeEffect::SourceStructure;
    selected            = selected.replace(structure);
    ASSERT_TRUE(selected.get(7));
    EXPECT_EQ(selected.get(7)->effect, SnapshotChangeEffect::SourceStructure | SnapshotChangeEffect::AcceptedPrefix);
    EXPECT_EQ(captured.get(7)->effect, SnapshotChangeEffect::SourceStructure);
    auto full                 = change(7, 4);
    structure.publication_seq = 5;
    structure.after.content   = 5;
    selected                  = selected.replace(full).replace(structure);
    EXPECT_EQ(selected.get(7)->effect, SnapshotChangeEffect::FullContent | SnapshotChangeEffect::SourceStructure |
                                           SnapshotChangeEffect::AcceptedPrefix);
    EXPECT_EQ(selected.get(7)->before.content, 0);
}

TEST(SnapshotChanges, SingleUpdateUsesBoundedCreditAndFailedCopyLeavesInputIntact)
{
    const auto original = populated();
    using detail::BudgetClass;
    using detail::ResourceBudget;
    constexpr auto         category = static_cast<size_t>(BudgetClass::kRouting);
    const auto             bound    = SnapshotChanges::change_allocation_bound();
    ResourceBudget::Limits limits{};
    limits[category] = bound;
    ResourceBudget budget(limits, 1);
    auto           account = budget.make_account(limits);
    auto           grant   = ResourceBudget::try_reserve(account, BudgetClass::kRouting, bound);
    if (!grant.has_value()) {
        ADD_FAILURE() << "snapshot update credit unavailable";
        return;
    }
    auto reservation = std::make_shared<detail::AllocationReservation>(std::move(*grant));
    {
        auto updated = original.replace(change(513 * 1024, 2), reservation);
        EXPECT_EQ(updated.size(), original.size());
        EXPECT_LE(budget.snapshot().allocated[category], bound);
        EXPECT_EQ(updated.get(513 * 1024)->after.content, 2U);
        EXPECT_EQ(original.get(513 * 1024)->after.content, 1U);
    }
    EXPECT_EQ(reservation->available(), bound);
    reservation->seal();
    auto small = ResourceBudget::try_reserve(account, BudgetClass::kRouting, 1);
    if (!small.has_value()) {
        ADD_FAILURE() << "failure injection credit unavailable";
        return;
    }
    auto exhausted = std::make_shared<detail::AllocationReservation>(std::move(*small));
    EXPECT_THROW((void)original.replace(change(1, 3), exhausted), std::bad_alloc);
    EXPECT_EQ(exhausted->available(), 1U);
    EXPECT_EQ(original.get(1), nullptr);
    EXPECT_EQ(original.size(), 1024U);
}

TEST(SnapshotChanges, BootstrapUsesOneTreeWithinItsLinearCreditBound)
{
    std::vector<SnapshotPageChange> initial;
    initial.reserve(1024);
    for (uint64_t i = 0; i < 1024; ++i) {
        initial.push_back(change(i * 4096, i + 1));
    }
    const auto bound = SnapshotChanges::initial_allocation_bound(initial.size());
    if (!bound.has_value()) {
        ADD_FAILURE() << "snapshot bootstrap bound overflowed";
        return;
    }
    detail::ResourceBudget::Limits limits{};
    const auto                     category = static_cast<size_t>(detail::BudgetClass::kRouting);
    limits[category]                        = *bound;
    detail::ResourceBudget budget(limits, 1);
    auto                   grant =
        detail::ResourceBudget::try_reserve(budget.make_account(limits), detail::BudgetClass::kRouting, *bound);
    if (!grant.has_value()) {
        ADD_FAILURE() << "snapshot bootstrap credit unavailable";
        return;
    }
    auto reservation = std::make_shared<detail::AllocationReservation>(std::move(*grant));
    {
        auto selected = SnapshotChanges::from_sorted(initial, reservation);
        EXPECT_EQ(selected.size(), initial.size());
        for (const auto &entry : initial) {
            ASSERT_TRUE(selected.get(entry.page_id));
            EXPECT_EQ(selected.get(entry.page_id)->after.content, entry.after.content);
        }
    }
    EXPECT_EQ(reservation->available(), *bound);
    initial[1].page_id = initial[0].page_id;
    EXPECT_THROW((void)SnapshotChanges::from_sorted(initial), std::invalid_argument);
    EXPECT_FALSE(SnapshotChanges::initial_allocation_bound(SIZE_MAX));
}
} // namespace
} // namespace crowdb::tree
