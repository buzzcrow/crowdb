// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "crowdb-tree/external_allocation.h"
#include "crowdb-tree/memtable/memtable.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
constexpr size_t kWriter = static_cast<size_t>(detail::BudgetClass::kAdmittedWriter);

struct TestBudget
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result{};
        result[kWriter] = 1024 * 1024;
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> reserve(size_t bytes = 256 * 1024) const
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kAdmittedWriter, bytes);
        if (!credit) {
            throw std::bad_alloc();
        }
        return std::make_shared<detail::AllocationReservation>(std::move(*credit));
    }

    [[nodiscard]] size_t allocated() const
    {
        return budget.snapshot().allocated[kWriter];
    }
};

struct TestPayload
{
    std::array<uint8_t, 8192> bytes{};
    size_t                   *drops;
};

std::shared_ptr<const ExternalAllocation> payload(const std::shared_ptr<AllocationBudget> &budget, size_t &drops)
{
    auto *owner = new TestPayload{.drops = &drops};
    owner->bytes.fill('v');
    ExternalAllocation::Owner handle(owner, [](void *context) {
        auto *owner = static_cast<TestPayload *>(context);
        ++*owner->drops;
        delete owner;
    });
    // The owner allocation includes the drop-counter pointer too.
    return ExternalAllocation::create(owner->bytes.data(), sizeof(TestPayload), std::move(handle), budget);
}

TEST(ExternalAllocation, SharedSlicesKeepOneCapacityChargeAndSurviveSourceDetachment)
{
    TestBudget  budget;
    auto        payload_credit = budget.reserve();
    auto        source_credit  = budget.reserve();
    size_t      drops          = 0;
    auto        allocation     = payload(payload_credit, drops);
    auto        table          = std::make_shared<MemTable>(1, nullptr, source_credit);
    const auto  before         = budget.allocated();
    const auto *base           = allocation->data();
    for (size_t index = 0; index < 4; ++index) {
        auto value = buffer::share_external(allocation, index * 1024, 1024);
        ASSERT_TRUE(table->upsert_external(std::to_string(index), 1, 0, std::move(value), 0));
    }
    // Four 1 KiB views retain one 8 KiB allocation, plus bounded descriptors.
    EXPECT_LT(budget.allocated() - before, 4096U);
    std::shared_ptr<const CellVersion> retained;
    {
        auto guard = table->read_guard();
        retained   = table->prefix_cursor(1).cell_owner();
    }
    ASSERT_NE(retained, nullptr);
    EXPECT_EQ(retained->cell.data(), base);
    EXPECT_EQ(retained->cell.external_allocation(), allocation.get());
    allocation.reset();
    payload_credit->seal();
    source_credit->seal();
    table.reset();
    EXPECT_EQ(drops, 0U);
    EXPECT_GE(budget.allocated(), sizeof(TestPayload));
    EXPECT_EQ(retained->cell.slice().to_string(), std::string(1024, 'v'));
    retained.reset();
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(budget.allocated(), 0U);
}

TEST(ExternalAllocation, SeparateTreeAccountsShareOneHostPayloadCharge)
{
    TestBudget budget;
    auto       payload_credit = budget.reserve();
    auto       second_account = budget.budget.make_account(TestBudget::limits());
    auto credit = detail::ResourceBudget::try_reserve(second_account, detail::BudgetClass::kAdmittedWriter, 65536);
    if (!credit) {
        FAIL() << "test source credit unavailable";
    }
    auto        source_credit = std::make_shared<detail::AllocationReservation>(std::move(*credit));
    size_t      drops         = 0;
    auto        allocation    = payload(payload_credit, drops);
    const auto *data          = allocation->data();
    auto        first         = buffer::share_external(allocation, 0, 1024);
    auto        second        = buffer::share_external(allocation, 1024, 1024);
    MemTable    table(1, nullptr, source_credit);
    const auto  before = budget.allocated();
    ASSERT_TRUE(table.upsert_external("key", 1, 0, std::move(second), 0));
    EXPECT_LT(budget.allocated() - before, 1024U);
    allocation.reset();
    first      = {};
    auto guard = table.read_guard();
    ASSERT_NE(table.find("key"), nullptr);
    EXPECT_EQ(table.find("key")->cell.data(), data + 1024);
    EXPECT_EQ(drops, 0U);
}

TEST(ExternalAllocation, ForeignHostCopiesAndReleasesOriginalOwner)
{
    TestBudget first;
    TestBudget second;
    auto       payload_credit = first.reserve();
    auto       source_credit  = second.reserve();
    size_t     drops          = 0;
    auto       allocation     = payload(payload_credit, drops);
    auto       value          = buffer::share_external(allocation, 0, 1024);
    allocation.reset();
    MemTable table(1, nullptr, source_credit);
    ASSERT_TRUE(table.upsert_external("key", 1, 0, std::move(value), 0));
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(first.allocated(), 0U);
    auto guard = table.read_guard();
    ASSERT_NE(table.find("key"), nullptr);
    EXPECT_EQ(table.find("key")->cell.ownership(), buffer::mode::kOwned);
    EXPECT_EQ(CellView(table.find("key")->cell.slice()).value().to_string(), std::string(1024, 'v'));
}

TEST(ExternalAllocation, SliceBoundsAndMovesDoNotLeakOrDuplicateOwner)
{
    TestBudget budget;
    auto       credit     = budget.reserve();
    size_t     drops      = 0;
    auto       allocation = payload(credit, drops);
    const auto before     = budget.allocated();
    EXPECT_THROW(buffer::share_external(allocation, SIZE_MAX, 1), std::out_of_range);
    EXPECT_THROW(buffer::share_external(allocation, 1, SIZE_MAX), std::out_of_range);
    EXPECT_EQ(budget.allocated(), before);
    auto value = buffer::share_external(allocation, 0, 1024);
    auto moved = std::move(value);
    EXPECT_EQ(moved.external_allocation(), allocation.get());
    allocation.reset();
    EXPECT_EQ(drops, 0U);
    moved = {};
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(budget.allocated(), 0U);
}

TEST(ExternalAllocation, FailedDescriptorOrPayloadReservationDropsInputExactlyOnce)
{
    for (size_t bytes : {size_t{0}, size_t{1024}}) {
        TestBudget budget;
        auto       credit = budget.reserve(bytes);
        size_t     drops  = 0;
        EXPECT_THROW(payload(credit, drops), std::bad_alloc);
        EXPECT_EQ(drops, 1U);
        EXPECT_EQ(budget.allocated(), 0U);
    }
}
} // namespace
} // namespace crowdb::tree
