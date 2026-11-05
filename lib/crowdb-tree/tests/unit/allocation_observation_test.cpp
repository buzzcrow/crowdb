// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "crowdb-tree/external_allocation.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
size_t observed(const std::shared_ptr<ResourceBudget::Account> &account, AllocationKind kind)
{
    return account->snapshot().physical[static_cast<size_t>(kind)];
}

class AllocationObservation : public testing::Test
{
  protected:
    ResourceBudget budget{
        {4096, 4096, 4096, 4096, 4096, 2},
        2
    };
    std::shared_ptr<ResourceBudget::Account> account = budget.make_account({4096, 4096, 4096, 4096, 4096, 2});

    std::shared_ptr<AllocationReservation> reserve(BudgetClass category, size_t bytes)
    {
        auto credit = ResourceBudget::try_reserve(account, category, bytes);
        if (!credit) {
            ADD_FAILURE() << "test reservation must fit";
            return nullptr;
        }
        return AllocationReservation::create(std::move(*credit));
    }
};

TEST_F(AllocationObservation, SharedControlRemainsMetadataUntilTheFinalWeakOwnerReleases)
{
    auto owner = reserve(BudgetClass::kRouting, 1024);
    ASSERT_TRUE(owner);
    const auto bootstrap = observed(account, AllocationKind::kMetadata);
    EXPECT_GT(bootstrap, 0U);
    auto       value     = reserved_shared<uint64_t>(owner, 42);
    const auto allocated = observed(account, AllocationKind::kMetadata);
    EXPECT_GT(allocated, bootstrap + sizeof(uint64_t));
    std::weak_ptr<uint64_t> weak = value;
    owner->seal();
    value.reset();
    EXPECT_EQ(observed(account, AllocationKind::kMetadata), allocated);
    owner.reset();
    EXPECT_EQ(observed(account, AllocationKind::kMetadata), allocated);
    weak.reset();
    EXPECT_EQ(observed(account, AllocationKind::kMetadata), 0U);
    EXPECT_EQ(account->snapshot().allocated[static_cast<size_t>(BudgetClass::kRouting)], 0U);
}

TEST_F(AllocationObservation, VectorCapacityAndFailedGrowthUseTheActualAllocationKind)
{
    auto owner = reserve(BudgetClass::kHandoff, 1024);
    ASSERT_TRUE(owner);
    using Allocator = ReservationAllocator<uint8_t, AllocationKind::kFrame>;
    {
        std::vector<uint8_t, Allocator> frame(96, Allocator(owner));
        EXPECT_EQ(observed(account, AllocationKind::kFrame), frame.capacity());
        frame.reserve(192);
        EXPECT_EQ(observed(account, AllocationKind::kFrame), frame.capacity());
        const auto held = frame.capacity();
        EXPECT_THROW(frame.reserve(4096), std::bad_alloc);
        EXPECT_EQ(frame.capacity(), held);
        EXPECT_EQ(observed(account, AllocationKind::kFrame), held);
        owner->seal();
    }
    EXPECT_EQ(observed(account, AllocationKind::kFrame), 0U);
    owner.reset();
    EXPECT_EQ(observed(account, AllocationKind::kMetadata), 0U);
}

TEST_F(AllocationObservation, SharedExternalPayloadIsChargedOnceAndSurvivesReservationSeal)
{
    auto owner = reserve(BudgetClass::kRouting, 2048);
    ASSERT_TRUE(owner);
    auto *bytes      = new uint8_t[512];
    auto  allocation = ExternalAllocation::create(
        bytes, 512, ExternalAllocation::Owner(bytes, [](void *value) { delete[] static_cast<uint8_t *>(value); }),
        owner);
    auto clone = allocation;
    EXPECT_EQ(observed(account, AllocationKind::kPayload), 512U);
    owner->seal();
    allocation.reset();
    owner.reset();
    EXPECT_EQ(observed(account, AllocationKind::kPayload), 512U);
    clone.reset();
    EXPECT_EQ(observed(account, AllocationKind::kPayload), 0U);
    EXPECT_EQ(observed(account, AllocationKind::kMetadata), 0U);
}

TEST_F(AllocationObservation, ClassifiedCreditTransfersWithoutDoubleCounting)
{
    auto reserved = ResourceBudget::try_reserve(account, BudgetClass::kRouting, 512);
    if (!reserved) {
        ADD_FAILURE() << "reservation must fit";
        return;
    }
    auto allocation = reserved->allocate(128);
    if (!allocation) {
        ADD_FAILURE() << "allocation must fit reservation";
        return;
    }
    ASSERT_TRUE(allocation->mark_allocation(AllocationKind::kScratch));
    EXPECT_FALSE(allocation->mark_allocation(AllocationKind::kPayload));
    auto part = allocation->split(32);
    ASSERT_TRUE(part);
    EXPECT_EQ(observed(account, AllocationKind::kScratch), 128U);
    allocation.reset();
    EXPECT_EQ(observed(account, AllocationKind::kScratch), 32U);
    part.reset();
    EXPECT_EQ(observed(account, AllocationKind::kScratch), 0U);
}
} // namespace
} // namespace crowdb::tree::detail
