// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "btree/budget_memory_resource.h"

#include <gtest/gtest.h>

#include <functional>
#include <vector>

namespace crowdb::tree::detail
{
namespace
{
constexpr size_t kMetadata = static_cast<size_t>(BudgetClass::kHandoff);

struct TestBudget
{
    static ResourceBudget::Limits limits()
    {
        ResourceBudget::Limits result{};
        result[kMetadata] = 65536;
        return result;
    }

    ResourceBudget                           budget{limits(), 2};
    std::shared_ptr<ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] std::shared_ptr<AllocationReservation> reserve(size_t bytes = 16384) const
    {
        auto credit = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, bytes);
        if (!credit) {
            throw std::bad_alloc();
        }
        return std::make_shared<AllocationReservation>(std::move(*credit));
    }

    [[nodiscard]] size_t allocated() const
    {
        return budget.snapshot().allocated[kMetadata];
    }
};

class TestUpstream final : public std::pmr::memory_resource
{
  public:
    bool                  fail = false;
    std::function<void()> before_free;

  private:
    void *do_allocate(size_t bytes, size_t alignment) override
    {
        if (fail) {
            throw std::bad_alloc();
        }
        return std::pmr::new_delete_resource()->allocate(bytes, alignment);
    }

    void do_deallocate(void *pointer, size_t bytes, size_t alignment) override
    {
        if (before_free) {
            before_free();
        }
        std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
    }

    [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override
    {
        return this == &other;
    }
};

TEST(BudgetMemoryResource, AlignedBlockKeepsItsOriginalReservationUntilPhysicalFree)
{
    TestBudget           budget;
    TestUpstream         upstream;
    BudgetMemoryResource resource(&upstream);
    auto                 first  = budget.reserve();
    auto                 second = budget.reserve();
    void                *pointer;
    {
        BudgetMemoryResource::Scope scope(resource, first);
        pointer = resource.allocate(3, 128);
    }
    EXPECT_EQ(reinterpret_cast<uintptr_t>(pointer) % 128, 0U);
    const auto charged = budget.allocated();
    EXPECT_GT(charged, 3U);
    EXPECT_EQ(charged, resource.retained_bytes());
    first->seal();
    upstream.before_free = [&] { EXPECT_EQ(budget.allocated(), charged); };
    {
        BudgetMemoryResource::Scope scope(resource, second);
        resource.deallocate(pointer, 3, 128);
    }
    EXPECT_EQ(second->available(), 16384U);
    EXPECT_EQ(budget.allocated(), 0U);
    EXPECT_EQ(resource.retained_bytes(), 0U);
}

TEST(BudgetMemoryResource, UpstreamAndAdmissionFailuresReturnAllCredit)
{
    TestBudget                  budget;
    TestUpstream                upstream;
    BudgetMemoryResource        resource(&upstream);
    auto                        credit = budget.reserve();
    BudgetMemoryResource::Scope scope(resource, credit);
    upstream.fail = true;
    EXPECT_THROW((void)resource.allocate(128, 64), std::bad_alloc);
    EXPECT_EQ(credit->available(), 16384U);
    EXPECT_EQ(budget.allocated(), 0U);
    upstream.fail = false;
    // Exercise runtime overflow rejection without a constant oversized allocation diagnostic.
    volatile size_t overflowing_bytes = SIZE_MAX;
    EXPECT_THROW((void)resource.allocate(overflowing_bytes, 64), std::bad_alloc);
    EXPECT_THROW((void)resource.allocate(65536, 64), std::bad_alloc);
    EXPECT_EQ(budget.allocated(), 0U);
    EXPECT_EQ(resource.retained_bytes(), 0U);
}

TEST(BudgetMemoryResource, ContainerGrowthUsesNewCreditAndFreesSealedOldCapacity)
{
    TestBudget           budget;
    TestUpstream         upstream;
    BudgetMemoryResource resource(&upstream);
    auto                 first  = budget.reserve();
    auto                 second = budget.reserve();
    {
        std::pmr::vector<uint64_t> values(&resource);
        {
            BudgetMemoryResource::Scope scope(resource, first);
            values.resize(64, 7);
        }
        const auto old_bytes = budget.allocated();
        first->seal();
        upstream.before_free = [&] { EXPECT_GT(budget.allocated(), old_bytes + (128 * sizeof(uint64_t))); };
        {
            BudgetMemoryResource::Scope scope(resource, second);
            values.reserve(128);
        }
        upstream.before_free = {};
        EXPECT_EQ(values.front(), 7U);
        EXPECT_GT(resource.retained_bytes(), 128 * sizeof(uint64_t));
        second->seal();
    }
    EXPECT_EQ(budget.allocated(), 0U);
    EXPECT_EQ(resource.retained_bytes(), 0U);
}
} // namespace
} // namespace crowdb::tree::detail
