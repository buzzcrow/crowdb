// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "crowdb-tree/memtable/memtable.h"
#include "external_owner_bridge.h"

#include <gtest/gtest.h>

#include <array>
#include <thread>

namespace crowdb::tree
{
namespace
{
constexpr size_t kWriter = static_cast<size_t>(detail::BudgetClass::kAdmittedWriter);

struct TestHost
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result{};
        result[kWriter] = 1024 * 1024;
        return result;
    }

    detail::ResourceBudget budget{limits(), 4};

    std::shared_ptr<detail::AllocationReservation> reserve(size_t bytes = 65536)
    {
        auto account = budget.make_account(limits());
        auto credit  = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kAdmittedWriter, bytes);
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

Status create_owner(const std::shared_ptr<AllocationBudget> &budget, size_t &drops,
                    std::shared_ptr<const ExternalOwnerBridge> *out)
{
    auto payload = std::make_unique<TestPayload>();
    payload->bytes.fill('v');
    payload->drops                 = &drops;
    const auto               *data = payload->bytes.data();
    ExternalAllocation::Owner owner(payload.release(), [](void *context) {
        auto *payload = static_cast<TestPayload *>(context);
        ++*payload->drops;
        delete payload;
    });
    return ExternalOwnerBridge::create(data, 4096, sizeof(TestPayload), std::move(owner), budget, out);
}

TEST(ExternalOwnerBridge, SharedSlicesChargeCompleteCapacityAndFinalDropOnce)
{
    TestHost                                   host;
    auto                                       credit = host.reserve();
    size_t                                     drops  = 0;
    std::shared_ptr<const ExternalOwnerBridge> owner;
    ASSERT_TRUE(create_owner(credit, drops, &owner).ok());
    EXPECT_EQ(owner->capacity(), sizeof(TestPayload));
    EXPECT_GE(host.allocated(), sizeof(TestPayload));
    const auto *identity = owner->identity();
    buffer      first;
    buffer      second;
    const auto  before = host.allocated();
    ASSERT_TRUE(owner->slice(0, 1024, &first).ok());
    ASSERT_TRUE(owner->slice(1024, 1024, &second).ok());
    EXPECT_LT(host.allocated() - before, 256U);
    EXPECT_EQ(first.external_allocation(), identity);
    EXPECT_EQ(second.external_allocation(), identity);
    owner.reset();
    credit->seal();
    first = {};
    EXPECT_EQ(drops, 0U);
    EXPECT_EQ(second.slice().to_string(), std::string(1024, 'v'));
    second = {};
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(host.allocated(), 0U);
}

TEST(ExternalOwnerBridge, SliceRejectsSpareCapacityAndOverflowWithoutConsumingCredit)
{
    TestHost                                   host;
    auto                                       credit = host.reserve();
    size_t                                     drops  = 0;
    std::shared_ptr<const ExternalOwnerBridge> owner;
    ASSERT_TRUE(create_owner(credit, drops, &owner).ok());
    buffer     slice;
    const auto before = host.allocated();
    EXPECT_EQ(owner->slice(4096, 1, &slice).code(), Code::kInvalidArgument);
    EXPECT_EQ(owner->slice(SIZE_MAX, 1, &slice).code(), Code::kInvalidArgument);
    EXPECT_EQ(owner->slice(1, SIZE_MAX, &slice).code(), Code::kInvalidArgument);
    EXPECT_EQ(host.allocated(), before);
    ASSERT_TRUE(owner->slice(4096, 0, &slice).ok());
}

TEST(ExternalOwnerBridge, MissingHostAndExhaustedReservationsReleaseOwner)
{
    size_t                                     drops = 0;
    std::shared_ptr<const ExternalOwnerBridge> owner;
    EXPECT_EQ(create_owner({}, drops, &owner).code(), Code::kInvalidArgument);
    EXPECT_EQ(drops, 1U);
    TestHost host;
    for (size_t capacity : {size_t{0}, size_t{1024}}) {
        auto credit = host.reserve(capacity);
        EXPECT_EQ(create_owner(credit, drops, &owner).code(), Code::kResourceExhausted);
        EXPECT_EQ(owner, nullptr);
        EXPECT_EQ(host.allocated(), 0U);
    }
    EXPECT_EQ(drops, 3U);
}

TEST(ExternalOwnerBridge, InvalidCompleteAllocationDescriptorDropsWithoutCharging)
{
    TestHost                                   host;
    auto                                       credit = host.reserve();
    size_t                                     drops  = 0;
    std::array<uint8_t, 4>                     bytes{};
    std::shared_ptr<const ExternalOwnerBridge> owner;
    const auto                                 release = [](void *context) { ++*static_cast<size_t *>(context); };
    EXPECT_EQ(ExternalOwnerBridge::create(bytes.data(), 5, 4, {&drops, release}, credit, &owner).code(),
              Code::kInvalidArgument);
    EXPECT_EQ(ExternalOwnerBridge::create(nullptr, 1, 4, {&drops, release}, credit, &owner).code(),
              Code::kInvalidArgument);
    EXPECT_EQ(ExternalOwnerBridge::create(bytes.data(), 1, 4, {&drops, release}, credit, nullptr).code(),
              Code::kInvalidArgument);
    EXPECT_EQ(drops, 3U);
    EXPECT_EQ(host.allocated(), 0U);
}

TEST(ExternalOwnerBridge, TwoTreeAccountsRetainIdentityWithOneHostCapacityCharge)
{
    TestHost                                   host;
    auto                                       payload_credit = host.reserve();
    auto                                       first_credit   = host.reserve();
    auto                                       second_credit  = host.reserve();
    size_t                                     drops          = 0;
    std::shared_ptr<const ExternalOwnerBridge> owner;
    ASSERT_TRUE(create_owner(payload_credit, drops, &owner).ok());
    const auto *data = owner->identity()->data();
    {
        MemTable first(1, nullptr, first_credit);
        MemTable second(1, nullptr, second_credit);
        buffer   left;
        buffer   right;
        ASSERT_TRUE(owner->slice(0, 1024, &left).ok());
        ASSERT_TRUE(owner->slice(1024, 1024, &right).ok());
        const auto before = host.allocated();
        ASSERT_TRUE(first.upsert_external("left", 1, 0, std::move(left), 0));
        ASSERT_TRUE(second.upsert_external("right", 1, 0, std::move(right), 0));
        EXPECT_LT(host.allocated() - before, 4096U);
        owner.reset();
        auto left_guard  = first.read_guard();
        auto right_guard = second.read_guard();
        EXPECT_EQ(first.find("left")->cell.data(), data);
        EXPECT_EQ(second.find("right")->cell.data(), data + 1024);
        EXPECT_EQ(drops, 0U);
    }
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(host.allocated(), 0U);
}

TEST(ExternalOwnerBridge, ForeignHostCopiesAndCancellationCanReleaseOnAnotherThread)
{
    TestHost                                   first;
    TestHost                                   second;
    auto                                       payload_credit = first.reserve();
    auto                                       source_credit  = second.reserve();
    size_t                                     drops          = 0;
    std::shared_ptr<const ExternalOwnerBridge> owner;
    ASSERT_TRUE(create_owner(payload_credit, drops, &owner).ok());
    buffer value;
    ASSERT_TRUE(owner->slice(0, 1024, &value).ok());
    owner.reset();
    {
        MemTable table(1, nullptr, source_credit);
        ASSERT_TRUE(table.upsert_external("key", 1, 0, std::move(value), 0));
        EXPECT_EQ(drops, 1U);
        EXPECT_EQ(first.allocated(), 0U);
        auto guard = table.read_guard();
        EXPECT_TRUE(table.find("key")->cell.owned());
    }
    EXPECT_EQ(second.allocated(), 0U);
    ASSERT_TRUE(create_owner(payload_credit, drops, &owner).ok());
    ASSERT_TRUE(owner->slice(0, 1, &value).ok());
    owner.reset();
    payload_credit->seal();
    std::thread release([value = std::move(value)]() mutable { value = {}; });
    release.join();
    EXPECT_EQ(drops, 2U);
    EXPECT_EQ(first.allocated(), 0U);
}
} // namespace
} // namespace crowdb::tree
