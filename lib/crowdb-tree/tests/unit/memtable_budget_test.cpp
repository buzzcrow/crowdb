// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "crowdb-tree/memtable/memtable.h"
#include "runtime/tree_runtime_binding.h"

#include <gtest/gtest.h>

#include <semaphore>
#include <thread>

namespace crowdb::tree
{
namespace
{
constexpr size_t kWriter = static_cast<size_t>(detail::BudgetClass::kAdmittedWriter);

detail::ResourceBudget::Limits limits()
{
    detail::ResourceBudget::Limits result{};
    result[kWriter] = 1024 * 1024;
    return result;
}

struct TestCredit
{
    detail::ResourceBudget                         budget{limits(), 4};
    std::shared_ptr<detail::AllocationReservation> reservation;

    explicit TestCredit(std::shared_ptr<detail::RetirementQueue> queue = {})
    {
        if (queue) {
            budget.set_retirement_queue(std::move(queue));
        }
        auto credit = detail::ResourceBudget::try_reserve(budget.make_account(limits()),
                                                          detail::BudgetClass::kAdmittedWriter, limits()[kWriter]);
        if (!credit) {
            throw std::bad_alloc();
        }
        reservation = std::make_shared<detail::AllocationReservation>(std::move(*credit));
    }

    [[nodiscard]] size_t allocated() const
    {
        return budget.snapshot().allocated[kWriter];
    }
};

class TestFailBudget final : public AllocationBudget
{
  public:
    explicit TestFailBudget(std::shared_ptr<AllocationBudget> owner) : owner_(std::move(owner))
    {
    }

    size_t fail_after = SIZE_MAX;

    bool claim(size_t bytes) noexcept override
    {
        if (fail_after == 0) {
            return false;
        }
        --fail_after;
        return owner_->claim(bytes);
    }

    void release(size_t bytes) noexcept override
    {
        owner_->release(bytes);
    }

  private:
    std::shared_ptr<AllocationBudget> owner_;
};

TEST(MemTableBudget, PayloadCapacityStaysChargedThroughIndependentCellOwner)
{
    TestCredit credit;
    auto       table   = std::make_shared<MemTable>(1, nullptr, credit.reservation);
    auto       payload = buffer::alloc(4096);
    const auto encoded = encode_cell_buf(1, OpKind::kPut, "value");
    std::memcpy(payload.data(), encoded.data(), encoded.size());
    payload.set_size(encoded.size());
    ASSERT_TRUE(table->upsert("key", 1, std::move(payload)));
    EXPECT_GE(credit.allocated(), 4096U + sizeof(CellVersion));
    std::shared_ptr<const CellVersion> cell;
    {
        auto guard = table->read_guard();
        cell       = table->prefix_cursor(1).cell_owner();
    }
    credit.reservation->seal();
    table.reset();
    EXPECT_GE(credit.allocated(), 4096U + sizeof(CellVersion));
    EXPECT_EQ(CellView(cell->cell.slice()).value().to_string(), "value");
    EXPECT_EQ(credit.budget.snapshot().reserved[kWriter], 0U);
    cell.reset();
    EXPECT_EQ(credit.allocated(), 0U);
}

TEST(MemTableBudget, FrozenReaderNeedsNoNewAllocationAndRetainsItsChargedPayload)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
    TestCredit credit(binding->lifecycle_budget()->retirement_queue());
    auto       table = std::make_shared<MemTable>(1, nullptr, credit.reservation);
    ASSERT_TRUE(table->upsert("key", 1, encode_cell_buf(1, OpKind::kPut, "value"), 0));
    auto guard = table->read_guard();
    table->close();
    credit.reservation->seal();
    const auto   allocated = credit.allocated();
    std::jthread reader([owner = table] {
        auto frozen_guard = owner->read_guard();
        ASSERT_NE(owner->find("key"), nullptr);
        EXPECT_EQ(CellView(owner->find("key")->cell.slice()).value(), Slice("value"));
    });
    reader.join();
    EXPECT_EQ(credit.allocated(), allocated);
    table.reset();
    credit.reservation->account()->drain_retirements();
    EXPECT_GT(credit.allocated(), 0U);
    guard = {};
    credit.reservation->account()->drain_retirements();
    EXPECT_EQ(credit.allocated(), 0U);
}

TEST(MemTableBudget, FreezeRejectsAnEpochRegistrationThatAlreadyReadTheOldHead)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
    TestCredit credit(binding->lifecycle_budget()->retirement_queue());
    auto       table = std::make_shared<MemTable>(1, nullptr, credit.reservation);
    ASSERT_TRUE(table->upsert("key", 1, encode_cell_buf(1, OpKind::kPut, "value"), 0));
    auto                  old_guard = table->read_guard();
    auto                 &epoch     = table->epoch_for_tests();
    std::binary_semaphore reached{0};
    std::binary_semaphore proceed{0};

    struct Pause
    {
        std::binary_semaphore &reached;
        std::binary_semaphore &proceed;
    } pause{.reached = reached, .proceed = proceed};

    epoch.set_before_participant_append_for_tests(
        [](void *context) {
            auto &pause = *static_cast<Pause *>(context);
            pause.reached.release();
            pause.proceed.acquire();
        },
        &pause);
    std::jthread reader([owner = table] {
        auto guard = owner->read_guard();
        ASSERT_NE(owner->find("key"), nullptr);
        EXPECT_EQ(owner->find("key")->slot, 1U);
    });
    reached.acquire();
    table->close();
    proceed.release();
    reader.join();
    epoch.set_before_participant_append_for_tests(nullptr, nullptr);
    EXPECT_TRUE(epoch.source_sealed());
    EXPECT_EQ(epoch.active_guards(), 1U);
    old_guard = {};
    EXPECT_EQ(epoch.active_guards(), 0U);
    credit.reservation->seal();
    table.reset();
    credit.reservation->account()->drain_retirements();
    EXPECT_EQ(credit.allocated(), 0U);
}

TEST(MemTableBudget, EveryOverwriteAllocationFailureKeepsPublishedVersionAndReturnsCredit)
{
    size_t failures = 0;
    for (size_t fail_after = 0; fail_after < 8; ++fail_after) {
        TestCredit   credit;
        auto         failing = std::make_shared<TestFailBudget>(credit.reservation);
        EpochManager epoch;
        // The externally owned epoch is outside this table's data account.
        {
            auto registration = epoch.enter();
        }
        auto table = std::make_shared<MemTable>(1, &epoch, failing);
        ASSERT_TRUE(table->upsert("key", 1, encode_cell_buf(1, OpKind::kPut, "previous"), 0));
        const auto allocated = credit.allocated();
        failing->fail_after  = fail_after;
        bool failed          = false;
        try {
            ASSERT_TRUE(table->upsert("key", 2, encode_cell_buf(2, OpKind::kPut, std::string(4096, 'v')), 0));
        }
        catch (const std::bad_alloc &) {
            failed = true;
            ++failures;
        }
        {
            auto guard = table->read_guard();
            ASSERT_NE(table->find("key"), nullptr);
            EXPECT_EQ(table->find("key")->slot, failed ? 1U : 2U);
        }
        if (failed) {
            EXPECT_EQ(credit.allocated(), allocated);
        }
        table.reset();
        epoch.try_reclaim();
        credit.reservation->seal();
        EXPECT_EQ(credit.allocated(), 0U);
    }
    EXPECT_GE(failures, 4U);
}

TEST(MemTableBudget, RetiredHistoryRemainsChargedUntilReadGuardLeaves)
{
    TestCredit   credit;
    EpochManager epoch;
    {
        auto registration = epoch.enter();
    }
    auto table = std::make_shared<MemTable>(1, &epoch, credit.reservation);
    ASSERT_TRUE(table->upsert("key", 1, encode_cell_buf(1, OpKind::kPut, std::string(1024, 'a')), 0));
    size_t retained;
    {
        auto guard = table->read_guard();
        for (uint64_t slot = 2; slot <= 8; ++slot) {
            ASSERT_TRUE(table->upsert("key", slot, encode_cell_buf(slot, OpKind::kPut, std::string(1024, 'b')), 0));
        }
        retained = credit.allocated();
        table->prune("key", 8);
        epoch.try_reclaim();
        EXPECT_GE(credit.allocated(), retained);
    }
    epoch.try_reclaim();
    EXPECT_LT(credit.allocated(), retained);
    table.reset();
    epoch.try_reclaim();
    credit.reservation->seal();
    EXPECT_EQ(credit.allocated(), 0U);
}

TEST(MemTableBudget, UnknownExternalCapacityIsCopiedBeforeRetainingInBudgetedSource)
{
    TestCredit credit;
    auto       table = std::make_shared<MemTable>(1, nullptr, credit.reservation);

    struct TestExternal
    {
        std::string value;
        size_t     *drops;
    };

    size_t drops = 0;
    auto  *owner = new TestExternal{.value = std::string(8192, 'v'), .drops = &drops};
    auto   value =
        buffer::wrap_external(reinterpret_cast<const uint8_t *>(owner->value.data()), 1024, owner, [](void *context) {
            auto *data = static_cast<TestExternal *>(context);
            ++*data->drops;
            delete data;
        });
    ASSERT_TRUE(table->upsert_external("key", 1, 0, std::move(value), 0));
    EXPECT_EQ(drops, 1U);
    auto        guard = table->read_guard();
    const auto *cell  = table->find("key");
    ASSERT_NE(cell, nullptr);
    EXPECT_EQ(cell->cell.ownership(), buffer::mode::kOwned);
    EXPECT_EQ(CellView(cell->cell.slice()).value().to_string(), std::string(1024, 'v'));
    EXPECT_GE(credit.allocated(), 1024U + kCellHeaderSize);
    EXPECT_LT(credit.allocated(), 8192U);
}
} // namespace
} // namespace crowdb::tree
