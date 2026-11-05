// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"
#include "backend/local/block_account.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "test_tmp.h"

#include <thread>

namespace crowdb::tree
{
namespace
{
using namespace detail;
using namespace detail::test_allocation_account;
constexpr size_t kCapacity = 1U << 20;
constexpr size_t kIoBytes  = static_cast<size_t>(AllocationKind::kIo);

TEST(BlockAccountGrant, SparseGrowthAndBounceUsePrepaidCapacityAtTheHardLimit)
{
    tree_test::TempDir              directory("block_grant_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 65536, 4096, &store).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              account = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    ASSERT_TRUE(bind_block_account(store.get(), account, 1, &adopted).ok());
    std::shared_ptr<AllocationBudget> grant;
    const BackendOperationLimits      work{.max_io_bytes = 512, .write_calls = 1, .workers = 1, .barriers = false};
    ASSERT_TRUE(reserve_backend_operation(store.get(), account, work, &grant).ok());
    const auto admitted = account->snapshot();
    auto occupied = ResourceBudget::try_reserve(account, BudgetClass::kRouting,
                                                kCapacity - admitted.allocated[kRouting] - admitted.reserved[kRouting]);
    ASSERT_TRUE(occupied);
    const uint8_t  value  = 73;
    const uint64_t offset = (65536ULL * 1000) + 17;
    EXPECT_EQ(store->write_at(offset, &value, 1).code(), Code::kResourceExhausted);
    ASSERT_TRUE(store->write_at(offset, &value, 1, grant).ok());
    EXPECT_EQ(account->snapshot().physical[kIoBytes], 0U);
    for (size_t i = 0; i < 3; ++i) {
        uint8_t actual = 0;
        ASSERT_TRUE(store->read_at(offset, &actual, 1, grant).ok());
        EXPECT_EQ(actual, value);
        EXPECT_EQ(account->snapshot().physical[kIoBytes], 0U);
    }
    EXPECT_GT(account->snapshot().allocated[kRouting], adopted.owned_bytes);
    seal_backend_budget(grant);
    occupied.reset();
    EXPECT_EQ(account->snapshot().reserved[kRouting], 0U);
    store.reset();
    EXPECT_GT(account->snapshot().allocated[kRouting], 0U);
    grant.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(account->snapshot().physical[kMetadata], 0U);
    EXPECT_EQ(account->snapshot().physical[kIoBytes], 0U);
}

TEST(BlockAccountGrant, CompletePrepaidBoundBindsAfterAllUnreservedCapacityIsOccupied)
{
    tree_test::TempDir              directory("block_prepaid_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 65536, 4096, &store).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              account = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    ASSERT_TRUE(bind_block_account(store.get(), account, 1, &adopted).ok());
    const BackendOperationLimits work{.max_io_bytes = 512, .write_calls = 1, .workers = 1, .barriers = false};
    const auto                   bound = backend_operation_bound(store.get(), work);
    if (!bound) {
        FAIL() << "missing bound test prerequisite";
    }
    auto paid = ResourceBudget::try_reserve(account, BudgetClass::kRouting, *bound);
    if (!paid) {
        FAIL() << "missing paid test prerequisite";
    }
    const auto before = account->snapshot();
    auto occupied     = ResourceBudget::try_reserve(account, BudgetClass::kRouting,
                                                    kCapacity - before.allocated[kRouting] - before.reserved[kRouting]);
    ASSERT_TRUE(occupied);
    std::shared_ptr<AllocationBudget> grant;
    ASSERT_TRUE(bind_backend_operation(store.get(), work, std::move(*paid), &grant).ok());
    const uint8_t value = 11;
    ASSERT_TRUE(store->write_at((65536ULL * 900) + 1, &value, 1, grant).ok());
    uint8_t actual = 0;
    ASSERT_TRUE(store->read_at((65536ULL * 900) + 1, &actual, 1, grant).ok());
    EXPECT_EQ(actual, value);
    seal_backend_budget(grant);
    grant.reset();
    occupied.reset();
    store.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(host.snapshot().reserved[kRouting], 0U);
}

TEST(BlockAccountGrant, IncompletePrepaidBoundFailsWithoutAllocatingOrGrowing)
{
    tree_test::TempDir              directory("block_prepaid_small_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 65536, 4096, &store).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              account = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    ASSERT_TRUE(bind_block_account(store.get(), account, 1, &adopted).ok());
    const BackendOperationLimits work{.max_io_bytes = 512, .write_calls = 1, .workers = 1, .barriers = false};
    const auto                   bound = backend_operation_bound(store.get(), work);
    if (!bound) {
        FAIL() << "missing bound test prerequisite";
    }
    auto paid = ResourceBudget::try_reserve(account, BudgetClass::kRouting, *bound - 1);
    if (!paid) {
        FAIL() << "missing paid test prerequisite";
    }
    const auto                        before = account->snapshot();
    std::shared_ptr<AllocationBudget> grant;
    EXPECT_EQ(bind_backend_operation(store.get(), work, std::move(*paid), &grant).code(), Code::kResourceExhausted);
    EXPECT_FALSE(grant);
    EXPECT_EQ(account->snapshot().allocated, before.allocated);
    EXPECT_EQ(account->snapshot().reserved[kRouting], 0U);
    EXPECT_EQ(store->num_extents(), 1U);
}

TEST(BlockAccountGrant, WrongBackendGrantAndOverflowBoundAreRejectedBeforeGrowth)
{
    tree_test::TempDir              directory("block_grant_wrong_");
    std::unique_ptr<BlockPageStore> left;
    std::unique_ptr<BlockPageStore> right;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 1, 4096, 1, &left).ok());
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 4096, 1, &right).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              account = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    ASSERT_TRUE(bind_block_account(left.get(), account, 1, &adopted).ok());
    ASSERT_TRUE(bind_block_account(right.get(), account, 1, &adopted).ok());
    std::shared_ptr<AllocationBudget> grant;
    ASSERT_TRUE(reserve_backend_operation(
                    left.get(), account, {.max_io_bytes = 1, .write_calls = 1, .workers = 1, .barriers = false}, &grant)
                    .ok());
    const uint8_t value = 3;
    EXPECT_EQ(right->write_at(4096, &value, 1, grant).code(), Code::kInvalidArgument);
    EXPECT_EQ(right->num_extents(), 1U);
    EXPECT_FALSE(backend_operation_bound(
        left.get(), {.max_io_bytes = SIZE_MAX, .write_calls = SIZE_MAX, .workers = SIZE_MAX, .barriers = true}));
    seal_backend_budget(grant);
    grant.reset();
    left.reset();
    right.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
}

TEST(BlockAccountGrant, DeletionOnNewBackendThreadUsesPrepaidMetadata)
{
    tree_test::TempDir              directory("block_grant_delete_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 4096, 1, &store).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              account = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    ASSERT_TRUE(bind_block_account(store.get(), account, 1, &adopted).ok());
    std::shared_ptr<AllocationBudget> grant;
    ASSERT_TRUE(reserve_backend_operation(store.get(), account, {}, &grant).ok());
    const auto admitted = account->snapshot();
    auto occupied = ResourceBudget::try_reserve(account, BudgetClass::kRouting,
                                                kCapacity - admitted.allocated[kRouting] - admitted.reserved[kRouting]);
    ASSERT_TRUE(occupied);
    Status      deleted;
    std::thread backend([&] { deleted = store->delete_block(0, grant); });
    backend.join();
    ASSERT_TRUE(deleted.ok()) << deleted.to_string();
    EXPECT_EQ(store->num_extents(), 0U);
    seal_backend_budget(grant);
    occupied.reset();
    store.reset();
    grant.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(account->snapshot().physical[kMetadata], 0U);
}

TEST(BlockAccountGrant, OpeningThreadMetadataUsesItsPrepaidParticipant)
{
    tree_test::TempDir              directory("block_grant_thread_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 4096, 1, &store).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              account = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    ASSERT_TRUE(bind_block_account(store.get(), account, 1, &adopted).ok());
    std::shared_ptr<AllocationBudget> grant;
    ASSERT_TRUE(reserve_backend_operation(store.get(), account,
                                          {.max_io_bytes = 1, .write_calls = 0, .workers = 1, .barriers = false},
                                          &grant)
                    .ok());
    const auto admitted = account->snapshot();
    auto occupied = ResourceBudget::try_reserve(account, BudgetClass::kRouting,
                                                kCapacity - admitted.allocated[kRouting] - admitted.reserved[kRouting]);
    ASSERT_TRUE(occupied);
    bool        succeeded = false;
    std::thread opening([&] {
        try {
            succeeded = backend_size(*store, grant) == 0;
        }
        catch (const std::bad_alloc &) {
            succeeded = false;
        }
    });
    opening.join();
    EXPECT_TRUE(succeeded);
    seal_backend_budget(grant);
    occupied.reset();
    store.reset();
    grant.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
}
} // namespace
} // namespace crowdb::tree
