// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"
#include "backend/local/block_account.h"
#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "crowdb-tree/epoch.h"
#include "test_tmp.h"

#include <fcntl.h>

#include <cerrno>
#include <filesystem>

namespace crowdb::tree
{
namespace
{
using namespace detail;
using namespace detail::test_allocation_account;
constexpr size_t kCapacity = 1U << 20;

TEST(BlockAccount, StartupTracksSparseNodesControlAndEpochBeforeFirstBinding)
{
    tree_test::TempDir              directory("block_account_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 4096, 1, &store).ok());
    const uint8_t value = 19;
    ASSERT_TRUE(store->write_at(uint64_t{UINT32_MAX} * 4096, &value, 1).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              account = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    ASSERT_TRUE(bind_block_account(store.get(), account, 2, &adopted).ok());
    EXPECT_GT(adopted.owned_bytes, sizeof(BlockPageStore));
    EXPECT_EQ(adopted.growth_bytes, 3 * EpochManager::participant_allocation_bytes());
    EXPECT_EQ(host.snapshot().allocated[kRouting], adopted.owned_bytes);
    EXPECT_EQ(account->snapshot().physical[kMetadata], adopted.owned_bytes);
    EXPECT_EQ(store->num_extents(), 2U);
    store.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(account->snapshot().physical[kMetadata], 0U);
}

TEST(BlockAccount, SameHostReopenTransfersFdOwnersAndRejectsSmallerOrForeignAccount)
{
    tree_test::TempDir              directory("block_transfer_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 4096, 1, &store).ok());
    const uint8_t value = 23;
    ASSERT_TRUE(store->write_at(4096, &value, 1).ok());
    uint64_t local   = 0;
    auto     held    = store->file_for_offset(4096, &local);
    auto     release = store->release_service();
    ASSERT_TRUE(held);
    const auto        fd = held->fd();
    ResourceBudget    host(limits(kCapacity), 1);
    auto              old  = host.make_account(limits(kCapacity));
    auto              next = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    ASSERT_TRUE(bind_block_account(store.get(), old, 1, &adopted).ok());
    ASSERT_TRUE(store->delete_block(1).ok());
    const auto retained = old->snapshot().allocated[kRouting];
    auto       small    = host.make_account(limits(retained / 2));
    EXPECT_EQ(bind_block_account(store.get(), small, 1, &adopted).code(), Code::kResourceExhausted);
    EXPECT_EQ(old->snapshot().allocated[kRouting], retained);
    EXPECT_EQ(small->snapshot().allocated[kRouting], 0U);
    ResourceBudget foreign(limits(kCapacity), 1);
    auto           elsewhere = foreign.make_account(limits(kCapacity));
    EXPECT_EQ(bind_block_account(store.get(), elsewhere, 1, &adopted).code(), Code::kInvalidArgument);
    ASSERT_TRUE(bind_block_account(store.get(), next, 1, &adopted).ok());
    EXPECT_EQ(old->snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(old->snapshot().physical[kMetadata], 0U);
    EXPECT_EQ(next->snapshot().allocated[kRouting], retained);
    EXPECT_EQ(host.snapshot().allocated[kRouting], retained);
    store.reset();
    EXPECT_GT(next->snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(release->snapshot().registered, 1U);
    EXPECT_EQ(release->snapshot().pending, 0U);
    EXPECT_FALSE(release->drained());
    uint8_t actual = 0;
    ASSERT_TRUE(held->pread_at(0, &actual, 1).ok());
    EXPECT_EQ(actual, value);
    held.reset();
    // This standalone host owns the explicit release boundary after its late
    // file reader. Final owner destruction itself must not close the fd.
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_EQ(release->snapshot().pending, 1U);
    EXPECT_EQ(release->pump(1).completed, 1U);
    EXPECT_TRUE(release->drained());
    errno = 0;
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    release.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(next->snapshot().physical[kMetadata], 0U);
}

TEST(BlockAccount, FailedFirstAdoptionAndRejectedGrowthLeaveFilesAndExistingReadsIntact)
{
    tree_test::TempDir              directory("block_quota_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 4096, 1, &store).ok());
    const uint8_t value = 29;
    ASSERT_TRUE(store->write_at(0, &value, 1).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              small = host.make_account(limits(1));
    BackendOpenBudget adopted;
    EXPECT_EQ(bind_block_account(store.get(), small, 1, &adopted).code(), Code::kResourceExhausted);
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    auto account = host.make_account(limits(kCapacity));
    ASSERT_TRUE(bind_block_account(store.get(), account, 1, &adopted).ok());
    auto occupied = use_remaining_memory(account, block_allocation_budget(*store));
    EXPECT_EQ(store->write_at(4096000, &value, 1).code(), Code::kResourceExhausted);
    EXPECT_FALSE(std::filesystem::exists(directory.path + "/3-4.blk-1000"));
    EXPECT_EQ(store->num_extents(), 1U);
    uint8_t actual = 0;
    ASSERT_TRUE(store->read_at(0, &actual, 1).ok());
    EXPECT_EQ(actual, value);
    occupied.reset();
    ASSERT_TRUE(store->write_at(4096000, &value, 1).ok());
    EXPECT_GT(account->snapshot().allocated[kRouting], adopted.owned_bytes);
    store.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(account->snapshot().physical[kMetadata], 0U);
}
} // namespace
} // namespace crowdb::tree
