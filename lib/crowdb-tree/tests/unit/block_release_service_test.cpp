// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"
#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "test_tmp.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <thread>

namespace crowdb::tree
{
namespace
{
TEST(BlockReleaseService, BackendFinalOwnerQueuesDirectoryAndLateFileWithoutClosingOnCallback)
{
    tree_test::TempDir              path("block_release_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(path.path, 3, 4, 4096, 1, &store).ok());
    const uint8_t value = 37;
    ASSERT_TRUE(store->write_at(uint64_t{UINT32_MAX} * 4096, &value, 1).ok());
    auto service       = store->release_service();
    auto notifications = std::make_shared<std::atomic<size_t>>(0);
    ASSERT_TRUE(service
                    ->bind_wake(
                        notifications,
                        [](void *context) noexcept { static_cast<std::atomic<size_t> *>(context)->fetch_add(1); },
                        notifications.get())
                    .ok());
    uint64_t local  = 0;
    auto     reader = store->file_for_offset(uint64_t{UINT32_MAX} * 4096, &local);
    ASSERT_NE(reader, nullptr);
    const auto  fd = reader->fd();
    std::thread callback([owned = std::move(store)]() mutable { owned.reset(); });
    callback.join();
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_FALSE(service->drained());
    EXPECT_EQ(service->snapshot().pending, 1U);
    size_t steps = 0;
    while (service->snapshot().pending != 0 && steps < 256) {
        const auto step = service->pump(1);
        EXPECT_LE(step.visited, 1U);
        ++steps;
    }
    EXPECT_LT(steps, 256U);
    EXPECT_GT(steps, 1U);
    EXPECT_EQ(service->snapshot().registered, 1U);
    uint8_t actual = 0;
    EXPECT_TRUE(reader->pread_at(0, &actual, 1).ok());
    EXPECT_EQ(actual, value);
    std::thread late([owned = std::move(reader)]() mutable { owned.reset(); });
    late.join();
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_EQ(service->snapshot().pending, 1U);
    EXPECT_EQ(service->pump(1).completed, 1U);
    EXPECT_TRUE(service->drained());
    errno = 0;
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_GT(notifications->load(), 0U);
}

TEST(BlockReleaseService, ManualHostDestructionDrainsCurrentWorkAndPreservesLateReaders)
{
    tree_test::TempDir              path("block_manual_release_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(path.path, 3, 4, 4096, 1, &store).ok());
    auto service = store->release_service();
    auto account = std::dynamic_pointer_cast<detail::AllocationAccount>(service->allocation_budget());
    ASSERT_NE(account, nullptr);
    detail::ResourceBudget host(detail::test_allocation_account::limits(1 << 20), 1);
    auto                   ledger = host.make_account(detail::test_allocation_account::limits(1 << 20));
    ASSERT_TRUE(account->rebind(ledger).ok());
    uint64_t local  = 0;
    auto     reader = store->file_for_offset(0, &local);
    ASSERT_NE(reader, nullptr);
    const auto fd = reader->fd();
    store.reset();
    EXPECT_EQ(service->snapshot().pending, 0U);
    EXPECT_EQ(service->snapshot().registered, 1U);
    EXPECT_FALSE(service->drained());
    reader.reset();
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_EQ(service->pump(1).completed, 1U);
    EXPECT_TRUE(service->drained());
    EXPECT_GT(account->owned_bytes(), 0U);
    service.reset();
    // The stable account pays for its own object/control until its last owner.
    std::shared_ptr<detail::AllocationAccount> empty_account;
    ASSERT_TRUE(detail::AllocationAccount::create_unbound(&empty_account).ok());
    EXPECT_EQ(account->owned_bytes(), empty_account->owned_bytes());
    account.reset();
    EXPECT_EQ(ledger->snapshot().allocated[detail::test_allocation_account::kRouting], 0U);
    EXPECT_EQ(ledger->snapshot().physical[detail::test_allocation_account::kMetadata], 0U);
}

TEST(BlockReleaseService, FailedSingleFileOpenReturnsAllQueuedCloseControls)
{
    tree_test::TempDir              path("block_open_release_");
    std::unique_ptr<BlockPageStore> store;
    const auto                      status = BlockPageStore::open(path.path + "/missing/file", 1, &store);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(store);
}
} // namespace
} // namespace crowdb::tree
