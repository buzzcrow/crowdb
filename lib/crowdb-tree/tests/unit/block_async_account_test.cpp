// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#ifdef CROWDB_HAVE_LIBURING
#    include "allocation_account_test_support.h"
#    include "backend/local/block_account.h"
#    include "crowdb-tree/backend/async_page_store.h"
#    include "crowdb-tree/backend/block_page_store.h"
#    include "test_tmp.h"

#    include <chrono>
#    include <thread>

namespace crowdb::tree
{
namespace
{
using namespace detail;
using namespace detail::test_allocation_account;
constexpr size_t kCapacity = 1U << 20;

struct TestCompletion
{
    std::atomic<bool> done{false};
    Status            status;

    static void completed(void *context, Status status)
    {
        auto &self  = *static_cast<TestCompletion *>(context);
        self.status = std::move(status);
        self.done.store(true, std::memory_order_release);
    }

    AsyncCompletion callback(const std::shared_ptr<AllocationBudget> &budget)
    {
        done.store(false);
        return {.context = this, .complete_fn = &completed, .owner = {}, .allocations = budget};
    }

    [[nodiscard]] bool wait() const
    {
        for (size_t i = 0; i < 200; ++i) {
            if (done.load(std::memory_order_acquire)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return done.load(std::memory_order_acquire);
    }
};

TEST(BlockAsyncAccount, SplitControlsBounceAndBarrierSpendPrepaidCapacityAtTheHardLimit)
{
    tree_test::TempDir              directory("block_async_account_");
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 3, 4, 4096, 4096, &store).ok());
    ResourceBudget    host(limits(kCapacity), 1);
    auto              account = host.make_account(limits(kCapacity));
    BackendOpenBudget adopted;
    ASSERT_TRUE(bind_block_account(store.get(), account, 1, &adopted).ok());
    std::shared_ptr<AllocationBudget> grant;
    ASSERT_TRUE(reserve_backend_operation(
                    store.get(), account,
                    {.max_io_bytes = 8192, .write_calls = 1, .workers = 1, .barriers = true, .asynchronous = true},
                    &grant)
                    .ok());
    const auto totals = account->snapshot();
    auto occupied     = ResourceBudget::try_reserve(account, BudgetClass::kRouting,
                                                    kCapacity - totals.allocated[kRouting] - totals.reserved[kRouting]);
    ASSERT_TRUE(occupied);
    {
        ::crowdb::common::Topology topology;
        topology.pipelines.push_back({.entries = 16, .mode = ::crowdb::common::PollingMode::Classic});
        ::crowdb::common::DiskIOUring uring(std::move(topology));
        ASSERT_TRUE(uring.valid());
        BlockAsyncPageStore  backend(store.get(), &uring);
        std::vector<uint8_t> input(8193, 37);
        std::vector<uint8_t> output(8193, 0);
        TestCompletion       completion;
        backend.submit_write(0, input.data() + 1, 8192, completion.callback(grant));
        ASSERT_TRUE(completion.wait());
        ASSERT_TRUE(completion.status.ok()) << completion.status.to_string();
        for (size_t i = 0; i < 3; ++i) {
            backend.submit_read(0, output.data() + 1, 8192, completion.callback(grant));
            ASSERT_TRUE(completion.wait());
            ASSERT_TRUE(completion.status.ok()) << completion.status.to_string();
            EXPECT_TRUE(std::equal(input.begin() + 1, input.end(), output.begin() + 1));
        }
        ASSERT_TRUE(backend.submit_fsync(completion.callback(grant)).ok());
        ASSERT_TRUE(completion.wait());
        EXPECT_TRUE(completion.status.ok()) << completion.status.to_string();
    }
    seal_backend_budget(grant);
    occupied.reset();
    EXPECT_EQ(account->snapshot().reserved[kRouting], 0U);
    EXPECT_EQ(account->snapshot().physical[static_cast<size_t>(AllocationKind::kIo)], 0U);
    store.reset();
    grant.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(account->snapshot().physical[kMetadata], 0U);
}
} // namespace
} // namespace crowdb::tree
#endif
