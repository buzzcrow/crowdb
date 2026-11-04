// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/chunk_page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>

namespace crowdb::tree::detail
{
namespace
{

Status snapshot(Crowdbtree &tree, bool asynchronous)
{
    if (!asynchronous) {
        return tree.snapshot();
    }
    Status            result;
    std::atomic<bool> done{false};
    tree.snapshot_async([&](Status status, uint64_t /*unused*/) {
        result = std::move(status);
        done.store(true, std::memory_order_release);
        done.notify_one();
    });
    done.wait(false, std::memory_order_acquire);
    return result;
}

void check_snapshot_retry(bool asynchronous, bool write_while_unavailable = false)
{
    auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto           transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore store({.tree_id = 201, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
    Config         options;
    options.page_store       = &store;
    options.async_page_store = asynchronous ? &store : nullptr;
    Crowdbtree tree(options);
    ASSERT_TRUE(tree.apply(1, Batch{{{"key", OpKind::kPut, "old"}}}).ok());
    ASSERT_TRUE(tree.flush().ok());
    ASSERT_TRUE(snapshot(tree, asynchronous).ok());
    auto prior = catalog->load(201);
    ASSERT_NE(prior, nullptr);

    ASSERT_TRUE(tree.apply(2, Batch{{{"key", OpKind::kPut, "new"}}}).ok());
    ASSERT_TRUE(tree.flush().ok());
    store.inject_mirror_write_failures(1U << 1U);
    const Status failed = snapshot(tree, asynchronous);
    ASSERT_EQ(failed.code(), Code::kUnavailable) << failed.to_string();
    ASSERT_EQ(catalog->load(201)->generation, prior->generation);
    {
        ChunkPageStore durable_store({.tree_id = 201, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog,
                                     transport);
        Config         durable_options;
        durable_options.page_store = &durable_store;
        std::unique_ptr<Crowdbtree> durable;
        ASSERT_TRUE(Crowdbtree::open(durable_options, &durable).ok());
        std::string value;
        uint64_t    slot = 0;
        ASSERT_TRUE(durable->get(Slice("key"), &slot, &value));
        EXPECT_EQ(slot, 1U);
        EXPECT_EQ(value, "old");
    }
    if (write_while_unavailable) {
        ASSERT_TRUE(tree.apply(3, Batch{{{"key", OpKind::kPut, "newest"}}}).ok());
        ASSERT_TRUE(tree.flush().ok());
    }
    const Status still_unavailable = snapshot(tree, asynchronous);
    ASSERT_EQ(still_unavailable.code(), Code::kUnavailable) << still_unavailable.to_string();
    ASSERT_EQ(catalog->load(201)->generation, prior->generation);
    store.inject_mirror_write_failures(0);

    const Status retried = snapshot(tree, asynchronous);
    ASSERT_TRUE(retried.ok()) << retried.to_string();
    ASSERT_GT(catalog->load(201)->generation, prior->generation);
    ChunkPageStore reopened_store({.tree_id = 201, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog,
                                  transport);
    options.page_store       = &reopened_store;
    options.async_page_store = asynchronous ? &reopened_store : nullptr;
    std::unique_ptr<Crowdbtree> reopened;
    ASSERT_TRUE(Crowdbtree::open(options, &reopened).ok());
    std::string value;
    uint64_t    slot = 0;
    ASSERT_TRUE(reopened->get(Slice("key"), &slot, &value));
    EXPECT_EQ(slot, write_while_unavailable ? 3U : 2U);
    EXPECT_EQ(value, write_while_unavailable ? "newest" : "new");
}

TEST(ChunkSnapshotRetry, PublicationFailureDoesNotPoisonSynchronousSnapshotRetry)
{
    check_snapshot_retry(false);
}

TEST(ChunkSnapshotRetry, PublicationFailureDoesNotPoisonAsynchronousSnapshotRetry)
{
    check_snapshot_retry(true);
}

TEST(ChunkSnapshotRetry, SynchronousRetryPreservesWritesAfterFailedPublication)
{
    check_snapshot_retry(false, true);
}

TEST(ChunkSnapshotRetry, AsynchronousRetryPreservesWritesAfterFailedPublication)
{
    check_snapshot_retry(true, true);
}

} // namespace
} // namespace crowdb::tree::detail
