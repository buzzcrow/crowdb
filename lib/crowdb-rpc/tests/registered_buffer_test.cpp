// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/buffer.h"

#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <cstring>
#include <thread>
#include <vector>

using namespace crowdb::rpc;

TEST(RegisteredBufferTest, HeldLeaseRetainsArenaAfterFacadeDestruction)
{
    auto  pool   = std::make_unique<RegisteredBufferPool>(4096, 4, 19);
    auto *buffer = pool->alloc(32);
    ASSERT_NE(buffer, nullptr);
    buffer->write("portable lease", 14);
    const auto         *data  = buffer->payload_data();
    auto               *clone = buffer->ref_clone();
    std::weak_ptr<void> arena = buffer->pool_owner;
    pool.reset();
    buffer->release();
    EXPECT_FALSE(arena.expired());
    EXPECT_EQ(clone->payload_data(), data);
    EXPECT_EQ(clone->registration_id, 19U);
    EXPECT_EQ(std::memcmp(data, "portable lease", 14), 0);
    clone->release();
    EXPECT_TRUE(arena.expired());
}

TEST(RegisteredBufferTest, SmallerProducerReusesLargerFreeAllocationWithinHardBudget)
{
    RegisteredBufferPool pool(RDMA_DATAPATH_CAPACITY, 1);
    auto                *large = pool.alloc(RDMA_DATAPATH_CAPACITY);
    ASSERT_NE(large, nullptr);
    const auto *pointer = large->data;
    large->release();
    auto *small = pool.alloc(32);
    ASSERT_NE(small, nullptr);
    EXPECT_EQ(small->data, pointer);
    EXPECT_EQ(small->capacity, RDMA_DATAPATH_CAPACITY);
    EXPECT_EQ(small->capacity_class, BufferCapacityClass::Datapath);
    EXPECT_EQ(pool.committed_bytes(), RDMA_DATAPATH_CAPACITY);
    EXPECT_EQ(pool.alloc(32), nullptr);
    small->release();
}

TEST(RegisteredBufferTest, QuarantinedNestedViewRetainsAndRetiresCanonicalAllocation)
{
    RegisteredBufferPool pool(1024, 1);
    auto                *root = pool.alloc(32);
    ASSERT_NE(root, nullptr);
    auto *view = root->slice(4, 16);
    ASSERT_NE(view, nullptr);
    auto *nested = view->slice(2, 8);
    ASSERT_NE(nested, nullptr);
    root->release();
    view->release();
    EXPECT_TRUE(nested->quarantine());
    EXPECT_EQ(nested->allocation_owner->ownership.load(), BufferOwnership::Quarantined);
    nested->release();
    EXPECT_EQ(pool.alloc(32), nullptr);
    EXPECT_EQ(pool.quarantined_bytes(), 1024U);
}

TEST(RegisteredBufferTest, DetachedLeaseRetainsRegionAndCanReleaseAfterFacade)
{
    auto  pool   = std::make_unique<RegisteredBufferPool>(4096, 4);
    auto *buffer = pool->alloc(32);
    ASSERT_NE(buffer, nullptr);
    buffer->write("detached lease", 14);
    ASSERT_TRUE(pool->detach(buffer));
    EXPECT_EQ(buffer->pool, nullptr);
    auto               *clone = buffer->ref_clone();
    std::weak_ptr<void> arena = buffer->pool_owner;
    pool.reset();
    buffer->release();
    EXPECT_FALSE(arena.expired());
    EXPECT_EQ(std::memcmp(clone->payload_data(), "detached lease", 14), 0);
    clone->release();
    EXPECT_TRUE(arena.expired());
}

TEST(RegisteredBufferTest, DetachedFinalReleaseReturnsDescriptorAdmission)
{
    RegisteredBufferPool pool(4096, 1);
    auto                *buffer = pool.alloc(1);
    ASSERT_NE(buffer, nullptr);
    ASSERT_TRUE(pool.detach(buffer));
    EXPECT_EQ(pool.alloc(1), nullptr);
    buffer->release();
    auto *next = pool.alloc(1);
    ASSERT_NE(next, nullptr);
    EXPECT_EQ(pool.committed_bytes(), 2048U);
    next->release();
}

TEST(RegisteredBufferTest, CachedAndQuarantinedDescriptorsStayWithinTotalLimit)
{
    RegisteredBufferPool pool(2 * RDMA_DATAPATH_CAPACITY, 2);
    auto                *control  = pool.alloc(1);
    auto                *metadata = pool.alloc(RDMA_METADATA_CAPACITY);
    ASSERT_NE(control, nullptr);
    ASSERT_NE(metadata, nullptr);
    control->release();
    EXPECT_TRUE(metadata->quarantine());
    metadata->release();
    EXPECT_EQ(pool.alloc(RDMA_DATAPATH_CAPACITY), nullptr);
    EXPECT_EQ(pool.quarantined_bytes(), RDMA_METADATA_CAPACITY);
    auto *reused = pool.alloc(1);
    ASSERT_EQ(reused, control);
    reused->release();
}

TEST(RegisteredBufferTest, DetachedQuarantineRemainsChargedAndRangeIsNotReused)
{
    RegisteredBufferPool pool(4096, 1);
    auto                *buffer = pool.alloc(1);
    ASSERT_NE(buffer, nullptr);
    const auto *old_range = buffer->data;
    ASSERT_TRUE(pool.detach(buffer));
    ASSERT_TRUE(buffer->quarantine());
    buffer->release();
    EXPECT_EQ(pool.quarantined_bytes(), 1024U);
    auto *next = pool.alloc(1);
    ASSERT_NE(next, nullptr);
    EXPECT_NE(next->data, old_range);
    next->release();
}

TEST(RegisteredBufferTest, ConcurrentRecyclingKeepsSlotOwnershipExclusive)
{
    RegisteredBufferPool              pool(32 * 1024, 32);
    std::array<std::atomic<bool>, 32> occupied{};
    std::atomic<uint64_t>             errors{0};
    std::vector<std::thread>          threads;
    for (uint8_t producer = 1; producer <= 8; ++producer) {
        threads.emplace_back([&, producer] {
            for (uint32_t iteration = 0; iteration < 20000; ++iteration) {
                auto *buffer = pool.alloc(32);
                if (buffer == nullptr) {
                    errors.fetch_add(1);
                    continue;
                }
                if (occupied[buffer->pool_index].exchange(true))
                    errors.fetch_add(1);
                std::memset(buffer->data, producer, 32);
                auto *clone = buffer->ref_clone();
                for (uint32_t index = 0; index < 32; ++index)
                    if (clone->data[index] != producer)
                        errors.fetch_add(1);
                buffer->release();
                occupied[clone->pool_index].store(false);
                clone->release();
            }
        });
    }
    for (auto &thread : threads)
        thread.join();
    EXPECT_EQ(errors.load(), 0U);
    EXPECT_LE(pool.committed_bytes(), 32U * 1024);
}

TEST(RegisteredBufferTest, SystemAdmissionNeverExceedsConcurrentCapacity)
{
    SystemBufferPool         pool(1);
    std::barrier             start(8);
    std::atomic<uint32_t>    live{0};
    std::atomic<uint32_t>    errors{0};
    std::vector<std::thread> threads;
    for (uint32_t producer = 0; producer < 8; ++producer) {
        threads.emplace_back([&] {
            start.arrive_and_wait();
            for (uint32_t iteration = 0; iteration < 20000; ++iteration) {
                if (auto *buffer = pool.alloc(32); buffer != nullptr) {
                    if (live.fetch_add(1) != 0)
                        errors.fetch_add(1);
                    std::this_thread::yield();
                    live.fetch_sub(1);
                    buffer->release();
                }
            }
        });
    }
    for (auto &thread : threads)
        thread.join();
    EXPECT_EQ(live.load(), 0U);
    EXPECT_EQ(errors.load(), 0U);
}
