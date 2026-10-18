// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/buffer.h"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>

using crowdb::rpc::Buffer;
using crowdb::rpc::BufferCapacityClass;
using crowdb::rpc::BufferOwnership;
using crowdb::rpc::BufferType;
using crowdb::rpc::RegisteredBufferPool;
using crowdb::rpc::SystemBufferPool;

TEST(BufferTest, AllocReturnsValidBuffer)
{
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(1024);
    ASSERT_NE(buf, nullptr);
    EXPECT_GE(buf->capacity, 1024U);
    EXPECT_EQ(buf->len, 0U);
    EXPECT_EQ(buf->type, BufferType::System);
    // ref == 1 after alloc
    EXPECT_EQ(buf->ref->load(), 1);
    buf->release();
}

TEST(BufferTest, WriteSetsLenAndBytes)
{
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(1024);
    ASSERT_NE(buf, nullptr);

    const uint8_t data[] = {1, 2, 3, 4, 5};
    buf->write(data, 5);
    EXPECT_EQ(buf->len, 5U);
    EXPECT_EQ(std::memcmp(buf->data, data, 5), 0);
    buf->release();
}

TEST(BufferTest, RefCloneAndRelease)
{
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(512);
    ASSERT_NE(buf, nullptr);
    uint8_t *original_ptr = buf->data;

    Buffer *ref1 = buf->ref_clone();
    EXPECT_EQ(buf->ref->load(), 2);
    EXPECT_EQ(ref1->data, original_ptr);

    // Release one — ref == 1, buffer not freed yet
    ref1->release();
    EXPECT_EQ(buf->ref->load(), 1);

    // Release the other — ref == 0, buffer freed (direct delete, no pool reuse)
    buf->release();
}

TEST(BufferTest, PoolExhaustedReturnsNull)
{
    SystemBufferPool pool(2); // max 2 outstanding
    Buffer          *a = pool.alloc(64);
    Buffer          *b = pool.alloc(64);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(pool.alloc(64), nullptr); // exhausted
    a->release();                       // frees buffer, outstanding drops
    Buffer *c = pool.alloc(64);         // should succeed now
    ASSERT_NE(c, nullptr);
    b->release();
    c->release();
}

TEST(BufferTest, ExactCapacityNoBucketing)
{
    // Direct allocation — capacity is exactly what was requested (no
    // power-of-2 bucket rounding). This differs from the old pool which
    // rounded up to the next power of 2.
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(200);
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(buf->capacity, 200U); // exact, not bucketed to 256
    buf->release();
}

TEST(BufferTest, DetachedReceiveBufferOutlivesPoolWithoutCopy)
{
    auto    pool = std::make_unique<SystemBufferPool>(1);
    Buffer *buf  = pool->alloc(4);
    ASSERT_NE(buf, nullptr);
    const uint8_t data[] = {1, 2, 3, 4};
    buf->write(data, 4);
    uint8_t *original = buf->data;

    EXPECT_TRUE(pool->detach(buf));
    EXPECT_EQ(buf->data, original);
    Buffer *next = pool->alloc(4);
    ASSERT_NE(next, nullptr);
    next->release();
    pool.reset();

    EXPECT_EQ(std::memcmp(buf->data, data, 4), 0);
    buf->release();
}

TEST(BufferTest, RegisteredPoolRoundsClassesAndEnforcesByteBudget)
{
    RegisteredBufferPool pool(crowdb::rpc::RDMA_METADATA_CAPACITY, 4, 7);
    Buffer              *metadata = pool.alloc(1024);
    ASSERT_NE(metadata, nullptr);
    EXPECT_EQ(metadata->capacity, crowdb::rpc::RDMA_CONTROL_CAPACITY);
    EXPECT_EQ(metadata->capacity_class, BufferCapacityClass::Control);
    EXPECT_EQ(metadata->registration_id, 7U);
    EXPECT_EQ(pool.alloc(crowdb::rpc::RDMA_METADATA_CAPACITY), nullptr);
    metadata->release();
    Buffer *reused = pool.alloc(512);
    ASSERT_NE(reused, nullptr);
    reused->release();
}

TEST(BufferTest, RegisteredLeaseOwnershipAndQuarantine)
{
    RegisteredBufferPool pool(2 * crowdb::rpc::RDMA_CONTROL_CAPACITY, 2);
    Buffer              *buf = pool.alloc(1);
    ASSERT_NE(buf, nullptr);
    EXPECT_TRUE(buf->transition(BufferOwnership::Available, BufferOwnership::PostedReceive));
    EXPECT_TRUE(buf->transition(BufferOwnership::PostedReceive, BufferOwnership::ReceiveInFlight));
    EXPECT_TRUE(buf->transition(BufferOwnership::ReceiveInFlight, BufferOwnership::CallerOwned));
    EXPECT_FALSE(buf->transition(BufferOwnership::ReceiveInFlight, BufferOwnership::Available));
    EXPECT_TRUE(buf->quarantine());
    buf->release();
    EXPECT_EQ(pool.quarantined_bytes(), crowdb::rpc::RDMA_CONTROL_CAPACITY);
    Buffer *other = pool.alloc(1);
    ASSERT_NE(other, nullptr);
    EXPECT_NE(other, buf);
    other->release();
    Buffer *reused = pool.alloc(1);
    ASSERT_NE(reused, nullptr);
    EXPECT_EQ(reused, other);
    reused->release();
}

TEST(BufferTest, RegisteredCapacityRejectsUnsupportedValues)
{
    RegisteredBufferPool pool(8 * crowdb::rpc::RDMA_DATAPATH_CAPACITY, 8);
    EXPECT_EQ(pool.alloc(crowdb::rpc::RDMA_DATAPATH_CAPACITY + 1), nullptr);
    EXPECT_EQ(pool.alloc(0), nullptr);
}
