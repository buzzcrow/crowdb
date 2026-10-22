// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/buffer.h"

#include <gtest/gtest.h>

#include <barrier>
#include <cstring>
#include <memory>
#include <thread>

TEST(SystemBufferPoolTest, HeldLeaseReleasesAfterFacadeDestruction)
{
    auto  pool   = std::make_unique<crowdb::rpc::SystemBufferPool>(1);
    auto *buffer = pool->alloc(16);
    ASSERT_NE(buffer, nullptr);
    buffer->write("producer payload", 16);
    auto *second_owner = buffer->ref_clone();
    ASSERT_NE(second_owner, nullptr);
    EXPECT_EQ(pool->outstanding(), 1U);
    EXPECT_EQ(pool->alloc(1), nullptr);
    pool.reset();
    EXPECT_EQ(std::memcmp(buffer->payload_data(), "producer payload", 16), 0);
    buffer->release();
    EXPECT_EQ(std::memcmp(second_owner->payload_data(), "producer payload", 16), 0);
    second_owner->release();
}

TEST(SystemBufferPoolTest, FacadeDestructionRacesFinalLeaseReleaseSafely)
{
    for (uint32_t iteration = 0; iteration < 1000; ++iteration) {
        auto  pool   = std::make_unique<crowdb::rpc::SystemBufferPool>(1);
        auto *buffer = pool->alloc(16);
        ASSERT_NE(buffer, nullptr);
        std::barrier ready(2);
        std::jthread release([buffer, &ready] {
            ready.arrive_and_wait();
            buffer->release();
        });
        ready.arrive_and_wait();
        pool.reset();
    }
}
