// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <gtest/gtest.h>

using crowdb::rpc::RdmaConfig;

TEST(RdmaConfigTest, PostedReceiveTargetMustFitRqAndRegisteredBudget)
{
    RdmaConfig config;
    EXPECT_TRUE(config.valid());
    config.posted_receive_target = config.rq_depth + 1;
    EXPECT_FALSE(config.valid());
    config.posted_receive_target = config.rq_depth;
    config.receive_arena_bytes   = static_cast<uint64_t>(config.posted_receive_target) * config.wire_slot_capacity - 1;
    EXPECT_FALSE(config.valid());
}

TEST(RdmaConfigTest, SmallWireSlotsAllowDeepRqWithinSmallArena)
{
    RdmaConfig config;
    config.wire_slot_capacity    = 1024;
    config.rq_depth              = 4096;
    config.posted_receive_target = 4096;
    config.receive_arena_bytes   = 4U << 20;
    config.cq_depth              = 8192;
    EXPECT_TRUE(config.valid());
    config.wire_slot_capacity = 4096;
    EXPECT_FALSE(config.valid());
}

TEST(RdmaConfigTest, LargeWireSlotsAnd4096ReceivesUse64BitArenaBudget)
{
    RdmaConfig config;
    config.rq_depth              = 4096;
    config.posted_receive_target = 4096;
    config.receive_arena_bytes   = uint64_t{4} << 30;
    config.cq_depth              = 8192;
    EXPECT_TRUE(config.valid());
    EXPECT_EQ(config.receive_arena_bytes, uint64_t{4096} * config.wire_slot_capacity);
}

#endif
