// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_environment.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <gtest/gtest.h>

using crowdb::rpc::RdmaConfig;

TEST(RdmaConfigTest, ControlDataChargesBothRegionsAndRequiresTwoSges)
{
    RdmaConfig config;
    config.wire_layout         = crowdb::rpc::RdmaWireLayout::ControlData;
    config.wire_slot_capacity  = crowdb::rpc::RDMA_DATAPATH_CAPACITY + crowdb::rpc::RDMA_CONTROL_CAPACITY;
    config.receive_arena_bytes = uint64_t{config.posted_receive_target} * config.wire_slot_capacity;
    EXPECT_TRUE(config.valid());
    EXPECT_EQ(config.data_slot_capacity(), crowdb::rpc::RDMA_DATAPATH_CAPACITY);
    EXPECT_EQ(config.receive_control_bytes(), uint64_t{config.posted_receive_target} * 1024);
    --config.receive_arena_bytes;
    EXPECT_FALSE(config.valid());
    ++config.receive_arena_bytes;
    config.aggregation_enabled = true;
    EXPECT_FALSE(config.valid());
    config.aggregation_enabled = false;
    config.qp_max_send_sge     = 1;
    EXPECT_FALSE(config.valid());
    config.qp_max_send_sge = 2;
    config.qp_max_recv_sge = 1;
    EXPECT_FALSE(config.valid());
    config.qp_max_recv_sge = 2;
    config.wire_layout     = static_cast<crowdb::rpc::RdmaWireLayout>(2);
    EXPECT_FALSE(config.valid());
}

TEST(RdmaConfigTest, EnvironmentRejectsUnknownWireLayout)
{
    const TestEnvironment valid("CROWDB_RDMA_WIRE_LAYOUT", "1");
    RdmaConfig            config;
    ASSERT_TRUE(config.apply_environment());
    EXPECT_EQ(config.wire_layout, crowdb::rpc::RdmaWireLayout::ControlData);
    const TestEnvironment invalid("CROWDB_RDMA_WIRE_LAYOUT", "2");
    EXPECT_FALSE(config.apply_environment());
}

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

TEST(RdmaConfigTest, EnvironmentWireSlotCapacityMatchesRunnerContract)
{
    const TestEnvironment slot("CROWDB_RDMA_WIRE_SLOT_CAPACITY", "1024");
    const TestEnvironment receive_bytes("CROWDB_RDMA_RECEIVE_ARENA_BYTES", "1048576");
    RdmaConfig            config;
    ASSERT_TRUE(config.apply_environment());
    EXPECT_EQ(config.wire_slot_capacity, 1024U);
    EXPECT_TRUE(config.valid());
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

TEST(RdmaConfigTest, CqCountAndWorkTokensAreBounded)
{
    RdmaConfig config;
    config.cq_count = 0;
    EXPECT_FALSE(config.valid());
    config.cq_count = 257;
    EXPECT_FALSE(config.valid());
    config.cq_count = 256;
    EXPECT_TRUE(config.valid());
    config.cq_depth = INT32_MAX;
    EXPECT_FALSE(config.valid());
}

TEST(RdmaConfigTest, PollingBudgetsMustBeNonzeroAndFitTheVerbsBatch)
{
    RdmaConfig config;
    config.poll_batch = 0;
    EXPECT_FALSE(config.valid());
    config.poll_batch       = 256;
    config.verbs_poll_batch = 0;
    EXPECT_FALSE(config.valid());
    config.verbs_poll_batch = 257;
    EXPECT_FALSE(config.valid());
    config.verbs_poll_batch = 256;
    EXPECT_TRUE(config.valid());
    config.poll_empty_budget = 0;
    EXPECT_FALSE(config.valid());
}

TEST(RdmaConfigTest, EnvironmentParsesCqAndPollingKnobsStrictly)
{
    const TestEnvironment count("CROWDB_RDMA_CQ_COUNT", "2");
    const TestEnvironment batch("CROWDB_RDMA_VERBS_POLL_BATCH", "64");
    RdmaConfig            config;
    ASSERT_TRUE(config.apply_environment());
    EXPECT_EQ(config.cq_count, 2U);
    EXPECT_EQ(config.verbs_poll_batch, 64U);
    EXPECT_TRUE(config.valid());
    const TestEnvironment invalid("CROWDB_RDMA_CQ_COUNT", "2x");
    EXPECT_FALSE(config.apply_environment());
}

TEST(RdmaConfigTest, RegistrationBudgetIncludesAdmittedQpsAndCopyPools)
{
    RdmaConfig config;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 8;
    config.wire_slot_capacity                                        = 1024;
    config.send_arena_bytes = config.receive_arena_bytes = 8192;
    config.cq_depth                                      = 16;
    EXPECT_EQ(config.registration_budget(), 32768U);
    config.cq_depth = 32;
    config.cq_count = 2;
    EXPECT_EQ(config.registration_budget(), 81920U);
    config.registered_memory_budget_bytes = 32767;
    EXPECT_FALSE(config.valid());
    config.registered_memory_budget_bytes = 32768;
    EXPECT_TRUE(config.valid());
    config.receive_arena_bytes = UINT64_MAX;
    EXPECT_FALSE(config.valid());
    config.registered_memory_budget_bytes = 0;
    EXPECT_EQ(config.registration_budget(), 0U);
}

TEST(RdmaConfigTest, RegistrationBudgetParses64BitValuesStrictly)
{
    const TestEnvironment budget("CROWDB_RDMA_REGISTERED_MEMORY_BUDGET_BYTES", "8589934592");
    RdmaConfig            config;
    ASSERT_TRUE(config.apply_environment());
    EXPECT_EQ(config.registered_memory_budget_bytes, uint64_t{8} << 30);
    EXPECT_TRUE(config.valid());
    const TestEnvironment invalid("CROWDB_RDMA_REGISTERED_MEMORY_BUDGET_BYTES", "-1");
    EXPECT_FALSE(config.apply_environment());
}

TEST(RdmaConfigTest, AggregationAndQueueLimitsRejectInvalidBounds)
{
    RdmaConfig config;
    config.aggregation_enabled    = true;
    config.aggregation_max_frames = 257;
    EXPECT_FALSE(config.valid());
    config.aggregation_max_frames = 0;
    EXPECT_FALSE(config.valid());
    config.aggregation_max_frames = 256;
    config.aggregation_max_bytes  = 0;
    EXPECT_FALSE(config.valid());
    config.aggregation_max_bytes = 1024;
    EXPECT_TRUE(config.valid());
    config.send_queue_capacity = 3;
    EXPECT_FALSE(config.valid());
    config.send_queue_capacity = 1U << 31;
    EXPECT_FALSE(config.valid());
    config.send_queue_capacity = 4096;
    EXPECT_TRUE(config.valid());
    const TestEnvironment enabled("CROWDB_RDMA_AGGREGATION_ENABLED", "1");
    EXPECT_TRUE(config.apply_environment());
    EXPECT_TRUE(config.aggregation_enabled);
    const TestEnvironment invalid("CROWDB_RDMA_AGGREGATION_ENABLED", "yes");
    EXPECT_FALSE(config.apply_environment());
}

#endif
