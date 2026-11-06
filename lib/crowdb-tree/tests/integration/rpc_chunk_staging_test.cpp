// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/rpc_chunk_transport.h"
#include "common/test_chunk_route_service.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
TEST(RpcChunkStaging, FixedIdentityIsInTheSubmittedAllocation)
{
    TestChunkRouteFixture          fixture;
    const ct_chunk_rpc_disk_route  disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
    ct_chunk_rpc_transport_options options{};
    options.disk_routes      = &disk;
    options.disk_route_count = 1;
    options.rpc_timeout_ms   = 50;
    options.chunkdb_resolver = fixture.resolver();
    RpcChunkTransport transport(options);
    const ChunkId     id(0x0200'0000'0000'0042ULL, 144);
    ASSERT_TRUE(transport.allocate_fixed_chunk(id, 4096, 17, PagePurpose::kBtreePage).ok());
    EXPECT_EQ(fixture.services[0].requested_low.load(), id.low);
    EXPECT_EQ(fixture.services[0].allocations.load(), 1U);
    EXPECT_EQ(fixture.services[1].allocations.load(), 0U);
}

TEST(RpcChunkStaging, UnknownAllocationDoesNotRetryWithAnotherIdentity)
{
    TestChunkRouteFixture fixture;
    fixture.services[1].drop_allocation = true;
    const ct_chunk_rpc_disk_route  disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
    ct_chunk_rpc_transport_options options{};
    options.disk_routes      = &disk;
    options.disk_route_count = 1;
    options.rpc_timeout_ms   = 50;
    options.chunkdb_resolver = fixture.resolver();
    RpcChunkTransport transport(options);
    const ChunkId     id(0x0200'0000'0000'0042ULL, 145);
    EXPECT_FALSE(transport.allocate_fixed_chunk(id, 4096, 17, PagePurpose::kBtreePage).ok());
    EXPECT_EQ(fixture.services[1].requested_low.load(), id.low);
    EXPECT_EQ(fixture.services[1].allocations.load(), 1U);
    EXPECT_EQ(fixture.services[0].allocations.load(), 0U);
}

TEST(RpcChunkStaging, DeleteRequiresACompleteTombstoneAndFencesMissingAllocations)
{
    TestChunkRouteFixture          fixture;
    const ct_chunk_rpc_disk_route  disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
    ct_chunk_rpc_transport_options options{};
    options.disk_routes      = &disk;
    options.disk_route_count = 1;
    options.rpc_timeout_ms   = 50;
    options.chunkdb_resolver = fixture.resolver();
    RpcChunkTransport transport(options);
    const ChunkId     id(0x0200'0000'0000'0042ULL, 144);
    fixture.services[0].unfinished_delete = true;
    EXPECT_FALSE(transport.delete_chunk(id).ok());
    EXPECT_TRUE(fixture.services[0].fence_missing.load());
    fixture.services[0].unfinished_delete = false;
    EXPECT_TRUE(transport.delete_chunk(id).ok());
    EXPECT_EQ(fixture.services[0].deletes.load(), 2U);
}
} // namespace crowdb::tree::detail
