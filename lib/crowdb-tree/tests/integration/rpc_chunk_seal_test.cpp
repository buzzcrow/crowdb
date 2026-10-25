// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/rpc_chunk_transport.h"
#include "common/test_chunk_route_service.h"

#include <gtest/gtest.h>

#include <cstdint>

namespace crowdb::tree::detail
{
namespace
{
TEST(RpcChunkTransport, SealsEmptyAndPartialKibBoundariesWithExactBytes)
{
    for (const uint64_t bytes : {uint64_t{0}, uint64_t{1025}}) {
        TestChunkRouteFixture          fixture;
        const ct_chunk_rpc_disk_route  disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
        ct_chunk_rpc_transport_options options{};
        options.disk_routes      = &disk;
        options.disk_route_count = 1;
        options.chunkdb_resolver = fixture.resolver();
        RpcChunkTransport transport(options);
        ChunkId           allocated;
        ASSERT_TRUE(
            transport.allocate_mirror_chunk(256U * 1024U * 1024U, 17, &allocated, PagePurpose::kBtreePage).ok());
        if (bytes != 0) {
            ASSERT_TRUE(transport.advance_write(allocated, 0, bytes).ok());
        }
        ASSERT_TRUE(transport.seal_chunk(allocated, 17, bytes).ok());
        EXPECT_EQ(fixture.services[0].sealed_bytes.load(), bytes);
        ChunkLayout sealed;
        ASSERT_TRUE(transport.query_chunk(allocated, &sealed).ok());
        EXPECT_TRUE(sealed.sealed);
        EXPECT_EQ(sealed.acknowledged_bytes, bytes);
        if (bytes == 0) {
            EXPECT_EQ(sealed.logical_capacity, 0);
        }
    }
}

TEST(RpcChunkTransport, RejectsASealReplyWithoutTheExactSealedBoundary)
{
    for (const auto fault : {TestSealReply::Active, TestSealReply::WrongCursor}) {
        TestChunkRouteFixture fixture;
        fixture.services[0].seal_reply = fault;
        const ct_chunk_rpc_disk_route  disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
        ct_chunk_rpc_transport_options options{};
        options.disk_routes      = &disk;
        options.disk_route_count = 1;
        options.chunkdb_resolver = fixture.resolver();
        RpcChunkTransport transport(options);
        ChunkId           allocated;
        ASSERT_TRUE(
            transport.allocate_mirror_chunk(256U * 1024U * 1024U, 17, &allocated, PagePurpose::kBtreePage).ok());
        ASSERT_TRUE(transport.advance_write(allocated, 0, 4).ok());
        EXPECT_FALSE(transport.seal_chunk(allocated, 17, 4).ok());
    }
}
} // namespace
} // namespace crowdb::tree::detail
