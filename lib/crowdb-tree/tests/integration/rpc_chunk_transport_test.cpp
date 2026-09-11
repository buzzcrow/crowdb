// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/rpc_chunk_transport.h"
#include "chunkdb_generated.h"
#include "crowdb-rpc/c_api.h"
#include "diskdb_generated.h"
#include "msg_type_generated.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <vector>

namespace crowdb::tree::detail
{
namespace
{

using crowdb::chunkdb::proto::CreateFBAllocateChunkResponse;
using crowdb::chunkdb::proto::CreateFBChunk;
using crowdb::chunkdb::proto::CreateFBChunkStrip;
using crowdb::chunkdb::proto::CreateFBMirrorStrip;
using crowdb::chunkdb::proto::FBAllocateChunkRequest;
using crowdb::chunkdb::proto::FBChunkState_Active;
using crowdb::chunkdb::proto::FBChunkType_BtreePage;
using crowdb::chunkdb::proto::FBStripBody_FBMirrorStrip;
using crowdb::chunkdb::proto::FBStripType_Mirror;
using crowdb::diskdb::proto::FBSegment;
using crowdb::rpc::proto::FBInt128;
using crowdb::rpc::proto::FBMsgType_EAllocateChunkRequest;
using crowdb::rpc::proto::FBMsgType_EAllocateChunkResponse;

struct AllocateHandlerState
{
    crowdb_rpc_server_t server = nullptr;
    std::atomic<bool>   request_valid{false};
};

extern "C" void handle_allocate(uint64_t request_id, uint64_t, uint16_t, const uint8_t *control, uint32_t control_len,
                                const uint8_t *, uint32_t, void *connection, void *frame, void *user_data)
{
    auto                 *state = static_cast<AllocateHandlerState *>(user_data);
    flatbuffers::Verifier verifier(control, control_len);
    if (!verifier.VerifyBuffer<FBAllocateChunkRequest>(nullptr)) {
        crowdb_rpc_frame_release(frame);
        return;
    }
    const auto *request = flatbuffers::GetRoot<FBAllocateChunkRequest>(control);
    state->request_valid.store(request->chunk_id() == nullptr && request->write_granularity() == 256U * 1024U &&
                                   request->strip_count() == 1 && request->strip_type() == FBStripType_Mirror &&
                                   request->copy_count() == 3 && request->chunk_type() == FBChunkType_BtreePage &&
                                   request->writer_epoch() == 17,
                               std::memory_order_release);

    flatbuffers::FlatBufferBuilder builder;
    const FBInt128                 chunk_id(0x0200'0000'0000'0042ULL, 0x1234);
    const FBInt128                 disk_id(9, 10);
    std::vector<FBSegment>         segments;
    segments.reserve(3);
    for (uint64_t index = 0; index < 3; ++index) {
        segments.emplace_back(disk_id, chunk_id, index * 4096, 1, 2, 4096);
    }
    const auto segment_vector = builder.CreateVectorOfStructs(segments);
    const auto mirror         = CreateFBMirrorStrip(builder, segment_vector);
    const auto strip          = CreateFBChunkStrip(builder, 0, 0, 256U * 1024U, 256U * 1024U * 1024U, 1, 0, 0,
                                                   FBStripType_Mirror, FBStripBody_FBMirrorStrip, mirror.Union());
    const auto strips =
        builder.CreateVector(std::vector<flatbuffers::Offset<crowdb::chunkdb::proto::FBChunkStrip>>{strip});
    const auto chunk = CreateFBChunk(builder, &chunk_id, 3, FBChunkState_Active, 1, 0, 256U * 1024U * 1024U, 0, strips,
                                     FBChunkType_BtreePage, 17);
    const auto response = CreateFBAllocateChunkResponse(
        builder, request_id, 0, crowdb::chunkdb::proto::FBChunkdbRetCode_Success, 0, 0, 0, chunk);
    builder.Finish(response);
    static_cast<void>(crowdb_rpc_server_submit_response(state->server, connection, builder.GetBufferPointer(),
                                                        builder.GetSize(), nullptr, 0, FBMsgType_EAllocateChunkResponse,
                                                        request_id));
    crowdb_rpc_frame_release(frame);
}

TEST(RpcChunkTransport, AllocatesOneFullMirrorStripAndPreserves128BitChunkId)
{
    crowdb_rpc_pool_t   pool   = crowdb_rpc_pool_create(128);
    crowdb_rpc_server_t server = crowdb_rpc_server_create(pool);
    ASSERT_NE(server, nullptr);
    ASSERT_EQ(crowdb_rpc_server_listen(server, "127.0.0.1", 0), CROWDB_RPC_OK);
    AllocateHandlerState handler{.server = server};
    crowdb_rpc_server_register_handler(server, FBMsgType_EAllocateChunkRequest, handle_allocate, &handler);
    crowdb_rpc_server_start(server);

    crowdb_rpc_client_t client = crowdb_rpc_client_create();
    ASSERT_NE(client, nullptr);
    crowdb_rpc_client_set_completion_pool_size(client, 16);
    crowdb_rpc_conn_t connection = crowdb_rpc_connect(server, "127.0.0.1", crowdb_rpc_server_port(server));
    ASSERT_NE(connection, nullptr);
    crowdb_rpc_client_attach(client, connection);

    ct_chunk_rpc_disk_route disk_route{
        .disk_id_high = 9, .disk_id_low = 10, .route = {.client = client, .server = server, .connection = connection}
    };
    ct_chunk_rpc_transport_options options{
        .chunkdb             = {.client = client, .server = server, .connection = connection},
        .disk_routes         = &disk_route,
        .disk_route_count    = 1,
        .writer_lease_ms     = 5000,
        .rpc_timeout_ms      = 1000,
        .completion_capacity = 16,
    };
    RpcChunkTransport transport(options);
    ChunkId           allocated;
    ASSERT_TRUE(transport.allocate_mirror_chunk(256U * 1024U * 1024U, 17, &allocated).ok());
    EXPECT_TRUE(handler.request_valid.load(std::memory_order_acquire));
    EXPECT_EQ(allocated, ChunkId(0x0200'0000'0000'0042ULL, 0x1234));

    crowdb_rpc_conn_destroy(connection);
    crowdb_rpc_client_destroy(client);
    crowdb_rpc_server_stop(server);
    crowdb_rpc_server_destroy(server);
    crowdb_rpc_pool_destroy(pool);
}

} // namespace
} // namespace crowdb::tree::detail
