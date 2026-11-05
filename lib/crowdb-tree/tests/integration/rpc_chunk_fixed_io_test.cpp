// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/rpc_chunk_transport.h"
#include "common/test_chunk_route_service.h"
#include "diskio_generated.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
struct TestFixedIoService
{
    crowdb_rpc_server_t   server = nullptr;
    std::atomic<bool>     age_receipt{false};
    std::atomic<bool>     sync_receipt{false};
    std::atomic<uint64_t> timestamp{0};
    std::atomic<bool>     sync_requested{false};
    std::atomic<uint32_t> write_size{UINT32_MAX};
    std::atomic<uint64_t> ordering_offset{UINT64_MAX};

    static void handle(uint64_t request_id, uint64_t /*unused*/, uint16_t message, const uint8_t *control,
                       uint32_t length, const uint8_t * /*unused*/, uint32_t /*unused*/, void *connection, void *frame,
                       void *context)
    {
        auto &self = *static_cast<TestFixedIoService *>(context);
        self.respond(request_id, message, control, length, connection);
        crowdb_rpc_frame_release(frame);
    }

    void respond(uint64_t id, uint16_t message, const uint8_t *control, uint32_t length, void *connection)
    {
        namespace dio = crowdb::diskio::proto;
        namespace rpc = crowdb::rpc::proto;
        flatbuffers::Verifier          verifier(control, length);
        flatbuffers::FlatBufferBuilder builder;
        uint16_t                       response = 0;
        uint32_t                       bytes    = 0;
        const std::array<uint8_t, 4>   data{1, 2, 3, 4};
        if (message == rpc::FBMsgType_EDiskReadRequest) {
            ASSERT_TRUE(verifier.VerifyBuffer<dio::FBDiskReadRequest>(nullptr));
            const auto *request = flatbuffers::GetRoot<dio::FBDiskReadRequest>(control);
            timestamp.store(request->read_create_time_ms());
            bytes = request->size();
            ASSERT_LE(bytes, data.size());
            builder.Finish(
                dio::CreateFBDiskReadResponse(builder, id, 0, dio::FBDiskIoRetCode_Success, age_receipt.load()));
            response = rpc::FBMsgType_EDiskReadResponse;
        }
        else {
            ASSERT_TRUE(verifier.VerifyBuffer<dio::FBDiskWriteRequest>(nullptr));
            const auto *request = flatbuffers::GetRoot<dio::FBDiskWriteRequest>(control);
            timestamp.store(request->write_create_time_ms());
            sync_requested.store(request->sync());
            write_size.store(request->size());
            ordering_offset.store(request->ordering_zone_offset());
            builder.Finish(
                dio::CreateFBDiskWriteResponse(builder, id, 0, dio::FBDiskIoRetCode_Success, sync_receipt.load()));
            response = rpc::FBMsgType_EDiskWriteResponse;
        }
        static_cast<void>(crowdb_rpc_server_submit_response(server, connection, builder.GetBufferPointer(),
                                                            builder.GetSize(), bytes == 0 ? nullptr : data.data(),
                                                            bytes, response, id));
    }
};

class RpcChunkFixedIo : public ::testing::Test
{
  protected:
    TestChunkRouteFixture              fixture;
    TestFixedIoService                 io;
    std::unique_ptr<RpcChunkTransport> transport;
    const ChunkId                      id{0x0200'0000'0000'0042ULL, 144};

    void SetUp() override
    {
        io.server = fixture.services[0].server;
        for (auto message :
             {crowdb::rpc::proto::FBMsgType_EDiskReadRequest, crowdb::rpc::proto::FBMsgType_EDiskWriteRequest}) {
            crowdb_rpc_server_register_handler(io.server, message, TestFixedIoService::handle, &io, nullptr);
        }
        const ct_chunk_rpc_disk_route  disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
        ct_chunk_rpc_transport_options options{};
        options.disk_routes      = &disk;
        options.disk_route_count = 1;
        options.rpc_timeout_ms   = 50;
        options.chunkdb_resolver = fixture.resolver();
        transport                = std::make_unique<RpcChunkTransport>(options);
        ASSERT_TRUE(transport->allocate_fixed_chunk(id, 4096, 17, PagePurpose::kBtreePage).ok());
    }
};
} // namespace

TEST_F(RpcChunkFixedIo, ReadRequiresNewServerReceiptAndKeepsOriginalRequestAge)
{
    const ChunkIoContext context{.created_ms = 1234};
    EXPECT_EQ(transport->verify_fixed_read_fence(id, 1, context).code(), Code::kNotSupported);
    io.age_receipt.store(true);
    ASSERT_TRUE(transport->verify_fixed_read_fence(id, 1, context).ok());
    EXPECT_EQ(io.timestamp.load(), context.created_ms);
    ASSERT_TRUE(transport->advance_write(id, 0, 4).ok());
    std::array<uint8_t, 4> data{};
    ASSERT_TRUE(transport->read_fixed_mirror(id, 0, 0, data.data(), data.size(), context).ok());
    EXPECT_EQ(io.timestamp.load(), context.created_ms);
    io.age_receipt.store(false);
    EXPECT_EQ(transport->read_fixed_mirror(id, 0, 0, data.data(), data.size(), context).code(), Code::kNotSupported);
    EXPECT_TRUE(transport->read_mirror(id, 0, 0, data.data(), data.size()).ok());
}

TEST_F(RpcChunkFixedIo, DrainRequiresZeroByteSynchronizedBarrier)
{
    EXPECT_EQ(transport->drain_fixed_chunk(id).code(), Code::kNotSupported);
    EXPECT_TRUE(io.sync_requested.load());
    EXPECT_EQ(io.write_size.load(), 0U);
    EXPECT_NE(io.ordering_offset.load(), UINT64_MAX);
    io.sync_receipt.store(true);
    EXPECT_TRUE(transport->drain_fixed_chunk(id).ok());
    EXPECT_EQ(io.write_size.load(), 0U);
}

TEST_F(RpcChunkFixedIo, WriteRequiresSynchronizedReceiptAndKeepsOriginalRequestAge)
{
    const ChunkIoContext         context{.created_ms = 5678};
    const std::array<uint8_t, 4> data{1, 2, 3, 4};
    EXPECT_EQ(transport->write_fixed_mirror(id, 0, 0, data.data(), data.size(), context).code(), Code::kNotSupported);
    EXPECT_TRUE(io.sync_requested.load());
    EXPECT_EQ(io.timestamp.load(), context.created_ms);
    io.sync_receipt.store(true);
    EXPECT_TRUE(transport->write_fixed_mirror(id, 0, 0, data.data(), data.size(), context).ok());
    EXPECT_EQ(io.timestamp.load(), context.created_ms);
}
} // namespace crowdb::tree::detail
