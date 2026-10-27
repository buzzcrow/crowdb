// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/rpc_chunk_transport.h"
#include "common/test_chunk_route_service.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
using namespace crowdb::chunkdb::proto;
using namespace crowdb::rpc::proto;
constexpr uint64_t kMiB = 1024U * 1024U;

struct TestGrowthService
{
    crowdb_rpc_server_t   server = nullptr;
    std::atomic<uint32_t> units{0};
    std::atomic<uint32_t> appends{0};
    std::atomic<uint64_t> revision{1};
    std::atomic<uint8_t>  purpose{2};
    std::atomic<bool>     conflict_once{false};
    std::atomic<bool>     fail_append{false};
    std::atomic<bool>     invalid_request{false};

    flatbuffers::Offset<FBChunkStrip> strip(flatbuffers::FlatBufferBuilder &builder, uint32_t index)
    {
        const FBInt128 id((static_cast<uint64_t>(purpose.load()) << 56U) | 0x42U, 100);
        const FBInt128 disk(9, 10);
        const std::vector<crowdb::diskdb::proto::FBSegment> segments{
            {disk, id, index, 1, 0, 1},
            {disk, id, index, 1, 1, 1}
        };
        auto mirror = CreateFBMirrorStrip(builder, builder.CreateVectorOfStructs(segments));
        return CreateFBChunkStrip(builder, index * 1024U, 0, 1024, 1024, 1, 0, 0, FBStripType_Mirror,
                                  FBStripBody_FBMirrorStrip, mirror.Union());
    }

    flatbuffers::Offset<FBChunk> chunk(flatbuffers::FlatBufferBuilder &builder)
    {
        const FBInt128                                 id((static_cast<uint64_t>(purpose.load()) << 56U) | 0x42U, 100);
        std::vector<flatbuffers::Offset<FBChunkStrip>> strips;
        strips.reserve(units.load());
        for (uint32_t index = 0; index < units.load(); ++index) {
            strips.push_back(strip(builder, index));
        }
        return CreateFBChunk(builder, &id, revision.load(), FBChunkState_Active, 1, 0, units.load() * 1024U, 0,
                             builder.CreateVector(strips), static_cast<FBChunkType>(purpose.load()), 17, 0);
    }

    static void handle(uint64_t request_id, uint64_t /*unused*/, uint16_t message, const uint8_t *control,
                       uint32_t length, const uint8_t * /*unused*/, uint32_t /*unused*/, void *connection, void *frame,
                       void *context)
    {
        auto &self = *static_cast<TestGrowthService *>(context);
        self.respond(request_id, message, control, length, connection);
        crowdb_rpc_frame_release(frame);
    }

    void respond(uint64_t request_id, uint16_t message, const uint8_t *control, uint32_t length, void *connection)
    {
        flatbuffers::Verifier          verifier(control, length);
        flatbuffers::FlatBufferBuilder builder;
        uint16_t                       response_type = 0;
        if (message == FBMsgType_EAllocateChunkRequest) {
            ASSERT_TRUE(verifier.VerifyBuffer<FBAllocateChunkRequest>(nullptr));
            const auto *request = flatbuffers::GetRoot<FBAllocateChunkRequest>(control);
            invalid_request.store(request->write_granularity() != 1024 || request->strip_count() != 4 ||
                                  request->copy_count() != 2);
            units.store(request->strip_count());
            purpose.store(request->chunk_type());
            builder.Finish(CreateFBAllocateChunkResponse(builder, request_id, 0, FBChunkdbRetCode_Success, 0, 0, 0,
                                                         chunk(builder)));
            response_type = FBMsgType_EAllocateChunkResponse;
        }
        else if (message == FBMsgType_EQueryChunkRequest) {
            ASSERT_TRUE(verifier.VerifyBuffer<FBQueryChunkRequest>(nullptr));
            builder.Finish(CreateFBQueryChunkResponse(builder, request_id, 0, FBChunkdbRetCode_Success, 0, 0, 0,
                                                      chunk(builder), 0));
            response_type = FBMsgType_EQueryChunkResponse;
        }
        else {
            ASSERT_TRUE(verifier.VerifyBuffer<FBAppendChunkRequest>(nullptr));
            const auto *request = flatbuffers::GetRoot<FBAppendChunkRequest>(control);
            appends.fetch_add(1);
            if (request->strip_size() != 1 || request->copy_count() != 2 ||
                request->strip_type() != FBStripType_Mirror) {
                invalid_request.store(true);
            }
            const bool conflict = conflict_once.exchange(false);
            if (conflict) {
                revision.fetch_add(1);
            }
            std::vector<flatbuffers::Offset<FBChunkStrip>> added;
            if (!fail_append.load() && !conflict) {
                EXPECT_EQ(request->modify_ts(), revision.load());
                const uint32_t start = units.fetch_add(request->strip_count());
                for (uint32_t index = start; index < units.load(); ++index) {
                    added.push_back(strip(builder, index));
                }
                revision.fetch_add(1);
            }
            builder.Finish(CreateFBAppendChunkResponse(
                builder, request_id, 0, fail_append.load() ? FBChunkdbRetCode_Unavailable : FBChunkdbRetCode_Success, 0,
                0, 0, revision.load(), builder.CreateVector(added), conflict ? chunk(builder) : 0));
            response_type = FBMsgType_EAppendChunkResponse;
        }
        static_cast<void>(crowdb_rpc_server_submit_response(server, connection, builder.GetBufferPointer(),
                                                            builder.GetSize(), nullptr, 0, response_type, request_id));
    }
};

TEST(RpcChunkGrowth, PageAndIndexAllocateSmallThenGrowWithoutDuplicateMirrorReservations)
{
    for (const auto purpose : {PagePurpose::kBtreePage, PagePurpose::kPageIndex}) {
        TestChunkRouteFixture fixture;
        TestGrowthService     service;
        service.server = fixture.services[0].server;
        for (auto message :
             {FBMsgType_EAllocateChunkRequest, FBMsgType_EAppendChunkRequest, FBMsgType_EQueryChunkRequest}) {
            crowdb_rpc_server_register_handler(service.server, message, TestGrowthService::handle, &service, nullptr);
        }
        const ct_chunk_rpc_disk_route        disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
        const ct_chunk_rpc_transport_options options{.chunkdb              = fixture.routes[0],
                                                     .disk_routes          = &disk,
                                                     .disk_route_count     = 1,
                                                     .writer_lease_ms      = 5000,
                                                     .rpc_timeout_ms       = 1000,
                                                     .completion_capacity  = 16,
                                                     .mirror_copies        = 2,
                                                     .strip_capacity_bytes = 1024U * 1024U,
                                                     .chunkdb_resolver     = {},
                                                     .disk_resolver        = {}};
        RpcChunkTransport                    transport(options);
        ChunkId                              id;
        ASSERT_TRUE(transport.allocate_mirror_chunk(256 * kMiB, 17, &id, purpose).ok());
        EXPECT_EQ(service.units.load(), 4U);
        EXPECT_FALSE(service.invalid_request.load());
        ASSERT_TRUE(transport.ensure_capacity(id, 1).ok());
        ASSERT_TRUE(transport.ensure_capacity(id, 4 * kMiB).ok());
        EXPECT_EQ(service.appends.load(), 0U);
        ASSERT_TRUE(transport.ensure_capacity(id, (4 * kMiB) + 1).ok());
        EXPECT_EQ(service.units.load(), 8U);
        ASSERT_TRUE(transport.ensure_capacity(id, (4 * kMiB) + 1).ok());
        EXPECT_EQ(service.appends.load(), 1U);
        service.conflict_once.store(true);
        ASSERT_TRUE(transport.ensure_capacity(id, (8 * kMiB) + 1).ok());
        EXPECT_EQ(service.units.load(), 12U);
        EXPECT_EQ(service.appends.load(), 3U);
        service.fail_append.store(true);
        EXPECT_FALSE(transport.ensure_capacity(id, (12 * kMiB) + 1).ok());
        EXPECT_EQ(service.units.load(), 12U);
        service.fail_append.store(false);
        ASSERT_TRUE(transport.ensure_capacity(id, 256 * kMiB).ok());
        EXPECT_EQ(service.units.load(), 256U);
        const auto appends = service.appends.load();
        EXPECT_FALSE(transport.ensure_capacity(id, (256 * kMiB) + 1).ok());
        EXPECT_EQ(service.appends.load(), appends);
        EXPECT_FALSE(service.invalid_request.load());
    }
}
} // namespace
} // namespace crowdb::tree::detail
