// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_stage_gc.h"
#include "backend/chunk/rpc_chunk_transport.h"
#include "common/test_chunk_physical.h"
#include "common/test_chunk_route_service.h"
#include "common/test_chunk_stage_backend.h"
#include "diskio_generated.h"

namespace crowdb::tree::detail
{
namespace
{
struct TestChunkCollectionService
{
    crowdb_rpc_server_t   server = nullptr;
    std::atomic<bool>     deleted{false};
    std::atomic<bool>     lose_delete{false};
    std::atomic<uint64_t> epoch{17};
    std::atomic<size_t>   deletes{0};
    std::atomic<size_t>   physical_deletes{0};

    static void handle(uint64_t id, uint64_t /*unused*/, uint16_t message, const uint8_t *control, uint32_t length,
                       const uint8_t * /*unused*/, uint32_t /*unused*/, void *connection, void *frame, void *context)
    {
        static_cast<TestChunkCollectionService *>(context)->respond(id, message, control, length, connection);
        crowdb_rpc_frame_release(frame);
    }

    void respond(uint64_t id, uint16_t message, const uint8_t *control, uint32_t length, void *connection)
    {
        using namespace crowdb::chunkdb::proto;
        using namespace crowdb::rpc::proto;
        flatbuffers::Verifier          verifier(control, length);
        flatbuffers::FlatBufferBuilder builder;
        uint16_t                       response = 0;
        const FBInt128                 chunk_id(0x0200'0000'0000'0042ULL, 144);
        const FBInt128                 disk(9, 10);
        if (message == FBMsgType_EQueryChunkRequest) {
            ASSERT_TRUE(verifier.VerifyBuffer<FBQueryChunkRequest>(nullptr));
            flatbuffers::Offset<FBChunk> chunk;
            if (deleted.load()) {
                chunk = CreateFBChunk(builder, &chunk_id, 2, FBChunkState_Deleted, 123, 0, 0, 0, 0,
                                      FBChunkType_BtreePage, epoch.load(), 0);
            }
            else {
                const std::vector<crowdb::diskdb::proto::FBSegment> segments{
                    {disk, chunk_id, 0, 1, 2, 7},
                    {disk, chunk_id, 9, 1, 2, 9}
                };
                auto mirror = CreateFBMirrorStrip(builder, builder.CreateVectorOfStructs(segments));
                auto strip  = CreateFBChunkStrip(builder, 0, 0, 4, 4, 4, 0, 0, FBStripType_Mirror,
                                                 FBStripBody_FBMirrorStrip, mirror.Union());
                auto strips = builder.CreateVector(std::vector<flatbuffers::Offset<FBChunkStrip>>{strip});
                chunk       = CreateFBChunk(builder, &chunk_id, 1, FBChunkState_Active, 123, 0, 4, 0, strips,
                                            FBChunkType_BtreePage, epoch.load(), 0);
            }
            builder.Finish(CreateFBQueryChunkResponse(builder, id, 0, FBChunkdbRetCode_Success, 0, 0, 0, chunk, 0));
            response = FBMsgType_EQueryChunkResponse;
        }
        else if (message == FBMsgType_EDeleteChunkRequest) {
            ASSERT_TRUE(verifier.VerifyBuffer<FBDeleteChunkRequest>(nullptr));
            EXPECT_TRUE(flatbuffers::GetRoot<FBDeleteChunkRequest>(control)->fence_missing());
            ++deletes;
            if (!deleted.exchange(true)) {
                ++physical_deletes;
            }
            if (lose_delete.exchange(false)) {
                return;
            }
            builder.Finish(CreateFBDeleteChunkResponse(builder, id, 0, FBChunkdbRetCode_NotFound));
            response = FBMsgType_EDeleteChunkResponse;
        }
        else {
            namespace dio = crowdb::diskio::proto;
            ASSERT_TRUE(verifier.VerifyBuffer<dio::FBDiskWriteRequest>(nullptr));
            const auto *request = flatbuffers::GetRoot<dio::FBDiskWriteRequest>(control);
            EXPECT_TRUE(request->sync());
            EXPECT_EQ(request->size(), 0U);
            builder.Finish(dio::CreateFBDiskWriteResponse(builder, id, 0, dio::FBDiskIoRetCode_Success, true));
            response = FBMsgType_EDiskWriteResponse;
        }
        static_cast<void>(crowdb_rpc_server_submit_response(server, connection, builder.GetBufferPointer(),
                                                            builder.GetSize(), nullptr, 0, response, id));
    }
};

class RpcChunkCollection : public ::testing::Test
{
  protected:
    TestChunkRouteFixture              fixture;
    TestChunkCollectionService         service;
    std::unique_ptr<RpcChunkTransport> transport;
    std::shared_ptr<TestStageCatalog>  catalog = std::make_shared<TestStageCatalog>();
    ChunkStageId                       stage{.owner_epoch = 17, .operation_high = 10, .operation_low = 20};
    ChunkId                            id{0x0200'0000'0000'0042ULL, 144};

    void SetUp() override
    {
        service.server = fixture.services[0].server;
        for (const auto message :
             {crowdb::rpc::proto::FBMsgType_EQueryChunkRequest, crowdb::rpc::proto::FBMsgType_EDeleteChunkRequest,
              crowdb::rpc::proto::FBMsgType_EDiskWriteRequest}) {
            crowdb_rpc_server_register_handler(service.server, message, TestChunkCollectionService::handle, &service,
                                               nullptr);
        }
        const ct_chunk_rpc_disk_route  disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
        ct_chunk_rpc_transport_options options{};
        options.disk_routes      = &disk;
        options.disk_route_count = 1;
        options.rpc_timeout_ms   = 50;
        options.chunkdb_resolver = fixture.resolver();
        transport                = std::make_unique<RpcChunkTransport>(options);
        catalog->record          = ChunkStageRecord{.tree_id             = 19,
                                                    .id                  = stage,
                                                    .revision            = 2,
                                                    .chunk_high          = id.high,
                                                    .chunk_low           = id.low,
                                                    .capacity            = 4096,
                                                    .manifest_generation = 0,
                                                    .purpose             = 2,
                                                    .state               = ChunkStageState::Retired,
                                                    .packs               = {}};
    }
};
} // namespace

TEST_F(RpcChunkCollection, PhysicalReceiptCountsRoundedSegmentsAcrossAllMirrors)
{
    ChunkAllocationIdentity receipt;
    const auto              status = transport->inspect_fixed_allocation(id, 17, &receipt);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(receipt.chunk, id);
    EXPECT_EQ(receipt.owner_epoch, 17U);
    EXPECT_EQ(receipt.created_ms, 123U);
    EXPECT_EQ(receipt.physical_bytes, (7U + 9U) * 4U * 1024U);
    if (!catalog->record) {
        FAIL() << "stage unexpectedly absent";
        return;
    }
    EXPECT_NE(receipt.physical_bytes, catalog->record->capacity);
}

TEST_F(RpcChunkCollection, LostDeleteReplyAndDurableDeletingNotFoundConverge)
{
    service.lose_delete.store(true);
    auto result = reclaim_retired_stage(*catalog, *transport, 19, 17, stage);
    EXPECT_EQ(result.status.code(), Code::kUnavailable);
    EXPECT_FALSE(result.deletion_acknowledged);
    EXPECT_FALSE(result.stage_removed);
    if (!catalog->record) {
        FAIL() << "stage unexpectedly absent";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Deleting);
    EXPECT_EQ(service.physical_deletes.load(), 1U);
    result = reclaim_retired_stage(*catalog, *transport, 19, 17, stage);
    EXPECT_TRUE(result.status.ok()) << result.status.to_string();
    EXPECT_TRUE(result.deletion_acknowledged);
    EXPECT_TRUE(result.stage_removed);
    EXPECT_EQ(service.deletes.load(), 2U);
    EXPECT_EQ(service.physical_deletes.load(), 1U);
    EXPECT_FALSE(catalog->record);
}

TEST_F(RpcChunkCollection, ANewPhysicalOwnerCannotBeDeletedByAnOldStage)
{
    service.epoch.store(18);
    const auto result = reclaim_retired_stage(*catalog, *transport, 19, 18, stage);
    EXPECT_EQ(result.status.code(), Code::kUnavailable);
    EXPECT_FALSE(result.deletion_acknowledged);
    EXPECT_FALSE(result.stage_removed);
    EXPECT_EQ(service.deletes.load(), 0U);
    EXPECT_EQ(service.physical_deletes.load(), 0U);
    if (!catalog->record) {
        FAIL() << "stage unexpectedly absent";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Deleting);
}

TEST_F(RpcChunkCollection, PhysicalAllocationRemainsCountedUntilActualDeleteAcknowledgement)
{
    test_chunk::TestPhysicalHost           host;
    std::shared_ptr<ChunkPhysicalRegistry> registry;
    ASSERT_TRUE(transport->physical_registry(19, host.budget, &registry).ok());
    const auto              initial_scope = registry->backing_stats().scope_id;
    ChunkAllocationIdentity receipt;
    ASSERT_TRUE(transport->inspect_fixed_allocation(id, 17, &receipt).ok());
    std::shared_ptr<ChunkPhysicalPack> physical;
    ASSERT_TRUE(registry->prepare(stage, id, host.budget, &physical).ok());
    ASSERT_TRUE(registry->observe(physical, receipt, host.budget).ok());
    service.lose_delete.store(true);
    auto result = reclaim_retired_stage(*catalog, *transport, 19, 17, stage, host.budget, registry);
    EXPECT_EQ(result.status.code(), Code::kUnavailable);
    EXPECT_EQ(registry->backing_metrics()->snapshot().allocated_backing_bytes, receipt.physical_bytes);
    result = reclaim_retired_stage(*catalog, *transport, 19, 17, stage, host.budget, registry);
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    EXPECT_TRUE(result.deletion_acknowledged);
    EXPECT_EQ(service.physical_deletes.load(), 1U);
    host.cpu.drain();
    EXPECT_EQ(registry->backing_metrics()->snapshot().allocated_backing_bytes, 0U);
    registry.reset();
    ASSERT_TRUE(transport->physical_registry(19, host.budget, &registry).ok());
    EXPECT_EQ(registry->backing_stats().scope_id, initial_scope);
    EXPECT_EQ(registry->backing_stats().allocated_bytes, 0U);
}
} // namespace crowdb::tree::detail
