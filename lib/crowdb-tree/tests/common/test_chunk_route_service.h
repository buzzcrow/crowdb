// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunkdb_generated.h"
#include "crowdb-rpc/c_api.h"
#include "crowdb-tree/c_api.h"
#include "msg_type_generated.h"

#include <array>
#include <atomic>
#include <vector>

namespace crowdb::tree::detail
{

struct TestChunkRouteService
{
    crowdb_rpc_server_t   server = nullptr;
    uint64_t              parity = 0;
    std::atomic<uint32_t> allocations{0};
    std::atomic<uint32_t> queries{0};
    std::atomic<uint32_t> advances{0};
    std::atomic<uint32_t> seals{0};
    std::atomic<uint64_t> acknowledged{0};
    bool                  drop_allocation = false;

    static void handle(uint64_t request_id, uint64_t /*unused*/, uint16_t message, const uint8_t *control,
                       uint32_t length, const uint8_t * /*unused*/, uint32_t /*unused*/, void *connection, void *frame,
                       void *context)
    {
        auto &self = *static_cast<TestChunkRouteService *>(context);
        self.respond(request_id, message, control, length, connection);
        crowdb_rpc_frame_release(frame);
    }

    void respond(uint64_t request_id, uint16_t message, const uint8_t *control, uint32_t length, void *connection)
    {
        using namespace crowdb::chunkdb::proto;
        using namespace crowdb::rpc::proto;
        flatbuffers::Verifier verifier(control, length);
        uint64_t              low = 100 + parity;
        if (message == FBMsgType_EAllocateChunkRequest) {
            if (!verifier.VerifyBuffer<FBAllocateChunkRequest>(nullptr)) {
                return;
            }
            allocations.fetch_add(1);
            if (drop_allocation) {
                return;
            }
        }
        else if (message == FBMsgType_EQueryChunkRequest) {
            if (!verifier.VerifyBuffer<FBQueryChunkRequest>(nullptr)) {
                return;
            }
            low = flatbuffers::GetRoot<FBQueryChunkRequest>(control)->chunk_id()->low();
            queries.fetch_add(1);
        }
        else if (message == FBMsgType_EAdvanceChunkWriteRequest) {
            if (!verifier.VerifyBuffer<FBAdvanceChunkWriteRequest>(nullptr)) {
                return;
            }
            low = flatbuffers::GetRoot<FBAdvanceChunkWriteRequest>(control)->chunk_id()->low();
            acknowledged.store(flatbuffers::GetRoot<FBAdvanceChunkWriteRequest>(control)->acknowledged_cursor());
            advances.fetch_add(1);
        }
        else {
            if (!verifier.VerifyBuffer<FBSealChunkRequest>(nullptr)) {
                return;
            }
            low = flatbuffers::GetRoot<FBSealChunkRequest>(control)->chunk_id()->low();
            seals.fetch_add(1);
        }
        const auto code = low % 2 == parity ? FBChunkdbRetCode_Success : FBChunkdbRetCode_NotMyRange;
        flatbuffers::FlatBufferBuilder builder;
        const auto                     chunk         = make_chunk(builder, low, acknowledged.load());
        uint16_t                       response_type = 0;
        if (message == FBMsgType_EAllocateChunkRequest) {
            builder.Finish(CreateFBAllocateChunkResponse(builder, request_id, 0, code, 0, 0, 0, chunk));
            response_type = FBMsgType_EAllocateChunkResponse;
        }
        else if (message == FBMsgType_EQueryChunkRequest) {
            builder.Finish(CreateFBQueryChunkResponse(builder, request_id, 0, code, 0, 0, 0, chunk, 0));
            response_type = FBMsgType_EQueryChunkResponse;
        }
        else if (message == FBMsgType_EAdvanceChunkWriteRequest) {
            builder.Finish(CreateFBAdvanceChunkWriteResponse(builder, request_id, 0, code, 0, 0, 0, chunk));
            response_type = FBMsgType_EAdvanceChunkWriteResponse;
        }
        else {
            builder.Finish(CreateFBSealChunkResponse(builder, request_id, 0, code, 0, 0, 0, chunk));
            response_type = FBMsgType_ESealChunkResponse;
        }
        static_cast<void>(crowdb_rpc_server_submit_response(server, connection, builder.GetBufferPointer(),
                                                            builder.GetSize(), nullptr, 0, response_type, request_id));
    }

    static flatbuffers::Offset<crowdb::chunkdb::proto::FBChunk> make_chunk(flatbuffers::FlatBufferBuilder &builder,
                                                                           uint64_t low, uint64_t acknowledged_bytes)
    {
        using namespace crowdb::chunkdb::proto;
        const crowdb::rpc::proto::FBInt128                  id(0x0200'0000'0000'0042ULL, low);
        const crowdb::rpc::proto::FBInt128                  disk(9, 10);
        const std::vector<crowdb::diskdb::proto::FBSegment> segments{
            {disk, id, 0, 1, 2, 4096}
        };
        const auto mirror = CreateFBMirrorStrip(builder, builder.CreateVectorOfStructs(segments));
        const auto strip  = CreateFBChunkStrip(builder, 0, 0, 256U * 1024U, 256U * 1024U, 1, 0, 0, FBStripType_Mirror,
                                               FBStripBody_FBMirrorStrip, mirror.Union());
        const auto strips = builder.CreateVector(std::vector<flatbuffers::Offset<FBChunkStrip>>{strip});
        return CreateFBChunk(builder, &id, 3, FBChunkState_Active, 1, 0, 256U * 1024U, 0, strips, FBChunkType_BtreePage,
                             17, acknowledged_bytes);
    }
};

struct TestChunkRouteFixture
{
    std::array<TestChunkRouteService, 2> services;
    std::array<ct_chunk_rpc_route, 2>    routes{};
    crowdb_rpc_client_t                  client = crowdb_rpc_client_create();
    std::atomic<uint32_t>                retains{0};
    std::atomic<uint32_t>                releases{0};
    std::atomic<uint32_t>                leases{0};
    std::atomic<uint32_t>                refreshes{0};
    bool                                 stale = false;

    TestChunkRouteFixture()
    {
        using namespace crowdb::rpc::proto;
        crowdb_rpc_client_set_completion_pool_size(client, 32);
        crowdb_rpc_client_start_reaper(client, 100'000'000, 10'000'000);
        for (size_t index = 0; index != services.size(); ++index) {
            auto &service  = services[index];
            service.parity = index;
            service.server = crowdb_rpc_server_create(nullptr);
            static_cast<void>(crowdb_rpc_server_listen(service.server, "127.0.0.1", 0));
            for (auto message : {FBMsgType_EAllocateChunkRequest, FBMsgType_EQueryChunkRequest,
                                 FBMsgType_EAdvanceChunkWriteRequest, FBMsgType_ESealChunkRequest}) {
                crowdb_rpc_server_register_handler(service.server, message, TestChunkRouteService::handle, &service);
            }
            crowdb_rpc_server_start(service.server);
            auto *connection = crowdb_rpc_connect(service.server, "127.0.0.1", crowdb_rpc_server_port(service.server));
            crowdb_rpc_client_attach(client, connection);
            routes[index] = {.client = client, .server = service.server, .connection = connection};
        }
    }

    TestChunkRouteFixture(const TestChunkRouteFixture &)            = delete;
    TestChunkRouteFixture &operator=(const TestChunkRouteFixture &) = delete;

    ~TestChunkRouteFixture()
    {
        for (auto route : routes) {
            crowdb_rpc_conn_destroy(static_cast<crowdb_rpc_conn_t>(route.connection));
        }
        crowdb_rpc_client_destroy(client);
        for (auto &service : services) {
            crowdb_rpc_server_stop(service.server);
            crowdb_rpc_server_destroy(service.server);
        }
    }

    ct_chunk_rpc_resolver resolver()
    {
        return {
            .context = this,
            .resolve =
                [](void *context, uint64_t, uint64_t low, bool refresh, ct_chunk_rpc_route *route, void **lease) {
                    auto &self = *static_cast<TestChunkRouteFixture *>(context);
                    if (refresh) {
                        self.refreshes.fetch_add(1);
                        self.stale = false;
                    }
                    *route = self.routes[self.stale ? 0 : low % 2];
                    *lease = context;
                    self.leases.fetch_add(1);
                    return ct_status{0};
                },
            .release_route = [](void *context) { static_cast<TestChunkRouteFixture *>(context)->leases.fetch_sub(1); },
            .retain_context =
                [](void *context) { static_cast<TestChunkRouteFixture *>(context)->retains.fetch_add(1); },
            .release_context =
                [](void *context) { static_cast<TestChunkRouteFixture *>(context)->releases.fetch_add(1); }};
    }
};

} // namespace crowdb::tree::detail
