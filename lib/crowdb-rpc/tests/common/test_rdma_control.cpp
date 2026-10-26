// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "test_rdma_control.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include "crowdb-rpc/server/message.h"

#    include <thread>

namespace crowdb::rpc
{
RdmaConfig test_control_config(uint32_t capacity, uint32_t max_control_bytes)
{
    auto config            = RdmaConfig::for_layout(RdmaWireLayout::ControlData, capacity);
    config.use_environment = false;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 2;
    config.cq_depth                                                  = 4;
    config.send_arena_bytes                                          = uint64_t{2} * capacity;
    config.receive_arena_bytes                                       = uint64_t{2} * config.wire_slot_capacity;
    config.max_control_message_bytes                                 = max_control_bytes;
    config.control_send_budget_bytes                                 = uint64_t{3} * max_control_bytes;
    config.control_receive_budget_bytes                              = max_control_bytes;
    config.poll_empty_budget                                         = 1;
    if (const auto *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR")) {
        config.source_address = address;
    }
    if (const auto *device = std::getenv("CROWDB_RDMA_DEVICE")) {
        config.device = device;
    }
    return config;
}

OutFrame *test_control_frame(BufferPool &pool, uint64_t id, uint32_t size)
{
    auto *control = pool.alloc(size);
    if (control == nullptr) {
        return nullptr;
    }
    for (uint32_t index = 0; index < size; ++index) {
        control->data[index] = static_cast<uint8_t>(index * 131U + id);
    }
    auto *identity = build_ping_request(&pool, id, 11);
    if (identity == nullptr || size < identity->len) {
        if (identity != nullptr) {
            identity->release();
        }
        control->release();
        return nullptr;
    }
    std::memcpy(control->data, identity->data, identity->len);
    identity->release();
    control->len = size;
    return build_out_frame(id, 100, control, nullptr);
}

OutFrame *test_control_reply(Frame *frame)
{
    auto *control     = new Buffer;
    control->data     = frame->control.data();
    control->len      = static_cast<uint32_t>(frame->control.size());
    control->capacity = control->len;
    control->ref      = new std::atomic<int32_t>(1);
    control->free_cb  = [](void *owner) { delete static_cast<Frame *>(owner); };
    control->free_ctx = frame;
    return build_out_frame(frame->request_id, 100, control, nullptr);
}

std::unique_ptr<TestControlPeers> TestControlPeers::create(const RdmaConfig &client_config,
                                                           const RdmaConfig &server_config)
{
    auto pair    = std::make_unique<TestControlPeers>();
    pair->server = std::make_unique<RdmaTransport>(server_config, &pair->application);
    pair->client = std::make_unique<RdmaTransport>(client_config, &pair->application);
    if (!pair->server->listen(server_config.source_address, 19401)) {
        return nullptr;
    }
    std::thread acceptor([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (pair->incoming == nullptr && std::chrono::steady_clock::now() < deadline) {
            pair->incoming = pair->server->accept();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    pair->outgoing = pair->client->connect(server_config.source_address, 19401);
    acceptor.join();
    if (pair->incoming == nullptr || pair->outgoing == nullptr) {
        return nullptr;
    }
    return pair;
}
} // namespace crowdb::rpc
#endif
