// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/server/message.h"

using namespace crowdb::rpc;

#define CONFIG_NUMBERS(X)                                                                                 \
    X(cq_count)                                                                                           \
    X(cq_depth)                                                                                           \
    X(sq_depth)                                                                                           \
    X(rq_depth)                                                                                           \
    X(posted_receive_target) X(wire_slot_capacity) X(poll_batch) X(verbs_poll_batch) X(poll_empty_budget) \
        X(send_queue_capacity) X(aggregation_max_bytes) X(aggregation_max_frames) X(send_arena_bytes)     \
            X(receive_arena_bytes) X(registered_memory_budget_bytes)

extern "C" void crowdb_rpc_rdma_config_init(crowdb_rpc_rdma_config_t *config)
{
    if (config == nullptr)
        return;
    *config                    = {};
    config->struct_size        = sizeof(*config);
    config->version            = 1;
    config->allow_tcp_fallback = 1;
    const RdmaConfig defaults;
    config->port                = defaults.port;
    config->aggregation_enabled = defaults.aggregation_enabled;
#define COPY_DEFAULT(name) config->name = defaults.name;
    CONFIG_NUMBERS(COPY_DEFAULT)
#undef COPY_DEFAULT
}

extern "C" crowdb_rpc_status crowdb_rpc_server_configure_rdma(crowdb_rpc_server_t             server,
                                                              const crowdb_rpc_rdma_config_t *config)
{
    if (server == nullptr || config == nullptr || config->struct_size != sizeof(*config) || config->version != 1 ||
        config->enabled > 1 || config->allow_tcp_fallback > 1 || config->listen_enabled > 1 ||
        config->aggregation_enabled > 1 || config->port > UINT8_MAX)
        return CROWDB_RPC_ERR_INVALID_ARG;
    try {
        RdmaConfig native;
        native.use_environment     = false;
        native.port                = static_cast<uint8_t>(config->port);
        native.aggregation_enabled = config->aggregation_enabled != 0;
        native.device              = config->device == nullptr ? "" : config->device;
        native.source_address      = config->source_address == nullptr ? "" : config->source_address;
        native.gid                 = config->gid == nullptr ? "" : config->gid;
#define COPY_VALUE(name) native.name = config->name;
        CONFIG_NUMBERS(COPY_VALUE)
#undef COPY_VALUE
        if (!native.valid())
            return CROWDB_RPC_ERR_INVALID_ARG;
        return server->server->configure_rdma(std::move(native), config->enabled != 0, config->allow_tcp_fallback != 0,
                                              config->listen_enabled != 0)
                 ? CROWDB_RPC_OK
                 : CROWDB_RPC_ERR_CONN_ERROR;
    }
    catch (...) {
        return CROWDB_RPC_ERR_REGISTRATION;
    }
}

#undef CONFIG_NUMBERS

extern "C" int crowdb_rpc_server_rdma_listening(crowdb_rpc_server_t server)
{
    if (server == nullptr)
        return 0;
    try {
        const auto capability = server->server->transport()->local_rdma_capability();
        return capability.rdma_available && capability.port != 0 ? 1 : 0;
    }
    catch (...) {
        return 0;
    }
}

extern "C" crowdb_rpc_buffer_t crowdb_rpc_server_status_json(crowdb_rpc_server_t server)
{
    if (server == nullptr)
        return nullptr;
    try {
        const auto json = server->server->transport()->status_json();
        return crowdb_rpc_buffer_create(reinterpret_cast<const uint8_t *>(json.data()), json.size());
    }
    catch (...) {
        return nullptr;
    }
}

extern "C" crowdb_rpc_buffer_t crowdb_rpc_conn_status_json(crowdb_rpc_conn_t connection)
{
    if (connection == nullptr || connection->conn == nullptr)
        return nullptr;
    try {
        const auto json = connection->conn->status_json();
        return crowdb_rpc_buffer_create(reinterpret_cast<const uint8_t *>(json.data()), json.size());
    }
    catch (...) {
        return nullptr;
    }
}

namespace
{
OutFrame *status_reply(RpcServer *server, Frame *request, Connection *connection)
{
    const std::unique_ptr<Frame> owner(request);
    try {
        const auto                                 json    = server->transport()->status_json();
        auto                                       release = [](Buffer *buffer) { buffer->release(); };
        std::unique_ptr<Buffer, decltype(release)> control(
            build_ping_response(server->pool(), request->request_id, request->rpc_create_nano, 0), release);
        std::unique_ptr<Buffer, decltype(release)> data(server->pool()->alloc(static_cast<uint32_t>(json.size())),
                                                        release);
        if (control != nullptr && data != nullptr) {
            data->write(json.data(), static_cast<uint32_t>(json.size()));
            auto *reply = build_out_frame(request->request_id, request->header.msg_type, control.get(), data.get());
            (void)control.release();
            (void)data.release();
            return reply;
        }
    }
    catch (...) {
    }
    connection->close();
    return nullptr;
}
} // namespace

extern "C" void crowdb_rpc_server_register_status_handler(crowdb_rpc_server_t server, uint16_t msg_type)
{
    if (server == nullptr)
        return;
    try {
        server->server->register_handler(msg_type, [native = server->server](Frame *request, Connection *connection) {
            return status_reply(native, request, connection);
        });
    }
    catch (...) {
    }
}
