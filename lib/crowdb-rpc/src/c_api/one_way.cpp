// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/server/server.h"

crowdb_rpc_status crowdb_rpc_client_send_one_way_conn(crowdb_rpc_client_t client, crowdb_rpc_server_t server,
                                                      void *conn_handle, uint64_t request_id,
                                                      crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                                      uint16_t msg_type)
{
    try {
        if (client == nullptr || server == nullptr || conn_handle == nullptr || control == nullptr) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }
        auto *conn    = static_cast<crowdb::rpc::Connection *>(conn_handle);
        auto *ctrl    = control->buf;
        auto *payload = data == nullptr ? nullptr : data->buf;
        ctrl->ref_clone();
        if (payload != nullptr) {
            payload->ref_clone();
        }
        bool submitted =
            client->client->send_one_way(server->server->transport(), conn, request_id, ctrl, payload, msg_type);
        crowdb_rpc_buffer_release(control);
        if (data != nullptr) {
            crowdb_rpc_buffer_release(data);
        }
        return submitted ? CROWDB_RPC_OK : conn->is_open() ? CROWDB_RPC_ERR_SEND_QUEUE : CROWDB_RPC_ERR_CONN_CLOSED;
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

crowdb_rpc_status crowdb_rpc_client_send_one_way(crowdb_rpc_client_t client, crowdb_rpc_server_t server,
                                                 crowdb_rpc_conn_t conn, uint64_t request_id,
                                                 crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                                 uint16_t msg_type)
{
    return crowdb_rpc_client_send_one_way_conn(client, server, conn == nullptr ? nullptr : conn->conn.get(), request_id,
                                               control, data, msg_type);
}
