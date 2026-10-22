// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/server/message.h"

#include <memory>

static std::shared_ptr<void> own_handler_context(void *user_data, crowdb_rpc_handler_release_fn release)
{
    return {user_data, [release](void *context) {
                if (release != nullptr) {
                    release(context);
                }
            }};
}

// ── Custom handler dispatch ──────────

void crowdb_rpc_server_register_handler(crowdb_rpc_server_t server, uint16_t msg_type, crowdb_rpc_handler_fn callback,
                                        void *user_data, crowdb_rpc_handler_release_fn release)
{
    try {
        auto owner = own_handler_context(user_data, release);
        if (server == nullptr || callback == nullptr) {
            return;
        }
        server->server->register_handler(
            msg_type, [callback, owner = std::move(owner)](crowdb::rpc::Frame *req, crowdb::rpc::Connection *conn) {
                crowdb::rpc::invoke_c_handler(callback, owner.get(), req, conn);
                return nullptr; // async — callback submits response later.
            });
    }
    catch (...) {
    }
}

crowdb_rpc_status crowdb_rpc_server_submit_response(crowdb_rpc_server_t server, void *conn_handle,
                                                    const uint8_t *control, uint32_t control_len, const uint8_t *data,
                                                    uint32_t data_len, uint16_t msg_type, uint64_t request_id)
{
    try {
        if (server == nullptr || conn_handle == nullptr) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }

        auto *conn = static_cast<crowdb::rpc::Connection *>(conn_handle);
        auto *pool = server->server->pool();

        // Allocate response buffers from the pool and copy data.
        crowdb::rpc::Buffer *resp_ctrl = nullptr;
        if (control != nullptr && control_len > 0) {
            resp_ctrl = pool->alloc(control_len);
            if (resp_ctrl == nullptr) {
                return CROWDB_RPC_ERR_SEND_QUEUE;
            }
            resp_ctrl->write(control, control_len);
        }

        crowdb::rpc::Buffer *resp_data = nullptr;
        if (data != nullptr && data_len > 0) {
            resp_data = pool->alloc(data_len);
            if (resp_data == nullptr) {
                if (resp_ctrl != nullptr) {
                    resp_ctrl->release();
                }
                return CROWDB_RPC_ERR_SEND_QUEUE;
            }
            resp_data->write(data, data_len);
        }

        auto *frame = crowdb::rpc::build_out_frame(request_id, msg_type, resp_ctrl, resp_data);
        if (!server->server->transport()->submit(conn, frame)) {
            if (frame->control != nullptr) {
                frame->control->release();
            }
            frame->release_data();
            delete frame;
            return CROWDB_RPC_ERR_SEND_QUEUE;
        }
        return CROWDB_RPC_OK;
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

// Submit a response using pre-filled buffer handles (zero-copy). The
// server takes ownership of the buffers.
crowdb_rpc_status crowdb_rpc_server_submit_response_buffer(crowdb_rpc_server_t server, void *conn_handle,
                                                           crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                                           uint16_t msg_type, uint64_t request_id)
{
    try {
        if (server == nullptr || conn_handle == nullptr) {
            // Release any provided buffers to avoid leaks.
            if (control != nullptr) {
                crowdb_rpc_buffer_release(control);
            }
            if (data != nullptr) {
                crowdb_rpc_buffer_release(data);
            }
            return CROWDB_RPC_ERR_INVALID_ARG;
        }

        auto *conn = static_cast<crowdb::rpc::Connection *>(conn_handle);

        // Extract Buffer* from handles (steal ownership — the handle's
        // release is a no-op after this since we set buf = nullptr).
        crowdb::rpc::Buffer *resp_ctrl = nullptr;
        if (control != nullptr && control->buf != nullptr) {
            resp_ctrl    = control->buf;
            control->buf = nullptr;
            delete control;
        }
        crowdb::rpc::Buffer *resp_data = nullptr;
        if (data != nullptr && data->buf != nullptr) {
            resp_data = data->buf;
            data->buf = nullptr;
            delete data;
        }

        auto *frame = crowdb::rpc::build_out_frame(request_id, msg_type, resp_ctrl, resp_data);
        if (!server->server->transport()->submit(conn, frame)) {
            if (frame->control != nullptr) {
                frame->control->release();
            }
            frame->release_data();
            delete frame;
            return CROWDB_RPC_ERR_SEND_QUEUE;
        }
        return CROWDB_RPC_OK;
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

// ── Client-side request handler dispatch ──────────────────

void crowdb_rpc_client_register_handler(crowdb_rpc_client_t client, uint16_t msg_type, crowdb_rpc_handler_fn callback,
                                        void *user_data, crowdb_rpc_handler_release_fn release)
{
    try {
        auto owner = own_handler_context(user_data, release);
        if (client == nullptr || callback == nullptr) {
            return;
        }
        client->client->register_handler(msg_type, callback, std::move(owner));
    }
    catch (...) {
    }
}

void crowdb_rpc_client_clear_handlers(crowdb_rpc_client_t client)
{
    try {
        if (client == nullptr) {
            return;
        }
        client->client->clear_handlers();
    }
    catch (...) {
    }
}

void crowdb_rpc_client_set_transport(crowdb_rpc_client_t client, crowdb_rpc_server_t server)
{
    try {
        if (client == nullptr || server == nullptr) {
            return;
        }
        client->client->set_transport(server->server->transport());
    }
    catch (...) {
    }
}

// ── Server-side request-response correlation ──────────────

void crowdb_rpc_server_set_request_client(crowdb_rpc_server_t server, crowdb_rpc_client_t client)
{
    try {
        if (server == nullptr || client == nullptr) {
            return;
        }
        server->server->set_request_client(client->client.get());
    }
    catch (...) {
    }
}
