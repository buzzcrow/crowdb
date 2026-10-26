// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/server/handler.h"

#include "crowdb-rpc/server/message.h"
#include "msg_type_generated.h"

#include <chrono>
#include <cstring>

namespace crowdb::rpc
{

OutFrame *handle_ping(Frame *request, Connection *conn)
{
    // request_id + rpc_create_nano extracted during parse.
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;

    BufferPool *pool      = conn->pool();
    uint64_t    resp_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    Buffer     *resp_ctrl = build_ping_response(pool, req_id, create_nano, resp_nano);

    delete request;

    auto *out =
        build_out_frame(req_id, static_cast<uint16_t>(proto::FBMsgType_EConnectionPingResponse), resp_ctrl, nullptr);
    return out;
}

OutFrame *handle_unknown(Frame *request, Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;

    BufferPool *pool      = conn->pool();
    uint64_t    resp_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    Buffer     *resp_ctrl = build_unknown_response(pool, req_id, create_nano, resp_nano);

    auto msg_type = static_cast<uint16_t>(proto::FBMsgType_EUnknownResponse);
    delete request;

    return build_out_frame(req_id, msg_type, resp_ctrl, nullptr);
}

OutFrame *handle_echo(Frame *request, Connection *conn)
{
    const auto request_id = request->request_id;
    const auto msg_type   = request->header.msg_type;
    const auto now        = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    auto      *control    = build_ping_response(conn->pool(), request_id, request->rpc_create_nano, now);
    Buffer    *data       = nullptr;
    const auto length     = request->data_buf == nullptr ? 0 : request->data_buf->len;
    if (length != 0) {
        data = conn->pool()->alloc(length);
        if (data != nullptr)
            data->write(request->data_buf->payload_data(), length);
    }
    delete request;
    if (control == nullptr || (length != 0 && data == nullptr)) {
        if (control != nullptr)
            control->release();
        if (data != nullptr)
            data->release();
        conn->close();
        return nullptr;
    }
    return build_out_frame(request_id, msg_type, control, data);
}

} // namespace crowdb::rpc
