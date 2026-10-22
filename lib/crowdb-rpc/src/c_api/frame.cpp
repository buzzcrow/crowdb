// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/framing.h"

namespace crowdb::rpc
{
namespace
{
struct ControlOwner
{
    std::shared_ptr<void> admission;
    std::vector<uint8_t>  bytes;
};

crowdb_rpc_buffer_t wrap_control(Frame &frame)
{
    if (frame.control.empty())
        return nullptr;
    if (frame.control_owner == nullptr)
        return crowdb_rpc_buffer_create(frame.control.data(), static_cast<uint32_t>(frame.control.size()));
    auto owner       = std::make_unique<ControlOwner>();
    owner->bytes     = std::move(frame.control);
    owner->admission = std::move(frame.control_owner);
    auto handle      = crowdb_rpc_buffer_create_external(
        owner->bytes.data(), static_cast<uint32_t>(owner->bytes.size()),
        [](void *context) { delete static_cast<ControlOwner *>(context); }, owner.get());
    if (handle != nullptr)
        (void)owner.release();
    return handle;
}

crowdb_rpc_buffer_t wrap_data(Buffer *buffer)
{
    if (buffer == nullptr || buffer->len == 0)
        return nullptr;
    auto handle = std::make_unique<crowdb_rpc_buffer_s>();
    handle->buf = buffer->ref_clone();
    return handle->buf == nullptr ? nullptr : handle.release();
}
} // namespace

void frame_to_c_handles(Frame *frame, crowdb_rpc_buffer_t *out_ctrl, crowdb_rpc_buffer_t *out_data)
{
    *out_ctrl = nullptr;
    *out_data = nullptr;
    std::unique_ptr<Frame> incoming(frame);
    if (incoming == nullptr)
        return;
    // Budget-owned controls move their exact storage and admission into the
    // external owner. Ordinary controls preserve their existing copied handle.
    *out_ctrl = wrap_control(*incoming);
    *out_data = wrap_data(incoming->data_buf);
}

void invoke_c_handler(crowdb_rpc_handler_fn callback, void *user_data, Frame *request, Connection *conn)
{
    if (callback == nullptr || request == nullptr) {
        delete request;
        return;
    }
    request->connection_owner  = conn == nullptr ? nullptr : conn->weak_from_this().lock();
    uint64_t       req_id      = request->request_id;
    uint64_t       create_nano = request->rpc_create_nano;
    uint16_t       msg_type    = request->header.msg_type;
    const uint8_t *ctrl_ptr    = request->control.empty() ? nullptr : request->control.data();
    auto           ctrl_len    = static_cast<uint32_t>(request->control.size());
    const uint8_t *data_ptr    = nullptr;
    uint32_t       data_len    = 0;
    if (request->data_buf != nullptr && request->data_buf->len > 0) {
        data_ptr = request->data_buf->payload_data();
        data_len = request->data_buf->len;
    }
    void *conn_handle  = static_cast<void *>(conn);
    void *frame_handle = static_cast<void *>(request);

    // Transfer frame ownership to the callback. The callback must call
    // crowdb_rpc_frame_release(frame_handle) when done.
    callback(req_id, create_nano, msg_type, ctrl_ptr, ctrl_len, data_ptr, data_len, conn_handle, frame_handle,
             user_data);
}

} // namespace crowdb::rpc

extern "C" void crowdb_rpc_frame_release(void *frame_handle)
{
    delete static_cast<crowdb::rpc::Frame *>(frame_handle);
}

extern "C" void crowdb_rpc_frame_close_connection(void *frame_handle)
{
    try {
        if (frame_handle != nullptr) {
            auto connection = static_cast<crowdb::rpc::Frame *>(frame_handle)->connection_owner;
            if (connection != nullptr) {
                connection->close();
            }
        }
    }
    catch (...) {
    }
}
