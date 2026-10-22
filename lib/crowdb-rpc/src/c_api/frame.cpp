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
} // namespace crowdb::rpc
