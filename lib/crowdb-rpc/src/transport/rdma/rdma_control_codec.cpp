// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_control_codec.h"

#include "common_msg_generated.h"
#include "msg_type_generated.h"

#include <algorithm>
#include <cstring>

namespace crowdb::rpc
{
static_assert(RPC_CONTROL_FRAGMENT_MSG_TYPE == proto::FBMsgType_ERpcControlFragment);

bool write_control_fragment(std::span<uint8_t> prefix, const ControlFragment &f, uint32_t data_length)
{
    if (prefix.size() != RDMA_CONTROL_CAPACITY || data_length > f.capacity) {
        return false;
    }
    flatbuffers::FlatBufferBuilder builder(256);
    builder.Finish(proto::CreateRpcControlFragment(builder, f.request_id, f.rpc_create_nano, f.response_create_nano,
                                                   f.stream_id, f.generation, f.sequence, f.offset, f.total, f.capacity,
                                                   f.message_type, f.message_flags, f.stream_flags));
    if (builder.GetSize() > RDMA_CONTROL_CAPACITY - HEADER_SIZE) {
        return false;
    }
    std::fill(prefix.begin(), prefix.end(), uint8_t{0});
    Header header;
    header.msg_type  = RPC_CONTROL_FRAGMENT_MSG_TYPE;
    header.msg_size  = RDMA_CONTROL_CAPACITY - HEADER_SIZE;
    header.data_size = data_length;
    header.flags     = FLAG_ONE_WAY;
    serialize_header(prefix.data(), header);
    std::memcpy(prefix.data() + HEADER_SIZE, builder.GetBufferPointer(), builder.GetSize());
    return true;
}

bool parse_control_fragment(std::span<const uint8_t> control, ControlFragment &fragment)
{
    if (control.empty() || control.size() > RDMA_CONTROL_CAPACITY - HEADER_SIZE) {
        return false;
    }
    flatbuffers::Verifier verifier(control.data(), control.size());
    if (!verifier.VerifyBuffer<proto::RpcControlFragment>()) {
        return false;
    }
    const auto *f = flatbuffers::GetRoot<proto::RpcControlFragment>(control.data());
    fragment      = {.request_id           = f->id(),
                     .rpc_create_nano      = f->rpc_create_nano(),
                     .response_create_nano = f->response_create_nano(),
                     .stream_id            = f->stream_id(),
                     .generation           = f->generation(),
                     .sequence             = f->sequence(),
                     .offset               = f->logical_offset(),
                     .total                = f->total_length(),
                     .capacity             = f->data_capacity(),
                     .message_type         = f->original_msg_type(),
                     .message_flags        = f->original_flags(),
                     .stream_flags         = f->stream_flags()};
    return true;
}

bool control_identity_matches(const Frame &frame)
{
    uint64_t id = 0, created = 0, response = 0;
    extract_control_fields(frame.control.data(), static_cast<uint32_t>(frame.control.size()), id, created, response);
    return id == frame.request_id && created == frame.rpc_create_nano && response == frame.response_create_nano;
}
} // namespace crowdb::rpc
