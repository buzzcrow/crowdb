// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_direct_frame.h"

#include "rdma_aggregate.h"

#include <algorithm>
#include <cstring>

namespace crowdb::rpc
{
bool rdma_direct_frame_size(const OutFrame &frame, uint32_t wire_limit, uint32_t &size)
{
    uint32_t plain = 0;
    if (!rdma_frame_size(frame, UINT32_MAX, plain))
        return false;
    const uint64_t total = static_cast<uint64_t>(RDMA_CONTROL_CAPACITY) + plain - HEADER_SIZE - frame.control->len;
    if (total > wire_limit)
        return false;
    size = static_cast<uint32_t>(total);
    return true;
}

void write_rdma_direct_control(uint8_t *destination, const OutFrame &frame)
{
    std::fill_n(destination, RDMA_CONTROL_CAPACITY, uint8_t{0});
    auto header      = frame.header;
    header.msg_size  = RDMA_CONTROL_CAPACITY - HEADER_SIZE;
    header.data_size = 0;
    for (uint8_t index = 0; index < frame.data_views(); ++index)
        header.data_size += frame.data_view(index)->len;
    serialize_header(destination, header);
    std::memcpy(destination + HEADER_SIZE, frame.control->data, frame.control->len);
}

bool parse_rdma_direct_control(std::span<const uint8_t> control, uint32_t data_capacity, uint32_t wire_length,
                               Header &header)
{
    if (control.size() != RDMA_CONTROL_CAPACITY || wire_length < RDMA_CONTROL_CAPACITY)
        return false;
    header = parse_header(control.data());
    return header.magic == MAGIC && header.msg_offset == HEADER_SIZE &&
           header.msg_size == RDMA_CONTROL_CAPACITY - HEADER_SIZE && header.data_size <= data_capacity &&
           static_cast<uint64_t>(RDMA_CONTROL_CAPACITY) + header.data_size == wire_length;
}
} // namespace crowdb::rpc
