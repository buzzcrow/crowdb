// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_packed_frame.h"

#include "crowdb-rpc/transport/rdma/rdma_capability.h"
#include "msg_type_generated.h"
#include "rdma_direct_frame.h"

#include <algorithm>

namespace crowdb::rpc
{
namespace
{
constexpr std::array<uint8_t, 6> ENVELOPE_PREFIX{'R', 'D', 'P', 'K', 1, 0};

bool packable_type(uint16_t type)
{
    // Disk READ also needs an independent receive allocation as its later
    // I/O destination, even though its request has no incoming data payload.
    return type != RDMA_HANDSHAKE_MSG_TYPE && type != RDMA_PACKED_MSG_TYPE &&
           type != proto::FBMsgType_ERpcControlFragment &&
           !(type >= proto::FBMsgType_EDiskWriteRequest && type <= proto::FBMsgType_EDiskFsyncResponse);
}
} // namespace

bool rdma_packable_frame(const OutFrame &frame, uint32_t limit, uint32_t &size)
{
    return packable_type(frame.header.msg_type) &&
           rdma_frame_size(frame, std::min(limit, RDMA_SMALL_FRAME_BYTES), size);
}

void write_rdma_packed_control(uint8_t *destination, uint32_t data_length, uint32_t count)
{
    std::fill_n(destination, RDMA_CONTROL_CAPACITY, uint8_t{0});
    Header header;
    header.msg_type  = RDMA_PACKED_MSG_TYPE;
    header.msg_size  = RDMA_CONTROL_CAPACITY - HEADER_SIZE;
    header.data_size = data_length;
    header.flags     = FLAG_ONE_WAY;
    serialize_header(destination, header);
    std::copy(ENVELOPE_PREFIX.begin(), ENVELOPE_PREFIX.end(), destination + HEADER_SIZE);
    destination[HEADER_SIZE + 6] = static_cast<uint8_t>(count);
    destination[HEADER_SIZE + 7] = static_cast<uint8_t>(count >> 8);
}

bool parse_rdma_packed_frame(std::span<const uint8_t> control, std::span<const uint8_t> data, uint32_t frame_limit,
                             RdmaAggregate &aggregate)
{
    if (data.size() > RDMA_DATAPATH_CAPACITY) {
        return false;
    }
    Header header;
    if (!parse_rdma_direct_control(control, static_cast<uint32_t>(data.size()),
                                   RDMA_CONTROL_CAPACITY + static_cast<uint32_t>(data.size()), header) ||
        header.msg_type != RDMA_PACKED_MSG_TYPE || header.flags != FLAG_ONE_WAY ||
        !std::equal(ENVELOPE_PREFIX.begin(), ENVELOPE_PREFIX.end(), control.begin() + HEADER_SIZE)) {
        return false;
    }
    const uint32_t count = control[HEADER_SIZE + 6] | (uint32_t{control[HEADER_SIZE + 7]} << 8);
    if (count < 2 || count > frame_limit ||
        !std::all_of(control.begin() + HEADER_SIZE + 8, control.end(), [](uint8_t byte) { return byte == 0; }) ||
        !parse_rdma_aggregate(data, aggregate) || aggregate.count != count) {
        return false;
    }
    for (uint32_t index = 0; index < count; ++index) {
        const auto &view = aggregate.frames[index];
        if (!packable_type(view.header.msg_type) ||
            uint64_t{HEADER_SIZE} + view.header.msg_size + view.header.data_size > RDMA_SMALL_FRAME_BYTES) {
            return false;
        }
    }
    return true;
}
} // namespace crowdb::rpc
