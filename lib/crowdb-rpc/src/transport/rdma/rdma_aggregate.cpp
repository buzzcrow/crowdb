// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_aggregate.h"

#include <cstring>

namespace crowdb::rpc
{
bool parse_rdma_aggregate(std::span<const uint8_t> bytes, RdmaAggregate &result)
{
    result.count  = 0;
    size_t offset = 0;
    while (offset < bytes.size()) {
        if (result.count == RDMA_MAX_AGGREGATE_FRAMES || bytes.size() - offset < HEADER_SIZE)
            return false;
        const auto     header      = parse_header(bytes.data() + offset);
        const uint64_t control_end = static_cast<uint64_t>(header.msg_offset) + header.msg_size;
        const uint64_t frame_size  = control_end + header.data_size;
        if (header.magic != MAGIC || header.msg_offset != HEADER_SIZE || control_end > RDMA_CONTROL_CAPACITY ||
            frame_size > bytes.size() - offset)
            return false;
        result.frames[result.count++] = {header, static_cast<uint32_t>(offset),
                                         static_cast<uint32_t>(offset + control_end)};
        offset += static_cast<size_t>(frame_size);
    }
    return result.count != 0;
}

bool rdma_frame_size(const OutFrame &frame, uint32_t limit, uint32_t &size)
{
    if (frame.control == nullptr || frame.control->data == nullptr || frame.control->len > frame.control->capacity ||
        frame.control->len > RDMA_CONTROL_CAPACITY - HEADER_SIZE || frame.header.magic != MAGIC ||
        frame.header.msg_offset != HEADER_SIZE || frame.data_views() > MAX_DATA_VIEWS)
        return false;
    uint64_t bytes = static_cast<uint64_t>(HEADER_SIZE) + frame.control->len;
    for (uint8_t index = 0; index < frame.data_views(); ++index) {
        const auto *view = frame.data_view(index);
        if (view == nullptr || view->len > view->capacity || (view->len != 0 && view->payload_data() == nullptr))
            return false;
        bytes += view->len;
    }
    if (bytes > limit)
        return false;
    size = static_cast<uint32_t>(bytes);
    return true;
}

void copy_rdma_frame(uint8_t *destination, const OutFrame &frame)
{
    auto header      = frame.header;
    header.msg_size  = frame.control->len;
    header.data_size = 0;
    for (uint8_t index = 0; index < frame.data_views(); ++index)
        header.data_size += frame.data_view(index)->len;
    serialize_header(destination, header);
    uint32_t offset = HEADER_SIZE;
    std::memcpy(destination + offset, frame.control->data, header.msg_size);
    offset += header.msg_size;
    for (uint8_t index = 0; index < frame.data_views(); ++index) {
        const auto *view = frame.data_view(index);
        if (view->len != 0)
            std::memcpy(destination + offset, view->payload_data(), view->len);
        offset += view->len;
    }
}

void release_rdma_frames(OutFrame *frame, bool quarantine)
{
    while (frame != nullptr) {
        auto *next = frame->next_aggregate;
        if (quarantine) {
            for (uint8_t index = 0; index < frame->data_views(); ++index)
                (void)frame->data_view(index)->quarantine();
        }
        if (frame->control != nullptr)
            frame->control->release();
        frame->release_data();
        delete frame;
        frame = next;
    }
}
} // namespace crowdb::rpc
