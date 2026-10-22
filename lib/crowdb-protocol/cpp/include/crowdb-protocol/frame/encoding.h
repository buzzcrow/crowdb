// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-protocol/frame/base.h"

namespace crowdb::protocol
{

[[nodiscard]] inline FrameError encode_frame(FrameMagic magic, FrameChunkId chunk_id, std::span<const uint8_t> payload,
                                             uint64_t write_time_ms, std::vector<uint8_t> *out)
{
    if (magic == FrameMagic::StreamV2) {
        return FrameError::InvalidRole;
    }
    const size_t header_bytes = kFrameV2HeaderBytes;
    if (out == nullptr || !valid_magic(static_cast<uint16_t>(magic)) ||
        payload.size() > kMaxFrameBytes - header_bytes - kFrameFooterBytes) {
        return FrameError::FrameTooLarge;
    }
    out->clear();
    out->reserve(header_bytes + payload.size() + kFrameFooterBytes);
    append_u16_le(out, static_cast<uint16_t>(magic));
    append_u16_le(out, static_cast<uint16_t>(header_bytes));
    append_u16_le(out, static_cast<uint16_t>(payload.size()));
    append_u64_le(out, write_time_ms);
    out->push_back(0);
    out->insert(out->end(), payload.begin(), payload.end());
    const uint32_t prefix_crc = crowdb::common::crc32c(out->data(), out->size());
    append_u64_be(out, chunk_id.high);
    append_u64_be(out, chunk_id.low);
    const uint32_t checksum = prefix_crc;
    out->insert(out->begin() + static_cast<ptrdiff_t>(header_bytes + payload.size()),
                {static_cast<uint8_t>(checksum), static_cast<uint8_t>(checksum >> 8U),
                 static_cast<uint8_t>(checksum >> 16U), static_cast<uint8_t>(checksum >> 24U)});
    return FrameError::Ok;
}

[[nodiscard]] inline size_t framed_physical_length(size_t payload_size, size_t header_bytes = kFrameV2HeaderBytes)
{
    const size_t capacity    = kMaxFrameBytes - header_bytes - kFrameFooterBytes;
    const size_t full_frames = payload_size / capacity;
    const size_t tail        = payload_size % capacity;
    if (full_frames > (SIZE_MAX - (tail == 0 ? 0 : tail + header_bytes + kFrameFooterBytes)) / kMaxFrameBytes) {
        return 0;
    }
    return (full_frames * kMaxFrameBytes) + (tail == 0 ? 0 : tail + header_bytes + kFrameFooterBytes);
}

[[nodiscard]] inline FrameError encode_frames(FrameMagic magic, FrameChunkId chunk_id, std::span<const uint8_t> payload,
                                              uint64_t write_time_ms, std::vector<uint8_t> *out)
{
    const size_t header_bytes = kFrameV2HeaderBytes;
    const size_t capacity     = kMaxFrameBytes - header_bytes - kFrameFooterBytes;
    if (out == nullptr || (framed_physical_length(payload.size(), header_bytes) == 0 && !payload.empty())) {
        return FrameError::FrameTooLarge;
    }
    out->clear();
    out->reserve(framed_physical_length(payload.size(), header_bytes));
    for (size_t offset = 0; offset < payload.size(); offset += capacity) {
        std::vector<uint8_t> frame;
        const size_t         length = std::min(capacity, payload.size() - offset);
        const FrameError error = encode_frame(magic, chunk_id, payload.subspan(offset, length), write_time_ms, &frame);
        if (error != FrameError::Ok) {
            return error;
        }
        out->insert(out->end(), frame.begin(), frame.end());
    }
    return FrameError::Ok;
}

} // namespace crowdb::protocol
