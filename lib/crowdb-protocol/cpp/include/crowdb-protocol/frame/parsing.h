// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-protocol/frame/base.h"
#include "crowdb-protocol/frame/encoding.h"

namespace crowdb::protocol
{

[[nodiscard]] inline FrameError parse_frame_role(std::span<const uint8_t> bytes, FrameMagic magic,
                                                 size_t payload_offset, FrameGroupFields *out)
{
    if (payload_offset == kFrameV2HeaderBytes && bytes[14] == 0 && magic != FrameMagic::StreamV2) {
        *out = {};
        return FrameError::Ok;
    }
    if (payload_offset != kFrameGroupHeaderBytes || magic != FrameMagic::StreamV2 ||
        (bytes[14] != 1 && bytes[14] != 2)) {
        return FrameError::InvalidRole;
    }
    *out = {.role  = static_cast<FrameRole>(bytes[14]),
            .value = read_u64_le(bytes.data() + 15),
            .count = read_u32_le(bytes.data() + 23)};
    if (out->count == 0 || (out->role == FrameRole::Head && out->value == 0)) {
        return FrameError::InvalidGroup;
    }
    return FrameError::Ok;
}

[[nodiscard]] inline FrameError parse_frame(std::span<const uint8_t> bytes, FrameChunkId expected_chunk_id,
                                            ParsedFrame *out)
{
    if (bytes.size() < kFrameHeaderPrefixBytes) {
        return FrameError::Incomplete;
    }
    const uint16_t magic_value  = read_u16_le(bytes.data());
    const auto     parsed_magic = frame_magic(magic_value);
    if (!parsed_magic.has_value()) {
        return FrameError::UnknownMagic;
    }
    const uint16_t payload_offset = read_u16_le(bytes.data() + 2);
    const uint16_t payload_size   = read_u16_le(bytes.data() + 4);
    if (payload_offset < kFrameHeaderPrefixBytes) {
        return FrameError::InvalidPayloadOffset;
    }
    if ((magic_value & 0xffU) == 2 && payload_offset != kFrameV2HeaderBytes &&
        payload_offset != kFrameGroupHeaderBytes) {
        return FrameError::InvalidPayloadOffset;
    }
    const size_t physical_length = static_cast<size_t>(payload_offset) + payload_size + kFrameFooterBytes;
    if (physical_length > kMaxFrameBytes) {
        return FrameError::FrameTooLarge;
    }
    if (bytes.size() < physical_length) {
        return FrameError::Incomplete;
    }
    const size_t     footer_start = physical_length - kFrameFooterBytes;
    const auto       magic        = *parsed_magic;
    FrameGroupFields group;
    if (is_v2(magic)) {
        const FrameError role_error = parse_frame_role(bytes, magic, payload_offset, &group);
        if (role_error != FrameError::Ok) {
            return role_error;
        }
    }
    const uint32_t crc          = crowdb::common::crc32c(bytes.data(), footer_start);
    const uint32_t expected_crc = read_u32_le(bytes.data() + footer_start);
    if (crc != expected_crc &&
        (is_v2(magic) || crowdb::common::crc32c_update(crc, bytes.data() + footer_start + 4, 16) != expected_crc)) {
        return FrameError::ChecksumMismatch;
    }
    const FrameChunkId chunk_id{
        .high = read_u64_be(bytes.data() + footer_start + 4),
        .low  = read_u64_be(bytes.data() + footer_start + 12),
    };
    if (chunk_id != expected_chunk_id) {
        return FrameError::ChunkIdMismatch;
    }
    if (out != nullptr) {
        *out = ParsedFrame{
            .header =
                {
                         .magic          = *parsed_magic,
                         .payload_offset = payload_offset,
                         .payload_size   = payload_size,
                         .write_time_ms  = read_u64_le(bytes.data() + 6),
                         },
            .group           = group,
            .payload         = bytes.subspan(payload_offset, payload_size),
            .chunk_id        = chunk_id,
            .physical_length = physical_length,
        };
    }
    return FrameError::Ok;
}

[[nodiscard]] inline FrameError parse_frames(std::span<const uint8_t> bytes, FrameChunkId expected_chunk_id,
                                             FrameMagic expected_magic, size_t logical_length,
                                             std::vector<uint8_t> *payload)
{
    if (payload == nullptr ||
        framed_physical_length(logical_length, ordinary_header_bytes(expected_magic)) != bytes.size()) {
        return FrameError::Incomplete;
    }
    payload->clear();
    payload->reserve(logical_length);
    size_t offset = 0;
    while (offset < bytes.size()) {
        ParsedFrame      frame;
        const FrameError error = parse_frame(bytes.subspan(offset), expected_chunk_id, &frame);
        if (error != FrameError::Ok) {
            return error;
        }
        if (frame.header.magic != expected_magic || frame.physical_length > bytes.size() - offset) {
            return FrameError::UnknownMagic;
        }
        payload->insert(payload->end(), frame.payload.begin(), frame.payload.end());
        offset += frame.physical_length;
    }
    return payload->size() == logical_length ? FrameError::Ok : FrameError::Incomplete;
}

/// Validates a concatenated sequence whose first frame already identifies its
/// public kind. DiskIO uses this boundary check without needing caller-private
/// location metadata: each footer supplies the expected chunk ID for parsing.
[[nodiscard]] inline FrameError validate_frame_sequence(std::span<const uint8_t> bytes)
{
    size_t offset = 0;
    while (offset < bytes.size()) {
        const auto remaining = bytes.subspan(offset);
        if (remaining.size() < kFrameHeaderPrefixBytes) {
            return FrameError::Incomplete;
        }
        const size_t physical_length = static_cast<size_t>(read_u16_le(remaining.data() + 2)) +
                                       read_u16_le(remaining.data() + 4) + kFrameFooterBytes;
        if (physical_length > remaining.size() || physical_length < kFrameHeaderPrefixBytes + kFrameFooterBytes) {
            return FrameError::Incomplete;
        }
        const size_t     footer_start = physical_length - kFrameFooterBytes;
        ParsedFrame      frame;
        const FrameError error = parse_frame(remaining.first(physical_length),
                                             {.high = read_u64_be(remaining.data() + footer_start + 4),
                                              .low  = read_u64_be(remaining.data() + footer_start + 12)},
                                             &frame);
        if (error != FrameError::Ok) {
            return error;
        }
        offset += frame.physical_length;
    }
    return FrameError::Ok;
}

} // namespace crowdb::protocol
