// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-common/crc32c.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace crowdb::protocol
{

inline constexpr size_t kFrameHeaderPrefixBytes = 14;
inline constexpr size_t kFrameFooterBytes       = 20;
inline constexpr size_t kMaxFrameBytes          = 64 * 1024;
inline constexpr size_t kMaxFramePayloadBytes   = kMaxFrameBytes - kFrameHeaderPrefixBytes - kFrameFooterBytes;

enum class FrameMagic : uint16_t {
    RepoSmallV1 = 0x0101,
    RepoLargeV1 = 0x0201,
    StreamV1    = 0x0301,
    BtreePageV1 = 0x0401,
    PageIndexV1 = 0x0501,
};

struct FrameChunkId
{
    uint64_t high = 0;
    uint64_t low  = 0;

    [[nodiscard]] bool operator==(const FrameChunkId &) const = default;
};

struct FrameHeaderPrefix
{
    FrameMagic magic          = FrameMagic::RepoSmallV1;
    uint16_t   payload_offset = 0;
    uint16_t   payload_size   = 0;
    uint64_t   write_time_ms  = 0;
};

enum class FrameError : uint8_t {
    Ok,
    Incomplete,
    UnknownMagic,
    InvalidPayloadOffset,
    FrameTooLarge,
    ChunkIdMismatch,
    ChecksumMismatch,
};

struct ParsedFrame
{
    FrameHeaderPrefix        header;
    std::span<const uint8_t> payload;
    FrameChunkId             chunk_id;
    size_t                   physical_length = 0;
};

[[nodiscard]] inline bool valid_magic(uint16_t value)
{
    return value >= static_cast<uint16_t>(FrameMagic::RepoSmallV1) &&
           value <= static_cast<uint16_t>(FrameMagic::PageIndexV1) && (value & 0xffU) == 1;
}

[[nodiscard]] inline uint16_t read_u16_le(const uint8_t *data)
{
    return static_cast<uint16_t>(data[0]) | (static_cast<uint16_t>(data[1]) << 8U);
}

[[nodiscard]] inline uint32_t read_u32_le(const uint8_t *data)
{
    return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8U) |
           (static_cast<uint32_t>(data[2]) << 16U) | (static_cast<uint32_t>(data[3]) << 24U);
}

[[nodiscard]] inline uint64_t read_u64_le(const uint8_t *data)
{
    uint64_t value = 0;
    for (uint32_t i = 0; i != 8; ++i) {
        value |= static_cast<uint64_t>(data[i]) << (i * 8U);
    }
    return value;
}

[[nodiscard]] inline uint64_t read_u64_be(const uint8_t *data)
{
    uint64_t value = 0;
    for (uint32_t i = 0; i != 8; ++i) {
        value = (value << 8U) | data[i];
    }
    return value;
}

inline void append_u16_le(std::vector<uint8_t> *out, uint16_t value)
{
    out->push_back(static_cast<uint8_t>(value));
    out->push_back(static_cast<uint8_t>(value >> 8U));
}

inline void append_u32_le(std::vector<uint8_t> *out, uint32_t value)
{
    for (uint32_t i = 0; i != 4; ++i) {
        out->push_back(static_cast<uint8_t>(value >> (i * 8U)));
    }
}

inline void append_u64_le(std::vector<uint8_t> *out, uint64_t value)
{
    for (uint32_t i = 0; i != 8; ++i) {
        out->push_back(static_cast<uint8_t>(value >> (i * 8U)));
    }
}

inline void append_u64_be(std::vector<uint8_t> *out, uint64_t value)
{
    for (int shift = 56; shift >= 0; shift -= 8) {
        out->push_back(static_cast<uint8_t>(value >> shift));
    }
}

[[nodiscard]] inline FrameError encode_frame(FrameMagic magic, FrameChunkId chunk_id, std::span<const uint8_t> payload,
                                             uint64_t write_time_ms, std::vector<uint8_t> *out)
{
    if (out == nullptr || payload.size() > kMaxFramePayloadBytes) {
        return FrameError::FrameTooLarge;
    }
    out->clear();
    out->reserve(kFrameHeaderPrefixBytes + payload.size() + kFrameFooterBytes);
    append_u16_le(out, static_cast<uint16_t>(magic));
    append_u16_le(out, static_cast<uint16_t>(kFrameHeaderPrefixBytes));
    append_u16_le(out, static_cast<uint16_t>(payload.size()));
    append_u64_le(out, write_time_ms);
    out->insert(out->end(), payload.begin(), payload.end());
    append_u64_be(out, chunk_id.high);
    append_u64_be(out, chunk_id.low);
    const uint32_t checksum = crowdb::common::crc32c(out->data(), out->size());
    out->insert(out->begin() + static_cast<ptrdiff_t>(kFrameHeaderPrefixBytes + payload.size()),
                {static_cast<uint8_t>(checksum), static_cast<uint8_t>(checksum >> 8U),
                 static_cast<uint8_t>(checksum >> 16U), static_cast<uint8_t>(checksum >> 24U)});
    return FrameError::Ok;
}

[[nodiscard]] inline FrameError parse_frame(std::span<const uint8_t> bytes, FrameChunkId expected_chunk_id,
                                            ParsedFrame *out)
{
    if (bytes.size() < kFrameHeaderPrefixBytes) {
        return FrameError::Incomplete;
    }
    const uint16_t magic_value = read_u16_le(bytes.data());
    if (!valid_magic(magic_value)) {
        return FrameError::UnknownMagic;
    }
    const uint16_t payload_offset = read_u16_le(bytes.data() + 2);
    const uint16_t payload_size   = read_u16_le(bytes.data() + 4);
    if (payload_offset < kFrameHeaderPrefixBytes) {
        return FrameError::InvalidPayloadOffset;
    }
    const size_t physical_length = static_cast<size_t>(payload_offset) + payload_size + kFrameFooterBytes;
    if (physical_length > kMaxFrameBytes) {
        return FrameError::FrameTooLarge;
    }
    if (bytes.size() < physical_length) {
        return FrameError::Incomplete;
    }
    const size_t         footer_start = physical_length - kFrameFooterBytes;
    std::vector<uint8_t> checksum_input;
    checksum_input.reserve(footer_start + 16);
    checksum_input.insert(checksum_input.end(), bytes.begin(), bytes.begin() + static_cast<ptrdiff_t>(footer_start));
    checksum_input.insert(checksum_input.end(), bytes.begin() + static_cast<ptrdiff_t>(footer_start + 4),
                          bytes.begin() + static_cast<ptrdiff_t>(physical_length));
    if (crowdb::common::crc32c(checksum_input.data(), checksum_input.size()) !=
        read_u32_le(bytes.data() + footer_start)) {
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
                         .magic          = static_cast<FrameMagic>(magic_value),
                         .payload_offset = payload_offset,
                         .payload_size   = payload_size,
                         .write_time_ms  = read_u64_le(bytes.data() + 6),
                         },
            .payload         = bytes.subspan(payload_offset, payload_size),
            .chunk_id        = chunk_id,
            .physical_length = physical_length,
        };
    }
    return FrameError::Ok;
}

} // namespace crowdb::protocol
