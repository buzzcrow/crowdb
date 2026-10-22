// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-common/crc32c.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
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
    RepoSmallV2 = 0x0102,
    RepoLargeV2 = 0x0202,
    StreamV2    = 0x0302,
    BtreePageV2 = 0x0402,
    PageIndexV2 = 0x0502,
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
    InvalidRole,
    InvalidGroup,
    LengthOverflow,
};

inline constexpr size_t kFrameV2HeaderBytes        = 15;
inline constexpr size_t kFrameGroupHeaderBytes     = 27;
inline constexpr size_t kMaxFrameV2PayloadBytes    = kMaxFrameBytes - kFrameV2HeaderBytes - kFrameFooterBytes;
inline constexpr size_t kMaxFrameGroupPayloadBytes = kMaxFrameBytes - kFrameGroupHeaderBytes - kFrameFooterBytes;

enum class FrameRole : uint8_t { Ordinary, Head, Follower };

struct FrameGroupFields
{
    FrameRole role  = FrameRole::Ordinary;
    uint64_t  value = 0;
    uint32_t  count = 0;
};

[[nodiscard]] inline bool is_v2(FrameMagic magic)
{
    return (static_cast<uint16_t>(magic) & 0xffU) == 2;
}

[[nodiscard]] inline size_t ordinary_header_bytes(FrameMagic magic)
{
    return is_v2(magic) ? kFrameV2HeaderBytes : kFrameHeaderPrefixBytes;
}

struct ParsedFrame
{
    FrameHeaderPrefix        header;
    FrameGroupFields         group;
    std::span<const uint8_t> payload;
    FrameChunkId             chunk_id;
    size_t                   physical_length = 0;
};

[[nodiscard]] inline std::optional<FrameMagic> frame_magic(uint16_t value)
{
    switch (value) {
    case 0x0101:
        return FrameMagic::RepoSmallV1;
    case 0x0201:
        return FrameMagic::RepoLargeV1;
    case 0x0301:
        return FrameMagic::StreamV1;
    case 0x0401:
        return FrameMagic::BtreePageV1;
    case 0x0501:
        return FrameMagic::PageIndexV1;
    case 0x0102:
        return FrameMagic::RepoSmallV2;
    case 0x0202:
        return FrameMagic::RepoLargeV2;
    case 0x0302:
        return FrameMagic::StreamV2;
    case 0x0402:
        return FrameMagic::BtreePageV2;
    case 0x0502:
        return FrameMagic::PageIndexV2;
    default:
        return std::nullopt;
    }
}

[[nodiscard]] inline bool valid_magic(uint16_t value)
{
    return frame_magic(value).has_value();
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

} // namespace crowdb::protocol
