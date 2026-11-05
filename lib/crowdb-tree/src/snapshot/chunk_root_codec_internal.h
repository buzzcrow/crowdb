// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_frame.h"
#include "crowdb-common/crc32c.h"
#include "snapshot/chunk_root_codec.h"

namespace crowdb::tree::chunk_codec
{
inline constexpr uint64_t kRootMagic  = 0x33544F4F524B4843ULL;
inline constexpr uint64_t kIndexMagic = 0x335845444E494843ULL;

inline uint64_t word(std::span<const uint8_t> input, size_t index)
{
    uint64_t value = 0;
    for (size_t byte = 0; byte < 8; ++byte) {
        value |= uint64_t{input[(index * 8) + byte]} << (byte * 8);
    }
    return value;
}

inline void put(std::span<uint8_t> out, size_t index, uint64_t value)
{
    for (size_t byte = 0; byte < 8; ++byte) {
        out[(index * 8) + byte] = static_cast<uint8_t>(value >> (byte * 8));
    }
}

inline void seal(std::span<uint8_t> out)
{
    const auto crc = crowdb::common::crc32c(out.data(), out.size() - 4);
    for (size_t i = 0; i < 4; ++i) {
        out[out.size() - 4 + i] = static_cast<uint8_t>(crc >> (8 * i));
    }
}

inline bool verified(std::span<const uint8_t> input, size_t header)
{
    if (input.size() < header + 4) {
        return false;
    }
    uint32_t saved = 0;
    for (size_t i = 0; i < 4; ++i) {
        saved |= uint32_t{input[input.size() - 4 + i]} << (i * 8);
    }
    return saved == crowdb::common::crc32c(input.data(), input.size() - 4);
}

inline bool valid_pack(const detail::ChunkFixedPackRef &pack, PagePurpose purpose)
{
    return pack.stage.owner_epoch != 0 && (pack.stage.operation_high != 0 || pack.stage.operation_low != 0) &&
           pack.purpose == purpose && detail::chunk_purpose(pack.chunk) == purpose && pack.length != 0 &&
           pack.length <= detail::kMaxChunkPackBytes &&
           pack.offset <= UINT64_MAX - detail::chunk_framed_length(pack.length);
}

inline void pack(std::span<uint8_t> out, const detail::ChunkFixedPackRef &ref)
{
    const uint64_t words[]{ref.stage.owner_epoch,
                           ref.stage.operation_high,
                           ref.stage.operation_low,
                           ref.chunk.high,
                           ref.chunk.low,
                           ref.offset,
                           ref.length,
                           ref.checksum,
                           static_cast<uint32_t>(ref.purpose)};
    for (size_t i = 0; i < std::size(words); ++i) {
        put(out, i, words[i]);
    }
}

inline bool unpack(std::span<const uint8_t> input, PagePurpose purpose, detail::ChunkFixedPackRef *out)
{
    if (word(input, 6) > UINT32_MAX || word(input, 7) > UINT32_MAX ||
        word(input, 8) != static_cast<uint32_t>(purpose)) {
        return false;
    }
    *out = {
        .stage    = {.owner_epoch = word(input, 0), .operation_high = word(input, 1), .operation_low = word(input, 2)},
        .chunk    = {word(input, 3), word(input, 4)},
        .offset   = word(input, 5),
        .length   = static_cast<uint32_t>(word(input, 6)),
        .checksum = static_cast<uint32_t>(word(input, 7)),
        .purpose  = purpose
    };
    return valid_pack(*out, purpose);
}
} // namespace crowdb::tree::chunk_codec
