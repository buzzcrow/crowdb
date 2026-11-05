// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/mapping_persist.h"

#include <array>
#include <span>

namespace crowdb::tree::snapshot
{
// Decodes into caller-owned storage without allocating. IU padding is ignored;
// only slot_count words are written. Both outputs remain unchanged on failure.
Status decode_segment_into(std::span<const uint8_t> input, SegmentImageHeader *header, std::span<uint64_t> words);

// Reads the first 16 bytes and checks that the count describes exactly
// logical_bytes. This allows charging output storage before streaming the body.
Status directory_entry_count(std::span<const uint8_t> header, uint32_t logical_bytes, uint32_t *count);

class DirectoryDecoder
{
  public:
    DirectoryDecoder(std::span<DirEntry> entries, uint32_t logical_bytes);

    // Consumes the entire window, including header/trailer fragments. The caller
    // must exclude IU padding. An error is sticky; output is valid only after
    // complete() and finish().ok(), and may be partially written on failure.
    Status               next(std::span<const uint8_t> input);
    [[nodiscard]] Status finish() const;

    [[nodiscard]] bool complete() const noexcept
    {
        return error_ == Code::kOk && position_ == logical_bytes_;
    }

    [[nodiscard]] uint32_t crc() const noexcept
    {
        return full_crc_;
    }

  private:
    Status finish_piece(size_t length);

    std::span<DirEntry>     entries_;
    std::array<uint8_t, 32> piece_{};
    uint32_t                logical_bytes_ = 0;
    uint32_t                position_      = 0;
    uint32_t                body_crc_      = 0;
    uint32_t                full_crc_      = 0;
    Code                    error_         = Code::kOk;
};
} // namespace crowdb::tree::snapshot
