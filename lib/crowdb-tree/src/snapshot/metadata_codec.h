// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/mapping_persist.h"
#include "snapshot/commit_anchor.h"

#include <span>

namespace crowdb::tree::snapshot
{
// Bounded encoders for the existing segment v1/v2, directory v1 and anchor v3
// formats. Output ownership/alignment and IU padding belong to the caller.
Status encode_segment_into(const SegmentImageHeader &header, std::span<const uint64_t> words, std::span<uint8_t> output,
                           uint32_t *body_crc);
Status encode_anchor_into(const CommitAnchor &anchor, std::span<uint8_t> output);

class DirectoryEncoder
{
  public:
    explicit DirectoryEncoder(std::span<const DirEntry> entries) : entries_(entries)
    {
    }

    [[nodiscard]] uint32_t logical_bytes() const noexcept;

    [[nodiscard]] uint32_t crc() const noexcept
    {
        return full_crc_;
    }

    [[nodiscard]] bool complete() const noexcept
    {
        return logical_bytes() != 0 && position_ == logical_bytes();
    }

    // Fills only logical bytes. The last window's unused tail is untouched.
    Status next(std::span<uint8_t> output, size_t *written);

  private:
    std::span<const DirEntry> entries_;
    size_t                    position_ = 0;
    uint32_t                  body_crc_ = 0;
    uint32_t                  full_crc_ = 0;
};
} // namespace crowdb::tree::snapshot
