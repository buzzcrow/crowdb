// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_page_store.h"

namespace crowdb::tree::detail
{
// A validated view over an owned format-seven manifest. No mutable legacy
// PageStore or executor is created by the migration reader.
class LegacyChunkLayout
{
  public:
    static Status parse(std::span<const uint8_t> bytes, uint64_t tree, uint64_t epoch, LegacyChunkLayout *out);
    [[nodiscard]] ChunkPagePack         pack(size_t index) const noexcept;
    [[nodiscard]] ChunkReferenceSegment segment(size_t index) const noexcept;
    [[nodiscard]] Status                validate_segment(size_t index, std::span<const uint8_t> bytes) const;
    [[nodiscard]] uint64_t              generation() const noexcept;
    [[nodiscard]] uint64_t              logical_size() const noexcept;
    [[nodiscard]] uint64_t              wal_offset() const noexcept;

    [[nodiscard]] size_t pack_count() const noexcept
    {
        return packs_;
    }

    [[nodiscard]] size_t segment_count() const noexcept
    {
        return segments_;
    }

  private:
    std::span<const uint8_t> bytes_;
    size_t                   segments_     = 0;
    size_t                   packs_        = 0;
    size_t                   packs_offset_ = 0;
};
} // namespace crowdb::tree::detail
