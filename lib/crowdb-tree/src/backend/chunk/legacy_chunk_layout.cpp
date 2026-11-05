// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "legacy_chunk_layout.h"

#include "chunk_frame.h"
#include "crowdb-tree/maptable/frame_page.h"

namespace crowdb::tree::detail
{
namespace
{
constexpr size_t kHeader      = 80;
constexpr size_t kSegment     = 33;
constexpr size_t kPack        = 53;
constexpr size_t kSegmentRefs = 256;
} // namespace

Status LegacyChunkLayout::parse(std::span<const uint8_t> bytes, uint64_t tree, uint64_t epoch, LegacyChunkLayout *out)
{
    if (out == nullptr || bytes.size() < kHeader + 8 || frame_u32(bytes.data(), 0) != kChunkManifestFormat ||
        frame_u64(bytes.data(), 4) != tree || frame_u64(bytes.data(), 12) == 0 ||
        frame_u64(bytes.data(), 12) == UINT64_MAX || frame_u64(bytes.data(), 20) > epoch) {
        return Status::corruption("invalid legacy Chunk manifest identity");
    }
    LegacyChunkLayout result;
    result.bytes_       = bytes;
    const auto segments = frame_u64(bytes.data(), 72);
    if (segments > (bytes.size() - kHeader - 8) / kSegment) {
        return Status::corruption("invalid legacy Chunk segment count");
    }
    result.segments_     = static_cast<size_t>(segments);
    result.packs_offset_ = kHeader + (result.segments_ * kSegment) + 8;
    const auto packs     = frame_u64(bytes.data(), result.packs_offset_ - 8);
    if (packs != (bytes.size() - result.packs_offset_) / kPack || (bytes.size() - result.packs_offset_) % kPack != 0 ||
        segments != (packs + kSegmentRefs - 1) / kSegmentRefs) {
        return Status::corruption("invalid legacy Chunk pack directory");
    }
    result.packs_ = static_cast<size_t>(packs);
    auto crc      = common::crc32c(bytes.data(), 68);
    crc           = common::crc32c_update(crc, bytes.data() + result.packs_offset_, result.packs_ * kPack);
    crc           = common::crc32c_update(crc, bytes.data() + kHeader, result.segments_ * kSegment);
    if (crc != frame_u32(bytes.data(), 68)) {
        return Status::corruption("legacy Chunk manifest checksum mismatch");
    }
    uint64_t end          = 0;
    uint64_t reused       = 0;
    uint64_t reused_bytes = 0;
    for (size_t i = 0; i < result.packs_; ++i) {
        const auto pack = result.pack(i);
        const auto flag = bytes[result.packs_offset_ + (i * kPack) + 52];
        if (pack.ordinal != i || pack.owner_tree_id == 0 || pack.ref.chunk_id.empty() ||
            !valid_page_purpose(chunk_purpose(pack.ref.chunk_id)) || pack.ref.length == 0 ||
            pack.ref.length > kMaxChunkPackBytes || pack.logical_offset < end ||
            pack.logical_offset > result.logical_size() ||
            pack.ref.length > result.logical_size() - pack.logical_offset || flag > 1 ||
            chunk_framed_length(pack.ref.length) > UINT64_MAX - pack.ref.offset) {
            return Status::corruption("invalid legacy Chunk pack bounds");
        }
        end = pack.logical_offset + pack.ref.length;
        reused += pack.reused ? 1 : 0;
        reused_bytes += pack.reused ? pack.ref.length : 0;
    }
    if (reused != frame_u64(bytes.data(), 44) || reused_bytes != frame_u64(bytes.data(), 52)) {
        return Status::corruption("invalid legacy Chunk reuse counts");
    }
    *out = result;
    return Status::Ok();
}

ChunkPagePack LegacyChunkLayout::pack(size_t index) const noexcept
{
    const auto *p = bytes_.data() + packs_offset_ + (index * kPack);
    return {
        .owner_tree_id  = frame_u64(p, 0),
        .ordinal        = frame_u64(p, 8),
        .logical_offset = frame_u64(p, 16),
        .ref            = {.chunk_id = {frame_u64(p, 24), frame_u64(p, 32)},
                           .offset   = frame_u64(p, 40),
                           .length   = frame_u32(p, 48)},
        .reused         = p[52] != 0
    };
}

ChunkReferenceSegment LegacyChunkLayout::segment(size_t index) const noexcept
{
    const auto *p = bytes_.data() + kHeader + (index * kSegment);
    return {.owner_tree_id = frame_u64(p, 0),
            .object_id     = frame_u64(p, 8),
            .first_ordinal = frame_u64(p, 16),
            .ref_count     = frame_u32(p, 24),
            .checksum      = frame_u32(p, 28),
            .reused        = p[32] != 0};
}

Status LegacyChunkLayout::validate_segment(size_t index, std::span<const uint8_t> bytes) const
{
    const auto descriptor = segment(index);
    const auto first      = index * kSegmentRefs;
    const auto count      = std::min(kSegmentRefs, packs_ - first);
    if (descriptor.owner_tree_id == 0 || descriptor.object_id == 0 || descriptor.first_ordinal != first ||
        descriptor.ref_count != count || bytes_[kHeader + (index * kSegment) + 32] > 1 ||
        bytes.size() != 24 + (count * 28) || frame_u64(bytes.data(), 0) != descriptor.object_id ||
        frame_u64(bytes.data(), 8) != first || frame_u64(bytes.data(), 16) != count) {
        return Status::corruption("invalid legacy Chunk reference segment");
    }
    auto crc = common::crc32c(bytes.data(), 16);
    crc      = common::crc32c_update(crc, bytes.data() + 24, bytes.size() - 24);
    if (crc != descriptor.checksum) {
        return Status::corruption("legacy Chunk reference checksum mismatch");
    }
    for (size_t i = 0; i < count; ++i) {
        const auto  ref = pack(first + i).ref;
        const auto *p   = bytes.data() + 24 + (i * 28);
        if (ref.chunk_id.high != frame_u64(p, 0) || ref.chunk_id.low != frame_u64(p, 8) ||
            ref.offset != frame_u64(p, 16) || ref.length != frame_u32(p, 24)) {
            return Status::corruption("legacy Chunk reference differs from its pack");
        }
    }
    return Status::Ok();
}

uint64_t LegacyChunkLayout::generation() const noexcept
{
    return frame_u64(bytes_.data(), 12);
}

uint64_t LegacyChunkLayout::logical_size() const noexcept
{
    return frame_u64(bytes_.data(), 28);
}

uint64_t LegacyChunkLayout::wal_offset() const noexcept
{
    return frame_u64(bytes_.data(), 60);
}
} // namespace crowdb::tree::detail
