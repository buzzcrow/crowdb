// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_catalog_codec.h"

#include "chunk_frame.h"

#include <algorithm>

namespace crowdb::tree::detail
{
namespace
{
template <typename T> void append_scalar(std::vector<uint8_t> *out, T value)
{
    for (size_t i = 0; i < sizeof(T); ++i) {
        out->push_back(static_cast<uint8_t>((value >> (i * 8U)) & 0xffU));
    }
}

class BlobReader
{
  public:
    explicit BlobReader(const std::vector<uint8_t> &bytes) : bytes_(bytes)
    {
    }

    template <typename T> bool scalar(T *out)
    {
        if (out == nullptr || offset_ > bytes_.size() || sizeof(T) > bytes_.size() - offset_) {
            return false;
        }
        T value = 0;
        for (size_t i = 0; i < sizeof(T); ++i) {
            value |= static_cast<T>(bytes_[offset_ + i]) << (i * 8U);
        }
        offset_ += sizeof(T);
        *out = value;
        return true;
    }

    [[nodiscard]] bool complete() const
    {
        return offset_ == bytes_.size();
    }

    [[nodiscard]] size_t remaining() const
    {
        return bytes_.size() - offset_;
    }

  private:
    const std::vector<uint8_t> &bytes_;
    size_t                      offset_ = 0;
};

} // namespace

namespace
{
bool valid_page_ref(const ChunkPageRef &ref)
{
    return ref.length > 0 && ref.length <= kMaxChunkPackBytes &&
           (ref.frame_revision == kLegacyChunkFrameRevision || ref.frame_revision == kChunkFrameRevision);
}
} // namespace

uint32_t encoded_page_ref_length(const ChunkPageRef &ref)
{
    return ref.length | (ref.frame_revision == kChunkFrameRevision ? 0x80000000U : 0U);
}

std::vector<uint8_t> encode_reference_segment(const ChunkReferenceSegmentImage &segment)
{
    if (std::any_of(segment.refs.begin(), segment.refs.end(), [](const auto &ref) { return !valid_page_ref(ref); })) {
        return {};
    }
    std::vector<uint8_t> out;
    out.reserve(24 + (segment.refs.size() * 28));
    append_scalar(&out, segment.object_id);
    append_scalar(&out, segment.first_ordinal);
    append_scalar(&out, static_cast<uint64_t>(segment.refs.size()));
    for (const auto &ref : segment.refs) {
        append_scalar(&out, ref.chunk_id.high);
        append_scalar(&out, ref.chunk_id.low);
        append_scalar(&out, ref.offset);
        append_scalar(&out, encoded_page_ref_length(ref));
    }
    return out;
}

std::shared_ptr<const ChunkReferenceSegmentImage> decode_reference_segment(const std::vector<uint8_t> &bytes)
{
    BlobReader reader(bytes);
    auto       segment = std::make_shared<ChunkReferenceSegmentImage>();
    uint64_t   count   = 0;
    if (!reader.scalar(&segment->object_id) || !reader.scalar(&segment->first_ordinal) || !reader.scalar(&count) ||
        count > reader.remaining() / 28U) {
        return nullptr;
    }
    segment->refs.reserve(static_cast<size_t>(count));
    for (uint64_t i = 0; i < count; ++i) {
        ChunkPageRef ref;
        if (!reader.scalar(&ref.chunk_id.high) || !reader.scalar(&ref.chunk_id.low) || !reader.scalar(&ref.offset) ||
            !reader.scalar(&ref.length)) {
            return nullptr;
        }
        ref.frame_revision = (ref.length & 0x80000000U) == 0 ? kLegacyChunkFrameRevision : kChunkFrameRevision;
        ref.length &= 0x7fffffffU;
        if (ref.length == 0 || ref.length > kMaxChunkPackBytes) {
            return nullptr;
        }
        segment->refs.push_back(ref);
    }
    return reader.complete() ? segment : nullptr;
}

std::vector<uint8_t> encode_manifest(const ChunkManifest &manifest)
{
    if (manifest.format_version > kChunkManifestFormat ||
        std::any_of(manifest.packs.begin(), manifest.packs.end(), [&](const auto &pack) {
            return !valid_page_ref(pack.ref) ||
                   (manifest.format_version < 7 && pack.ref.frame_revision != kLegacyChunkFrameRevision);
        })) {
        return {};
    }
    std::vector<uint8_t> out;
    append_scalar(&out, manifest.format_version);
    append_scalar(&out, manifest.tree_id);
    append_scalar(&out, manifest.generation);
    append_scalar(&out, manifest.owner_epoch);
    append_scalar(&out, manifest.logical_size);
    append_scalar(&out, manifest.published_at_ms);
    append_scalar(&out, manifest.packs_reused);
    append_scalar(&out, manifest.pack_bytes_reused);
    if (manifest.format_version >= 4) {
        append_scalar(&out, manifest.wal_replay_offset);
    }
    append_scalar(&out, manifest.checksum);
    append_scalar(&out, static_cast<uint64_t>(manifest.reference_segments.size()));
    for (const auto &segment : manifest.reference_segments) {
        append_scalar(&out, segment.owner_tree_id);
        append_scalar(&out, segment.object_id);
        append_scalar(&out, segment.first_ordinal);
        append_scalar(&out, segment.ref_count);
        append_scalar(&out, segment.checksum);
        append_scalar(&out, static_cast<uint8_t>(segment.reused));
    }
    append_scalar(&out, static_cast<uint64_t>(manifest.packs.size()));
    for (const auto &pack : manifest.packs) {
        append_scalar(&out, pack.owner_tree_id);
        append_scalar(&out, pack.ordinal);
        append_scalar(&out, pack.logical_offset);
        append_scalar(&out, pack.ref.chunk_id.high);
        append_scalar(&out, pack.ref.chunk_id.low);
        append_scalar(&out, pack.ref.offset);
        append_scalar(&out, encoded_page_ref_length(pack.ref));
        append_scalar(&out, static_cast<uint8_t>(pack.reused));
    }
    return out;
}

std::shared_ptr<const ChunkManifest> decode_manifest(const std::vector<uint8_t> &bytes)
{
    BlobReader reader(bytes);
    auto       manifest        = std::make_shared<ChunkManifest>();
    uint64_t   reference_count = 0;
    if (!reader.scalar(&manifest->format_version) || !reader.scalar(&manifest->tree_id) ||
        !reader.scalar(&manifest->generation) || !reader.scalar(&manifest->owner_epoch) ||
        !reader.scalar(&manifest->logical_size) || !reader.scalar(&manifest->published_at_ms) ||
        !reader.scalar(&manifest->packs_reused) || !reader.scalar(&manifest->pack_bytes_reused) ||
        (manifest->format_version >= 4 && !reader.scalar(&manifest->wal_replay_offset)) ||
        !reader.scalar(&manifest->checksum) || !reader.scalar(&reference_count) ||
        reference_count > reader.remaining() / 33U) {
        return nullptr;
    }
    manifest->reference_segments.reserve(static_cast<size_t>(reference_count));
    for (uint64_t i = 0; i < reference_count; ++i) {
        ChunkReferenceSegment segment;
        uint8_t               reused = 0;
        if (!reader.scalar(&segment.owner_tree_id) || !reader.scalar(&segment.object_id) ||
            !reader.scalar(&segment.first_ordinal) || !reader.scalar(&segment.ref_count) ||
            !reader.scalar(&segment.checksum) || !reader.scalar(&reused) || reused > 1) {
            return nullptr;
        }
        segment.reused = reused != 0;
        manifest->reference_segments.push_back(segment);
    }
    uint64_t pack_count = 0;
    if (!reader.scalar(&pack_count) || pack_count > reader.remaining() / 53U) {
        return nullptr;
    }
    manifest->packs.reserve(static_cast<size_t>(pack_count));
    for (uint64_t i = 0; i < pack_count; ++i) {
        ChunkPagePack pack;
        uint8_t       reused = 0;
        if (!reader.scalar(&pack.owner_tree_id) || !reader.scalar(&pack.ordinal) ||
            !reader.scalar(&pack.logical_offset) || !reader.scalar(&pack.ref.chunk_id.high) ||
            !reader.scalar(&pack.ref.chunk_id.low) || !reader.scalar(&pack.ref.offset) ||
            !reader.scalar(&pack.ref.length) || !reader.scalar(&reused) || reused > 1) {
            return nullptr;
        }
        pack.ref.frame_revision =
            (pack.ref.length & 0x80000000U) == 0 ? kLegacyChunkFrameRevision : kChunkFrameRevision;
        pack.ref.length &= 0x7fffffffU;
        if (pack.ref.length == 0 || pack.ref.length > kMaxChunkPackBytes ||
            (manifest->format_version < 7 && pack.ref.frame_revision != kLegacyChunkFrameRevision)) {
            return nullptr;
        }
        pack.reused = reused != 0;
        manifest->packs.push_back(pack);
    }
    return reader.complete() && manifest->format_version <= kChunkManifestFormat ? manifest : nullptr;
}

} // namespace crowdb::tree::detail
