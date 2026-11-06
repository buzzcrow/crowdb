// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "snapshot/chunk_root_codec_internal.h"

namespace crowdb::tree
{
namespace
{
bool valid_frame(const ChunkIndexFrame &frame)
{
    if (frame.page_id == kInvalidPageId || frame.frame_bytes < kFrameHeaderSize + kFrameTrailerSize ||
        frame.logical_bytes == 0 || frame.logical_bytes > frame.pack.length ||
        !chunk_codec::valid_pack(frame.pack, PagePurpose::kBtreePage)) {
        return false;
    }
    if (frame.type == page_type::kOverflowFrame) {
        return frame.stored_reference_words.empty() && frame.overflow_bytes != 0 &&
               frame.overflow_bytes <= frame.frame_bytes - kFrameHeaderSize - kFrameTrailerSize &&
               frame.next_overflow != frame.page_id;
    }
    return (frame.type == page_type::kLeafBase || frame.type == page_type::kInnerBase) &&
           frame.next_overflow == kInvalidPageId && frame.overflow_bytes == 0 &&
           (frame.type != page_type::kInnerBase || frame.stored_reference_words.empty());
}

Status frame_size(const DurableCapturedFrame &frame, const ChunkRootIdentity &identity, size_t *bytes)
{
    if (!frame.backing || frame.backing->storage != BackingStorage::kChunkPack || !frame.backing->reloadable() ||
        frame.backing->chunk->domain->tree != identity.tree ||
        (frame.type == page_type::kLeafBase && !frame.stored_overflow)) {
        return Status::invalid_argument("Chunk index needs owned same-tree pack references");
    }
    const auto refs = frame.stored_overflow ? frame.stored_overflow->size() : 0;
    if (refs > kChunkIndexReferencesPerFrame || (frame.type != page_type::kLeafBase && refs != 0)) {
        return Status::resource_exhausted("Chunk index stored-reference bound exceeded");
    }
    ChunkIndexFrame geometry{.page_id                = frame.page_id,
                             .type                   = frame.type,
                             .frame_bytes            = frame.frame_bytes,
                             .logical_bytes          = frame.logical_bytes,
                             .next_overflow          = frame.next_overflow,
                             .overflow_bytes         = frame.overflow_bytes,
                             .pack                   = frame.backing->chunk->pack,
                             .stored_reference_words = {}};
    if (!valid_frame(geometry)) {
        return Status::invalid_argument("Chunk index frame geometry is invalid");
    }
    if (frame.stored_overflow) {
        for (const auto &ref : *frame.stored_overflow) {
            if (ref.head == kInvalidPageId || ref.head >= identity.next_page_id || ref.bytes == 0) {
                return Status::invalid_argument("Chunk index overflow reference is invalid");
            }
        }
    }
    *bytes = kChunkIndexRecordBytes + (refs * 16);
    return Status::Ok();
}
} // namespace

Status encode_chunk_index(const ChunkRootIdentity &identity, uint64_t ordinal,
                          std::span<const DurableCapturedFrame *const> frames, ChunkMetadataBytes *out)
try {
    using namespace chunk_codec;
    if (out == nullptr || !out->empty() || identity.tree == 0 || identity.root_generation == 0 || frames.empty() ||
        frames.size() > kChunkIndexFrameLimit) {
        return Status::invalid_argument();
    }
    size_t   size     = kChunkIndexHeaderBytes + 4;
    uint64_t previous = 0;
    for (size_t i = 0; i < frames.size(); ++i) {
        if (frames[i] == nullptr || frames[i]->page_id >= identity.next_page_id ||
            (i != 0 && frames[i]->page_id <= previous)) {
            return Status::invalid_argument("Chunk index page identities must be ordered");
        }
        size_t bytes  = 0;
        auto   status = frame_size(*frames[i], identity, &bytes);
        if (!status.ok()) {
            return status;
        }
        size += bytes;
        previous = frames[i]->page_id;
    }
    out->resize(size);
    auto           bytes = std::span(*out);
    const uint64_t header[]{kIndexMagic, 1, identity.tree, identity.root_generation, ordinal, frames.size()};
    for (size_t i = 0; i < std::size(header); ++i) {
        put(bytes, i, header[i]);
    }
    size_t offset = kChunkIndexHeaderBytes;
    for (const auto *frame : frames) {
        auto           record = bytes.subspan(offset, kChunkIndexRecordBytes);
        const uint64_t words[]{frame->page_id,       static_cast<uint32_t>(frame->type),
                               frame->frame_bytes,   frame->logical_bytes,
                               frame->next_overflow, frame->overflow_bytes};
        for (size_t i = 0; i < std::size(words); ++i) {
            put(record, i, words[i]);
        }
        pack(record.subspan(48), frame->backing->chunk->pack);
        const auto refs = frame->stored_overflow ? frame->stored_overflow->size() : 0;
        put(record, 15, refs);
        offset += kChunkIndexRecordBytes;
        if (frame->stored_overflow) {
            for (const auto &ref : *frame->stored_overflow) {
                put(bytes.subspan(offset, 16), 0, ref.head);
                put(bytes.subspan(offset, 16), 1, ref.bytes);
                offset += 16;
            }
        }
    }
    seal(bytes);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status decode_chunk_index_header(std::span<const uint8_t> bytes, const ChunkRootIdentity &identity, uint64_t ordinal,
                                 uint64_t *count, size_t *offset)
{
    using namespace chunk_codec;
    if (count == nullptr || offset == nullptr) {
        return Status::invalid_argument();
    }
    if (bytes.size() > kChunkIndexWindowBytes || !verified(bytes, kChunkIndexHeaderBytes) ||
        word(bytes, 0) != kIndexMagic || word(bytes, 1) != 1 || word(bytes, 2) != identity.tree ||
        word(bytes, 3) == 0 || word(bytes, 3) > identity.root_generation ||
        (word(bytes, 3) == identity.root_generation && word(bytes, 4) != ordinal) || word(bytes, 5) == 0 ||
        word(bytes, 5) > kChunkIndexFrameLimit) {
        return Status::corruption("Chunk index checksum, format or identity mismatch");
    }
    *count  = word(bytes, 5);
    *offset = kChunkIndexHeaderBytes;
    return Status::Ok();
}

Status decode_chunk_index_frame(std::span<const uint8_t> bytes, size_t *offset, ChunkIndexFrame *out)
{
    using namespace chunk_codec;
    if (offset == nullptr || out == nullptr || *offset < kChunkIndexHeaderBytes || *offset > bytes.size()) {
        return Status::invalid_argument();
    }
    if (bytes.size() - *offset < kChunkIndexRecordBytes + 4) {
        return Status::corruption("Chunk index record is truncated");
    }
    const auto record = bytes.subspan(*offset, kChunkIndexRecordBytes);
    if (word(record, 1) > UINT8_MAX || word(record, 2) > UINT32_MAX || word(record, 3) > UINT32_MAX ||
        word(record, 5) > UINT32_MAX || word(record, 15) > kChunkIndexReferencesPerFrame ||
        !unpack(record.subspan(48), PagePurpose::kBtreePage, &out->pack)) {
        return Status::corruption("Chunk index frame fields exceed their encoded widths");
    }
    const auto reference_bytes = word(record, 15) * 16;
    if (reference_bytes > bytes.size() - *offset - kChunkIndexRecordBytes - 4) {
        return Status::corruption("Chunk index stored references are truncated");
    }
    out->page_id                = word(record, 0);
    out->type                   = static_cast<page_type>(word(record, 1));
    out->frame_bytes            = static_cast<uint32_t>(word(record, 2));
    out->logical_bytes          = static_cast<uint32_t>(word(record, 3));
    out->next_overflow          = word(record, 4);
    out->overflow_bytes         = static_cast<uint32_t>(word(record, 5));
    out->stored_reference_words = bytes.subspan(*offset + kChunkIndexRecordBytes, reference_bytes);
    if (!valid_frame(*out)) {
        return Status::corruption("Chunk index frame geometry is invalid");
    }
    for (size_t i = 0; i < reference_bytes; i += 16) {
        const auto ref = out->stored_reference_words.subspan(i, 16);
        if (word(ref, 0) == kInvalidPageId || word(ref, 1) == 0) {
            return Status::corruption("Chunk index stored reference is invalid");
        }
    }
    *offset += kChunkIndexRecordBytes + reference_bytes;
    return Status::Ok();
}
} // namespace crowdb::tree
