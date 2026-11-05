// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_backing.h"
#include "snapshot/readable_page_catalog.h"

namespace crowdb::tree
{
inline constexpr size_t kChunkIndexFrameLimit         = 64;
inline constexpr size_t kChunkIndexReferencesPerFrame = 4096;
inline constexpr size_t kChunkIndexHeaderBytes        = 48;
inline constexpr size_t kChunkIndexRecordBytes        = 128;
inline constexpr size_t kChunkRootHeaderBytes         = 128;
inline constexpr size_t kChunkRootEntryBytes          = 96;
inline constexpr size_t kChunkIndexWindowBytes =
    kChunkIndexHeaderBytes + 4 +
    (kChunkIndexFrameLimit * (kChunkIndexRecordBytes + (16 * kChunkIndexReferencesPerFrame)));

struct ChunkRootIdentity
{
    uint64_t tree              = 0;
    uint64_t root_generation   = 0;
    uint64_t owner_epoch       = 0;
    uint64_t snapshot_sequence = 0;
    uint64_t page_generation   = 0;
    uint64_t coverage          = 0;
    uint64_t gc_floor          = 0;
    uint64_t root_page_id      = kInvalidPageId;
    uint64_t next_page_id      = 0;
    uint64_t wal_replay_offset = 0;
};

struct ChunkIndexReference
{
    uint64_t                  first_page  = 0;
    uint64_t                  last_page   = 0;
    uint64_t                  frame_count = 0;
    detail::ChunkFixedPackRef pack;
};

struct ChunkRootManifest
{
    explicit ChunkRootManifest(const std::shared_ptr<detail::AllocationReservation> &budget)
        : indexes(detail::ReservationAllocator<ChunkIndexReference>(budget))
    {
    }

    ChunkRootIdentity                           identity;
    uint64_t                                    frame_count        = 0;
    uint64_t                                    stored_references  = 0;
    uint64_t                                    dependency_entries = 0;
    detail::ReservedVector<ChunkIndexReference> indexes;
};

using ChunkMetadataBytes = detail::ReservedVector<uint8_t>;

Status encode_chunk_root(const ChunkRootManifest &root, ChunkMetadataBytes *out);
Status decode_chunk_root(std::span<const uint8_t> bytes, uint64_t tree, uint64_t exact_generation,
                         ChunkRootManifest *out);
Status encode_chunk_index(const ChunkRootIdentity &identity, uint64_t ordinal,
                          std::span<const DurableCapturedFrame *const> frames, ChunkMetadataBytes *out);

// Parsing does no I/O or pin registration. Callers acquire exact pack pins on
// the backend lane before turning these records into readable BackingRefs.
struct ChunkIndexFrame
{
    uint64_t                  page_id        = 0;
    page_type                 type           = page_type::kLeafBase;
    uint32_t                  frame_bytes    = 0;
    uint32_t                  logical_bytes  = 0;
    uint64_t                  next_overflow  = kInvalidPageId;
    uint32_t                  overflow_bytes = 0;
    detail::ChunkFixedPackRef pack;
    std::span<const uint8_t>  stored_reference_words;
};

Status decode_chunk_index_header(std::span<const uint8_t> bytes, const ChunkRootIdentity &identity, uint64_t ordinal,
                                 uint64_t *count, size_t *offset);
Status decode_chunk_index_frame(std::span<const uint8_t> bytes, size_t *offset, ChunkIndexFrame *out);
} // namespace crowdb::tree
