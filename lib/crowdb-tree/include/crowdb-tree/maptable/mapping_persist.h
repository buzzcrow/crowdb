// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Durable segment image and directory codecs. Local checkpoints and explicit
// legacy migration use these backend-neutral blobs. Anchor encoding and
// validation are separate snapshot metadata operations.
//
// Segment images hold packed durable words (mapping_slot.h), never resident
// pointers. Version 1 carries local byte descriptors; version 2 additionally
// permits immutable-store page references for legacy migration. Recovery
// validates the words before constructing typed backing descriptors.
//
// A directory records each live segment's generation, image location and CRC.
// The existing on-disk encoding is independent of the mutable mapping test
// model.
#pragma once

#include "crowdb-tree/status.h"

#include <cstdint>
#include <vector>

namespace crowdb::tree
{

// ── Segment image ─────────────────────────────────────────────

struct SegmentImageHeader
{
    uint16_t format_version = 0;
    uint16_t flags          = 0;
    uint32_t seg_idx        = 0;
    uint64_t generation     = 0;
    uint32_t slot_count     = 0;
    uint32_t live_count     = 0;
    // Decode-only: the body CRC decode_segment_image already validated
    // internally, surfaced so a caller holding a DirEntry can additionally
    // cross-check DirEntry::image_crc against the image it actually read
    // (catches a directory pointing at the wrong/stale image address, which
    // a bare "did this blob decode" check can't). Left 0 by
    // encode_segment_image (only decode fills it in).
    uint32_t body_crc = 0;
};

// Encodes `words` (exactly `hdr.slot_count` packed slot words, mapping_slot.h
// encoding) as a self-describing, CRC-protected image. Appends to `*out`
// (does not clear it first) so callers can size/allocate from the result.
// `out_body_crc`, if non-null, receives the body-only CRC (over the packed
// words, not the header) -- callers persisting a segment directory entry
// need exactly this value (DirEntry::image_crc) without re-reading the
// image back from the store.
void encode_segment_image(const SegmentImageHeader &hdr, const std::vector<uint64_t> &words, std::vector<uint8_t> *out,
                          uint32_t *out_body_crc = nullptr);

// Decodes an image produced by encode_segment_image. Validates the header
// CRC, the magic, and the body CRC; `buf`/`len` may be longer than the
// logical image (e.g. IU-padded) -- only the first
// `header + slot_count*8 + 4` bytes are consumed.
Status decode_segment_image(const uint8_t *buf, size_t len, SegmentImageHeader *hdr_out,
                            std::vector<uint64_t> *words_out);

// Exact encoded size for a `slot_count`-word image (header + body + trailer).
// Callers use this to size the allocation before encode_segment_image runs.
[[nodiscard]] size_t segment_image_encoded_size(uint32_t slot_count);

// ── Segment directory ──────────────────────────────────────────

struct DirEntry
{
    uint32_t seg_idx    = 0;
    uint64_t generation = 0;
    uint64_t image_addr = 0;
    uint32_t image_len  = 0; // logical (unpadded) length of the segment image
    uint32_t image_crc  = 0; // the image's own body_crc, duplicated here so a
                             // directory-only scan can sanity-check without
                             // re-reading every image
};

// Encodes the full directory (every live segment). Appends to `*out`.
void encode_segment_directory(const std::vector<DirEntry> &entries, std::vector<uint8_t> *out);

Status decode_segment_directory(const uint8_t *buf, size_t len, std::vector<DirEntry> *entries_out);

} // namespace crowdb::tree
