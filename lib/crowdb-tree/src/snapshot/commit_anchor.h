// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once
#include "crowdb-tree/backend/block_page_store.h"
#include "crowdb-tree/backend/page_store.h"

#include <vector>

namespace crowdb::tree::snapshot
{
constexpr uint32_t kAnchorMagic   = 0x41435443; // 'CTCA' little-endian
constexpr uint32_t kFormatVersion = 3;          // clean-break format (v3: +leaf/inner_count)
constexpr uint64_t kAnchorBytes   = 4096;

// Per-store anchor slot size and the byte offset where the page/segment
// region begins (two A/B anchor slots precede it).
inline uint64_t superblock_slot_bytes(uint32_t iu)
{
    return round_up_to_iu(kAnchorBytes, iu);
}

inline uint64_t region_base_for(uint32_t iu)
{
    return superblock_slot_bytes(iu) * 2;
}

// The A/B commit point ties one snapshot sequence to its segment directory.
struct CommitAnchor
{
    uint32_t magic             = 0;
    uint32_t format_version    = 0;
    uint64_t snapshot_seq      = 0;
    uint64_t root_page_id      = 0;
    uint64_t last_applied_slot = 0;
    uint64_t next_page_id      = 0;
    uint32_t segment_slots     = 0; // MappingTable::kSegmentSize at persist time (format guard)
    uint64_t segdir_addr       = 0;
    uint32_t segdir_len        = 0;
    uint32_t segdir_crc        = 0;
    uint64_t leaf_count        = 0; // live leaf pages (O(1) gauge, restored on open)
    uint64_t inner_count       = 0; // live inner pages (O(1) gauge, restored on open)
};

void   encode_anchor(const CommitAnchor &anchor, uint64_t slot_bytes, std::vector<uint8_t> *out);
Status read_valid_anchors(const PageStore &store, uint32_t iu, std::vector<CommitAnchor> *out);
} // namespace crowdb::tree::snapshot
