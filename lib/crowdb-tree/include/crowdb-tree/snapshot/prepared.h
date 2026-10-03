// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/mapping_table.h"

#include <cstdint>
#include <set>
#include <vector>

namespace crowdb::tree
{
// One durable blob to write at a fixed offset, computed ahead of time by
// prepare_snapshot_locked() (persist.cpp) so the actual store->write_at()/
// submit_write() call is a pure I/O op with no further encoding logic --
// shared by snapshot()'s synchronous writes and snapshot_async()'s async
// ones.
struct PreparedSnapshotWrite
{
    uint64_t             addr = 0;
    std::vector<uint8_t> blob; // already IU-padded
};

// A page write plus enough identity to safely mark the *live* page durable
// once the write actually lands (see prepare_snapshot_locked's doc comment
// on why this can't happen eagerly at prepare time for the async path).
// `page` is never dereferenced except as an opaque identity check under
// write_mutex_ (mapping_.get_resident(page_id) == page) -- it may have been retired
// and its frame reused by the time the write completes (a concurrent
// consolidate/flush/split replaced this page_id's mapping entry with a
// fresh COW page in the meantime), in which case the identity check simply
// fails and this write's durable-bookkeeping is skipped (harmless: the old
// blob is still correctly on disk and referenced by *this* generation's
// segment image; the fresh page is independently dirty and picked up by
// the next snapshot).
struct PreparedPageWrite
{
    uint64_t             page_id     = 0;
    PageBase            *page        = nullptr; // opaque identity only
    uint64_t             prior_addr  = kNoAddr;
    uint64_t             addr        = 0;
    uint32_t             logical_len = 0; // unpadded; mirrors PageBase::durable_plen
    std::vector<uint8_t> blob;            // already IU-padded
};

// A dirty MappingSegment's fresh image write, plus enough identity to
// safely mark it durable at commit time (mirrors PreparedPageWrite's
// identity-check pattern, extended with `seen_write_seq` -- see
// MappingSegment's doc comment on why a segment needs a seq check, not just
// a pointer identity check: unlike a page, whose whole *pointer* is
// replaced on any change, a segment's pointer stays the same across a
// slot mutation, so identity alone can't detect "written again during the
// prepare-to-commit gap").
struct PreparedSegmentWrite
{
    uint64_t             seg_idx        = 0;
    MappingSegment      *seg            = nullptr; // opaque identity only
    uint64_t             seen_write_seq = 0;
    uint64_t             new_generation = 0;
    uint64_t             addr           = 0;
    uint32_t             logical_len    = 0; // unpadded
    uint32_t             image_crc      = 0; // body-only CRC, matches the directory entry prepare wrote
    std::vector<uint8_t> blob;               // already IU-padded
};

struct PreparedUnloadedRelocation
{
    uint64_t page_id  = 0;
    uint64_t old_word = slot_word::kEmpty;
    uint64_t new_word = slot_word::kEmpty;
};

// A prefetched unloaded page read outside write_mutex_ (R129). The mapping
// word is revalidated under the mutex during prepare; a mismatch discards
// the prefetched blob and the page is skipped for this pass.
struct PrefetchedPage
{
    uint64_t             page_id  = 0;
    uint64_t             old_word = slot_word::kEmpty;
    std::vector<uint8_t> blob; // IU-padded page content read from the store
};

// Output of prepare_snapshot_locked(): every byte this snapshot generation
// needs written, computed synchronously under write_mutex_ (the segment
// scan + delta-fold + page/segment-image/directory encode is CPU/memory-only
// -- see the "Lock scope" note on #11). The caller writes
// `page_writes` and `segment_writes` (any order/concurrency) then
// `directory_write`, then a durability barrier, then `anchor_write` --
// writing the anchor before that barrier would violate the crash-safety
// invariant persist.cpp's header comment documents (a crash mid-snapshot
// must fall back intact to the last *committed* anchor) -- then
// commit_prepared_snapshot() to mark each page/segment durable and publish
// the new version.
struct PreparedSnapshot
{
    std::vector<PreparedPageWrite>          page_writes;
    std::vector<PreparedSegmentWrite>       segment_writes;
    std::vector<PreparedUnloadedRelocation> unloaded_relocations;
    PreparedSnapshotWrite                   directory_write;
    PreparedSnapshotWrite                   anchor_write;
    uint64_t                                last_applied_slot = 0;
    // Diagnostics for the "snapshot committed" log line (matches the
    // pre-refactor synchronous snapshot()'s log fields exactly).
    uint64_t           seq             = 0;
    uint64_t           live_page_count = 0; // live slots across every present segment
    uint64_t           pages_written   = 0;
    uint64_t           segdir_len      = 0;
    std::set<uint32_t> empty_blocks; // block indices with zero live bytes (block compaction)
    // Block compaction stats (R129). blocks_selected is the count of source
    // blocks chosen for relocation this pass; pages_relocated and
    // bytes_relocated count only pages that were actually moved (not clean
    // pages that kept their durable address). blocks_deleted is filled by
    // finalize_prepared_snapshot after the finalizer runs.
    uint64_t blocks_selected = 0;
    uint64_t pages_relocated = 0;
    uint64_t bytes_relocated = 0;
    uint64_t blocks_deleted  = 0;
};

} // namespace crowdb::tree
