// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Legacy mutable mapping model for allocation, packed slots, segment recycling
// and prepare/commit identity regressions. Production uses immutable MappingView.
#pragma once

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/maptable/mapping_geometry.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "crowdb-tree/maptable/page.h"
#include "mapping_segment_test_support.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace crowdb::tree
{

using crowdb::common::metrics::Counter;

// Legacy mutable mapping model retained only for low-level regression tests.
class TestMappingTable
{
  public:
    static constexpr uint64_t kSegmentSize = kMappingSegmentSize;
    static constexpr uint64_t kMaxSegments = kMaxMappingSegments;

    explicit TestMappingTable(bool enabled = true);
    ~TestMappingTable();

    TestMappingTable(const TestMappingTable &)            = delete;
    TestMappingTable &operator=(const TestMappingTable &) = delete;

    // Both owners must exclude operations before exchanging generations.
    void swap_quiescent(TestMappingTable &other) noexcept;

    // Reader: lock-free atomic load of the raw packed slot word.
    // Use slot_word::is_empty / is_resident / is_unloaded to classify.
    [[nodiscard]] uint64_t get_word(uint64_t page_id) const;

    // Convenience: returns the resident PageBase* if the slot is resident,
    // nullptr for empty or unloaded slots.
    [[nodiscard]] PageBase *get_resident(uint64_t page_id) const;

    // Writer: store a resident page pointer (packs it into a slot word).
    void store(uint64_t page_id, PageBase *page);

    // Writer: store a pre-packed slot word (e.g. an unloaded descriptor from
    // slot_word::pack_unloaded, or slot_word::kEmpty to clear).
    void store_word(uint64_t page_id, uint64_t word);

    // Install an unloaded (on-disk, not-resident) descriptor for `page_id`
    // (recovery / eviction). Converts raw (addr, plen) to a packed word using
    // the store's indivisible unit size `iu`.
    void store_unloaded(uint64_t page_id, uint64_t addr, uint32_t plen, uint32_t iu);

    // Clear a slot to empty (slot_word::kEmpty).
    void clear(uint64_t page_id);

    // Wire the epoch manager used to safely reclaim segments that recycle to
    // empty. Call once,
    // before any concurrent readers can observe the table. If never called,
    // recycled segments are `delete`d directly on the writer thread -- fine
    // for a standalone table with no concurrent readers (e.g. unit tests),
    // unsafe otherwise.
    void set_epoch_manager(EpochManager *epoch)
    {
        epoch_ = epoch;
    }

    // Wire optional metrics handle for page allocation counting.
    void set_alloc_counter(Counter *c)
    {
        alloc_counter_ = c;
    }

    // Allocate a fresh monotonic PID; stale readers must never see a reused identity.
    uint64_t allocate_page_id();

    // Number of segments currently allocated (diagnostics).
    [[nodiscard]] size_t segments_allocated() const;

    // Recovery: resume fresh PID allocation past the highest persisted PID.
    void                   set_next_page_id(uint64_t next);
    [[nodiscard]] uint64_t next_page_id() const;

    // Inspect the test model's current segment, or nullptr after recycling.
    [[nodiscard]] TestMappingSegment *segment_at(uint64_t seg_idx) const;

    // Model a prepared segment image commit. Changed identity or write sequence
    // rejects the stale image without clearing newer dirty state.
    bool commit_segment_persist(uint64_t seg_idx, TestMappingSegment *expected, uint64_t seen_write_seq,
                                uint64_t new_generation, uint64_t new_image_addr, uint32_t new_image_len,
                                uint32_t new_image_crc);

    // Recovery: install a segment's full slot-word array plus its
    // bookkeeping directly (no live_count/write_seq transition logic --
    // recovery has no concurrent readers yet). `words.size()` must equal
    // kSegmentSize, matching the durable segment geometry.
    void install_recovered_segment(uint64_t seg_idx, uint64_t generation, uint32_t live_count,
                                   const std::vector<uint64_t> &words, uint64_t image_addr, uint32_t image_len,
                                   uint32_t image_crc);

  private:
    TestMappingSegment *ensure_segment(uint64_t seg_idx);

    // Called by the writer when a slot's last live entry in `seg` goes empty
    // CAS the top-level slot to nullptr -- final and safe
    // because PIDs are monotonic, so a freed segment's PID range is dead
    // forever and no allocation will ever revisit `seg_idx` -- then hands the
    // segment to the epoch manager (if wired) so any in-flight lock-free
    // reader that already loaded `seg` keeps a valid pointer until its guard
    // drains.
    void recycle_segment_if_empty(uint64_t seg_idx, TestMappingSegment *seg);

    std::vector<std::atomic<TestMappingSegment *>> segments_; // fixed-size top-level array

    mutable std::mutex alloc_mu_;
    uint64_t           next_page_id_ = 0;

    EpochManager *epoch_         = nullptr; // set via set_epoch_manager; not owned
    Counter      *alloc_counter_ = nullptr; // set via set_alloc_counter
};

} // namespace crowdb::tree
