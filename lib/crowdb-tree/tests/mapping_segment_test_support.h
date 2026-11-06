// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Mutable segment model retained only for low-level legacy mapping tests.
// Sequence checks model a write that races a prepared image commit.
#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>

namespace crowdb::tree
{

// Sentinel image_addr meaning "this segment has never been persisted".
inline constexpr uint64_t kNoSegmentImageAddr = std::numeric_limits<uint64_t>::max();

struct TestMappingSegment
{
    explicit TestMappingSegment(uint32_t slot_count)
        : slots(std::make_unique<std::atomic<uint64_t>[]>(slot_count)),
          slot_count(slot_count)
    {
        for (uint32_t i = 0; i < slot_count; ++i) {
            slots[i].store(0, std::memory_order_relaxed); // 0 == empty (see mapping_slot.h)
        }
    }

    TestMappingSegment(const TestMappingSegment &)            = delete;
    TestMappingSegment &operator=(const TestMappingSegment &) = delete;

    // True if any slot has changed since the last successful persist.
    [[nodiscard]] bool is_dirty() const
    {
        return write_seq.load(std::memory_order_relaxed) != persisted_seq.load(std::memory_order_relaxed);
    }

    std::unique_ptr<std::atomic<uint64_t>[]> slots; // packed slot words (mapping_slot.h)
    uint32_t                                 slot_count;
    std::atomic<uint32_t>                    live_count{0}; // non-empty slots; 0 => recyclable
    std::atomic<uint64_t>                    generation{0}; // bumped when persisted (image version)

    // Bumped by the writer on every slot mutation (store/store_word/clear).
    std::atomic<uint64_t> write_seq{0};
    // Snapshot of write_seq as of the last successfully *committed* image
    // persist. is_dirty() == (write_seq != persisted_seq).
    std::atomic<uint64_t> persisted_seq{0};

    // Durable location of the image matching `persisted_seq`/`generation`.
    // Writer-only bookkeeping in the test model.
    uint64_t image_addr = kNoSegmentImageAddr;
    uint32_t image_len  = 0;
    uint32_t image_crc  = 0;
};

} // namespace crowdb::tree
