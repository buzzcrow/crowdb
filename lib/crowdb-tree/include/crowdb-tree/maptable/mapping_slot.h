// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Packed 64-bit mapping slot words. Segment recovery validates durable words
// before constructing typed backing descriptors.
//
// Zero is empty. A nonzero word whose low three bits are zero is an aligned
// resident PageBase pointer, valid only in memory.
//
// A local descriptor has tag 1 in bit 0, an IU count in bits [23:1], and an
// address measured in IUs in bits [63:24].
//
// A Chunk reference has tag 2 in bits [2:0], an IU count in bits [23:3], and a
// reference ordinal in bits [63:24].
//
// Resident pointers are never persisted; disk slots are empty or tagged
// descriptors. The legacy mutable mapping model survives only in tests.
#pragma once

#include "crowdb-tree/maptable/page_types.h" // PageBase

#include <cstdint>

namespace crowdb::tree::slot_word
{

// Layout (see header comment). 40 + 23 + 1 = 64 bits, no overlap.
inline constexpr uint64_t kEmpty       = 0;
inline constexpr uint64_t kUnloadedTag = 1;
inline constexpr uint64_t kPageRefTag  = 2;
inline constexpr uint64_t kTagMask     = 7;

inline constexpr int kIuCountBits  = 23;
inline constexpr int kIuIndexBits  = 40;
inline constexpr int kIuCountShift = 1;
inline constexpr int kIuIndexShift = 24;

inline constexpr uint64_t kMaxIuIndex        = (uint64_t{1} << kIuIndexBits) - 1;
inline constexpr uint32_t kMaxIuCount        = (uint32_t{1} << kIuCountBits) - 1;
inline constexpr int      kPageRefCountBits  = 21;
inline constexpr int      kPageRefCountShift = 3;
inline constexpr uint32_t kMaxPageRefIuCount = (uint32_t{1} << kPageRefCountBits) - 1;

static_assert(kIuIndexShift == kIuCountShift + kIuCountBits, "iu_count/iu_index adjacent");
static_assert(kIuIndexShift + kIuIndexBits == 64, "packed word is exactly 64 bits");
static_assert(kIuIndexShift == kPageRefCountShift + kPageRefCountBits, "page-reference count/ordinal adjacent");

// ── Classification ────────────────────────────────────────────────
[[nodiscard]] constexpr bool is_empty(uint64_t w)
{
    return w == kEmpty;
}

[[nodiscard]] constexpr bool is_unloaded(uint64_t w)
{
    return (w & kUnloadedTag) != 0 || (w & kTagMask) == kPageRefTag;
}

[[nodiscard]] constexpr bool is_resident(uint64_t w)
{
    return w != kEmpty && (w & kTagMask) == 0;
}

[[nodiscard]] constexpr bool is_page_ref(uint64_t w)
{
    return (w & kTagMask) == kPageRefTag;
}

[[nodiscard]] constexpr bool is_byte_location(uint64_t w)
{
    return (w & kUnloadedTag) != 0;
}

// ── Unloaded descriptor ───────────────────────────────────────────
// Precondition: iu_index <= kMaxIuIndex && iu_count <= kMaxIuCount.
[[nodiscard]] constexpr uint64_t pack_unloaded(uint64_t iu_index, uint32_t iu_count)
{
    return (iu_index << kIuIndexShift) | (uint64_t{iu_count} << kIuCountShift) | kUnloadedTag;
}

[[nodiscard]] constexpr uint64_t unloaded_iu_index(uint64_t w)
{
    return w >> kIuIndexShift;
}

[[nodiscard]] constexpr uint32_t unloaded_iu_count(uint64_t w)
{
    return static_cast<uint32_t>((w >> kIuCountShift) & kMaxIuCount);
}

// True when the value fits the unloaded descriptor fields.
[[nodiscard]] constexpr bool fits_unloaded(uint64_t iu_index, uint32_t iu_count)
{
    return iu_index <= kMaxIuIndex && iu_count <= kMaxIuCount;
}

[[nodiscard]] constexpr uint64_t pack_page_ref(uint64_t ordinal, uint32_t iu_count)
{
    return (ordinal << kIuIndexShift) | (uint64_t{iu_count} << kPageRefCountShift) | kPageRefTag;
}

[[nodiscard]] constexpr uint64_t page_ref_ordinal(uint64_t w)
{
    return w >> kIuIndexShift;
}

[[nodiscard]] constexpr uint32_t page_ref_iu_count(uint64_t w)
{
    return static_cast<uint32_t>((w >> kPageRefCountShift) & kMaxPageRefIuCount);
}

[[nodiscard]] constexpr bool fits_page_ref(uint64_t ordinal, uint32_t iu_count)
{
    return ordinal <= kMaxIuIndex && iu_count <= kMaxPageRefIuCount;
}

// ── Resident pointer (in-memory only; never persisted) ────────────
[[nodiscard]] inline uint64_t pack_resident(PageBase *p)
{
    return reinterpret_cast<uint64_t>(p); // 8-byte aligned => low bit clear
}

[[nodiscard]] inline PageBase *resident_ptr(uint64_t w)
{
    return reinterpret_cast<PageBase *>(w); // NOLINT(performance-no-int-to-ptr)
}

} // namespace crowdb::tree::slot_word
