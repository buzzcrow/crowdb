// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Mapping-table frame-page service.

#include "crowdb-tree/maptable/frame_page.h"

#include "crowdb-common/crc32c.h"

#include <limits>

namespace crowdb::tree
{

namespace
{

// Compute + write the {logical_len, crc32c} trailer. CRC covers [0, body) where
// body = page_bytes - trailer (free space is zeroed at build start).
void stamp_trailer(uint8_t *f, uint32_t page_bytes)
{
    uint32_t body = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);
    frame_put_u32(f, body, page_bytes); // logical_len
    uint32_t crc = crowdb::common::crc32c(f, body);
    frame_put_u32(f, body + 4, crc);
}

} // namespace

void frame_restamp_crc(uint8_t *f, uint32_t page_bytes)
{
    stamp_trailer(f, page_bytes);
}

bool frame_has_lower_fence(const uint8_t *f)
{
    return (f[fh::kFlags] & kFrameHasLowerFence) != 0;
}

bool frame_has_upper_fence(const uint8_t *f)
{
    return (f[fh::kFlags] & kFrameHasUpperFence) != 0;
}

Slice frame_lower_fence(const uint8_t *f)
{
    return {reinterpret_cast<const char *>(f + frame_u32(f, fh::kLowerFenceOff)), frame_u32(f, fh::kLowerFenceLen)};
}

Slice frame_upper_fence(const uint8_t *f)
{
    return {reinterpret_cast<const char *>(f + frame_u32(f, fh::kUpperFenceOff)), frame_u32(f, fh::kUpperFenceLen)};
}

bool frame_fences_are_page_ids(const uint8_t *f)
{
    return (f[fh::kFlags] & kFrameFencePageIds) != 0;
}

uint64_t frame_lower_fence_page_id(const uint8_t *f)
{
    return frame_u64(f, fh::kLowerFenceOff);
}

uint64_t frame_upper_fence_page_id(const uint8_t *f)
{
    return frame_u64(f, fh::kUpperFenceOff);
}

void frame_set_inner_fence_pages(uint8_t *f, uint32_t page_bytes, uint64_t lower_leaf_page_id,
                                 uint64_t upper_leaf_page_id)
{
    f[fh::kFlags] &= static_cast<uint8_t>(~(kFrameHasLowerFence | kFrameHasUpperFence | kFrameFencePageIds));
    if (lower_leaf_page_id != kInvalidPageId && upper_leaf_page_id != kInvalidPageId) {
        f[fh::kFlags] |= kFrameHasLowerFence | kFrameHasUpperFence | kFrameFencePageIds;
        frame_put_u64(f, fh::kLowerFenceOff, lower_leaf_page_id);
        frame_put_u64(f, fh::kUpperFenceOff, upper_leaf_page_id);
    }
    else {
        frame_put_u64(f, fh::kLowerFenceOff, 0);
        frame_put_u64(f, fh::kUpperFenceOff, 0);
    }
    stamp_trailer(f, page_bytes);
}

bool frame_set_fences(uint8_t *f, uint32_t page_bytes, const Slice *lower, const Slice *upper)
{
    if (f == nullptr || page_bytes <= kFrameHeaderSize + kFrameTrailerSize) {
        return false;
    }
    if (frame_page_type(f) == page_type::kOverflowFrame || (lower == nullptr) != (upper == nullptr)) {
        return lower == nullptr && upper == nullptr;
    }
    uint32_t free_lo = frame_u32(f, fh::kFreeLo);
    uint32_t free_hi = frame_u32(f, fh::kFreeHi);
    uint32_t body    = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);
    if (free_hi < free_lo || free_hi > body) {
        return false;
    }
    auto internal_offset = [f, body](Slice key, uint32_t *offset) {
        const uintptr_t begin = reinterpret_cast<uintptr_t>(f);
        const uintptr_t end   = begin + body;
        const uintptr_t data  = reinterpret_cast<uintptr_t>(key.data());
        if (data < begin || data > end || key.size() > end - data) {
            return false;
        }
        *offset = static_cast<uint32_t>(data - begin);
        return true;
    };

    uint32_t   lower_off      = 0;
    uint32_t   upper_off      = 0;
    const bool lower_internal = lower != nullptr && internal_offset(*lower, &lower_off);
    const bool upper_internal = upper != nullptr && internal_offset(*upper, &upper_off);
    const bool reuse_lower    = lower != nullptr && !lower_internal && frame_has_lower_fence(f) &&
                                !frame_fences_are_page_ids(f) && lower->size() <= frame_u32(f, fh::kLowerFenceLen);
    const bool reuse_upper    = upper != nullptr && !upper_internal && frame_has_upper_fence(f) &&
                                !frame_fences_are_page_ids(f) && upper->size() <= frame_u32(f, fh::kUpperFenceLen);
    size_t     copy_bytes     = lower != nullptr && !lower_internal && !reuse_lower ? lower->size() : 0;
    copy_bytes += upper != nullptr && !upper_internal && !reuse_upper ? upper->size() : 0;
    if (copy_bytes > free_hi - free_lo || (lower != nullptr && lower->size() > std::numeric_limits<uint32_t>::max()) ||
        (upper != nullptr && upper->size() > std::numeric_limits<uint32_t>::max())) {
        return false;
    }
    if (lower != nullptr && !lower_internal) {
        if (reuse_lower) {
            lower_off = frame_u32(f, fh::kLowerFenceOff);
        }
        else {
            free_hi -= static_cast<uint32_t>(lower->size());
            lower_off = free_hi;
        }
        if (!lower->empty()) {
            std::memcpy(f + lower_off, lower->data(), lower->size());
        }
    }
    if (upper != nullptr && !upper_internal) {
        if (reuse_upper) {
            upper_off = frame_u32(f, fh::kUpperFenceOff);
        }
        else {
            free_hi -= static_cast<uint32_t>(upper->size());
            upper_off = free_hi;
        }
        if (!upper->empty()) {
            std::memcpy(f + upper_off, upper->data(), upper->size());
        }
    }
    f[fh::kFlags] &= static_cast<uint8_t>(~(kFrameHasLowerFence | kFrameHasUpperFence | kFrameFencePageIds));
    if (lower != nullptr) {
        f[fh::kFlags] |= kFrameHasLowerFence | kFrameHasUpperFence;
        frame_put_u32(f, fh::kLowerFenceOff, lower_off);
        frame_put_u32(f, fh::kLowerFenceLen, static_cast<uint32_t>(lower->size()));
        frame_put_u32(f, fh::kUpperFenceOff, upper_off);
        frame_put_u32(f, fh::kUpperFenceLen, static_cast<uint32_t>(upper->size()));
        frame_put_u32(f, fh::kFreeHi, free_hi);
    }
    else {
        frame_put_u32(f, fh::kLowerFenceOff, 0);
        frame_put_u32(f, fh::kLowerFenceLen, 0);
        frame_put_u32(f, fh::kUpperFenceOff, 0);
        frame_put_u32(f, fh::kUpperFenceLen, 0);
    }
    stamp_trailer(f, page_bytes);
    return true;
}

bool frame_validate(const uint8_t *f, uint32_t page_bytes)
{
    if (page_bytes <= kFrameHeaderSize + kFrameTrailerSize) {
        return false;
    }
    auto     t     = frame_page_type(f);
    uint32_t magic = frame_u32(f, fh::kMagic);
    if (t == page_type::kLeafBase) {
        if (magic != kFrameMagicLeaf) {
            return false;
        }
    }
    else if (t == page_type::kInnerBase) {
        if (magic != kFrameMagicInner) {
            return false;
        }
    }
    else if (t == page_type::kOverflowFrame) {
        if (magic != kFrameMagicOverflow) {
            return false;
        }
    }
    else {
        return false;
    }
    if (f[fh::kFormatVersion] != kFrameVersion) {
        return false;
    }
    uint32_t body = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);
    if (frame_u32(f, body) != page_bytes) {
        return false; // logical_len cross-check
    }
    uint32_t stored = frame_u32(f, body + 4);
    if (crowdb::common::crc32c(f, body) != stored) {
        return false;
    }

    const uint32_t count   = frame_u32(f, fh::kSlotCount);
    const uint32_t free_lo = frame_u32(f, fh::kFreeLo);
    const uint32_t free_hi = frame_u32(f, fh::kFreeHi);
    if (t == page_type::kOverflowFrame) {
        return f[fh::kFlags] == 0 && count <= body - kFrameHeaderSize;
    }
    if (free_lo < kFrameHeaderSize || free_hi < free_lo || free_hi > body) {
        return false;
    }
    if ((f[fh::kFlags] & ~(kFrameHasLowerFence | kFrameHasUpperFence | kFrameFencePageIds)) != 0 ||
        frame_has_lower_fence(f) != frame_has_upper_fence(f)) {
        return false;
    }
    if (frame_fences_are_page_ids(f) &&
        (t != page_type::kInnerBase || !frame_has_lower_fence(f) || frame_lower_fence_page_id(f) == kInvalidPageId ||
         frame_upper_fence_page_id(f) == kInvalidPageId)) {
        return false;
    }
    auto valid_fence = [f, free_hi, body](size_t offset_field, size_t length_field) {
        const uint32_t offset = frame_u32(f, offset_field);
        const uint32_t length = frame_u32(f, length_field);
        return offset >= free_hi && offset <= body && length <= body - offset;
    };
    if (frame_has_lower_fence(f) && !frame_fences_are_page_ids(f) &&
        (!valid_fence(fh::kLowerFenceOff, fh::kLowerFenceLen) || !valid_fence(fh::kUpperFenceOff, fh::kUpperFenceLen) ||
         frame_lower_fence(f).compare(frame_upper_fence(f)) > 0)) {
        return false;
    }

    auto valid_record = [f, free_hi, body](const uint8_t *slot, bool has_cell) {
        const uint32_t off   = frame_u32(slot, 0);
        const uint32_t klen  = frame_u32(slot, 4);
        const uint32_t extra = has_cell ? frame_u32(slot, 8) : 0;
        return off >= free_hi && off <= body && klen <= body - off && extra <= body - off - klen;
    };
    if (t == page_type::kLeafBase) {
        if (count > (body - kFrameHeaderSize) / kLeafSlotSize || kFrameHeaderSize + (count * kLeafSlotSize) > free_lo) {
            return false;
        }
        LeafFrameView view(f, page_bytes);
        for (uint32_t i = 0; i < count; ++i) {
            if (!valid_record(f + kFrameHeaderSize + (i * kLeafSlotSize), true) ||
                (i > 0 && view.key(i - 1).compare(view.key(i)) >= 0)) {
                return false;
            }
        }
        const uint32_t delta_count = view.delta_count();
        if (delta_count > (free_hi - free_lo) / kLeafSlotSize) {
            return false;
        }
        for (uint32_t i = 0; i < delta_count; ++i) {
            if (!valid_record(f + free_lo + (i * kLeafSlotSize), true)) {
                return false;
            }
        }
        if (frame_has_lower_fence(f)) {
            if (view.empty()) {
                return false;
            }
            Slice lower = count == 0 ? view.delta_key(0) : view.key(0);
            Slice upper = count == 0 ? view.delta_key(0) : view.key(count - 1);
            for (uint32_t i = 0; i < delta_count; ++i) {
                Slice key = view.delta_key(i);
                if (key.compare(lower) < 0) {
                    lower = key;
                }
                if (key.compare(upper) > 0) {
                    upper = key;
                }
            }
            if (frame_lower_fence(f).compare(lower) != 0 || frame_upper_fence(f).compare(upper) != 0) {
                return false;
            }
        }
        return true;
    }

    if (count == ~uint32_t{0}) {
        return false;
    }
    const uint32_t children = count + 1;
    if (children > (body - kFrameHeaderSize) / sizeof(uint64_t)) {
        return false;
    }
    const uint32_t slots = kFrameHeaderSize + (children * sizeof(uint64_t));
    if (count > (body - slots) / kInnerSlotSize || slots + (count * kInnerSlotSize) > free_lo) {
        return false;
    }
    InnerFrameView view(f, page_bytes);
    for (uint32_t i = 0; i < count; ++i) {
        if (!valid_record(f + slots + (i * kInnerSlotSize), false) ||
            (i > 0 && view.separator_at(i - 1).compare(view.separator_at(i)) >= 0)) {
            return false;
        }
    }
    return true;
}

bool frame_validate_key_range(const uint8_t *f, uint32_t page_bytes, const KeyRange &range)
{
    const bool valid = frame_validate(f, page_bytes);
    if (!valid || !range.is_bounded()) {
        return valid;
    }
    const page_type type = frame_page_type(f);
    if (type == page_type::kLeafBase) {
        LeafFrameView view(f, page_bytes);
        for (uint32_t i = 0; i < view.count(); ++i) {
            if (!range.contains(view.key(i))) {
                return false;
            }
        }
        for (uint32_t i = 0; i < view.delta_count(); ++i) {
            if (!range.contains(view.delta_key(i))) {
                return false;
            }
        }
    }
    else if (type == page_type::kInnerBase) {
        InnerFrameView view(f, page_bytes);
        for (uint32_t i = 0; i < view.num_separators(); ++i) {
            if (!range.contains(view.separator_at(i))) {
                return false;
            }
        }
    }
    return true;
}

// ── LeafFrameBuilder ──────────────────────────────────────────────

LeafFrameBuilder::LeafFrameBuilder(uint8_t *f, uint32_t page_bytes) : f_(f), page_bytes_(page_bytes)
{
    std::memset(f_, 0, page_bytes_);
    frame_put_u32(f_, fh::kMagic, kFrameMagicLeaf);
    f_[fh::kType]          = static_cast<uint8_t>(page_type::kLeafBase);
    f_[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);
    free_lo_               = static_cast<uint32_t>(kFrameHeaderSize);
    free_hi_               = page_bytes_ - static_cast<uint32_t>(kFrameTrailerSize);
}

bool LeafFrameBuilder::try_append_sorted(Slice key, Slice cell)
{
    size_t reclen = key.size() + cell.size();
    size_t need   = kLeafSlotSize + reclen; // one slot (fwd) + record (bwd)
    size_t avail  = free_hi_ - free_lo_;    // free_hi_ >= free_lo_ invariant
    if (need > avail) {
        return false;
    }
    uint32_t rec_off = free_hi_ - static_cast<uint32_t>(reclen);
    std::memcpy(f_ + rec_off, key.data(), key.size());
    std::memcpy(f_ + rec_off + key.size(), cell.data(), cell.size());
    uint8_t *slot = f_ + free_lo_;
    frame_put_u32(slot, 0, rec_off);
    frame_put_u32(slot, 4, static_cast<uint32_t>(key.size()));
    frame_put_u32(slot, 8, static_cast<uint32_t>(cell.size()));
    free_lo_ += static_cast<uint32_t>(kLeafSlotSize);
    free_hi_ = rec_off;
    ++count_;
    return true;
}

void LeafFrameBuilder::finish(uint64_t self_page_id, uint64_t right_sibling)
{
    frame_put_u32(f_, fh::kSlotCount, count_);
    frame_put_u32(f_, fh::kFreeLo, free_lo_);
    frame_put_u32(f_, fh::kFreeHi, free_hi_);
    frame_put_u64(f_, fh::kSelfpage_id, self_page_id);
    frame_put_u64(f_, fh::kRightSibling, right_sibling);
    if (count_ != 0) {
        LeafFrameView view(f_, page_bytes_);
        Slice         lower = view.key(0);
        Slice         upper = view.key(count_ - 1);
        (void)frame_set_fences(f_, page_bytes_, &lower, &upper);
    }
    else {
        stamp_trailer(f_, page_bytes_);
    }
}

// ── inner_frame_build ───────────────────────────────────────────────

bool inner_frame_build(uint8_t *f, uint32_t page_bytes, uint64_t self_page_id, const std::vector<uint64_t> &children,
                       const std::vector<Slice> &separators)
{
    if (children.size() != separators.size() + 1) {
        return false;
    }
    auto nsep = static_cast<uint32_t>(separators.size());

    std::memset(f, 0, page_bytes);
    frame_put_u32(f, fh::kMagic, kFrameMagicInner);
    f[fh::kType]          = static_cast<uint8_t>(page_type::kInnerBase);
    f[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);

    // Child PID array directly after the header.
    uint32_t child_region =
        static_cast<uint32_t>(kFrameHeaderSize) + (static_cast<uint32_t>(children.size()) * sizeof(uint64_t));
    uint32_t free_lo = child_region; // separator slot dir starts here
    uint32_t free_hi = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);

    // Capacity check: slot dir (nsep slots) + separator record bytes.
    size_t sep_bytes = 0;
    for (const Slice &s : separators) {
        sep_bytes += s.size();
    }
    if ((free_lo + (nsep * kInnerSlotSize)) + sep_bytes > free_hi) {
        return false;
    }

    for (size_t i = 0; i < children.size(); ++i) {
        frame_put_u64(f, kFrameHeaderSize + (i * sizeof(uint64_t)), children[i]);
    }
    for (uint32_t i = 0; i < nsep; ++i) {
        Slice    s       = separators[i];
        uint32_t rec_off = free_hi - static_cast<uint32_t>(s.size());
        std::memcpy(f + rec_off, s.data(), s.size());
        uint8_t *slot = f + (free_lo + (i * kInnerSlotSize));
        frame_put_u32(slot, 0, rec_off);
        frame_put_u32(slot, 4, static_cast<uint32_t>(s.size()));
        free_hi = rec_off;
    }

    frame_put_u32(f, fh::kSlotCount, nsep);
    frame_put_u32(f, fh::kFreeLo, free_lo + (nsep * kInnerSlotSize));
    frame_put_u32(f, fh::kFreeHi, free_hi);
    frame_put_u64(f, fh::kSelfpage_id, self_page_id);
    stamp_trailer(f, page_bytes);
    return true;
}

// ── leaf_frame_append_deltas (PT12) ──────────────────────────────────

bool leaf_frame_append_deltas(const uint8_t *src, uint32_t page_bytes, const std::vector<leaf_entry> &entries,
                              uint8_t *out)
{
    std::memcpy(out, src, page_bytes);
    uint32_t free_lo     = frame_u32(out, fh::kFreeLo); // end of main slot dir
    uint32_t free_hi     = frame_u32(out, fh::kFreeHi);
    uint32_t delta_count = frame_u32(out, fh::kDeltaCount);

    // Total bytes needed: one slot per delta (forward) + record bytes (backward).
    size_t need_slots = entries.size() * kLeafSlotSize;
    size_t need_recs  = 0;
    for (const auto &e : entries) {
        need_recs += e.key.size() + e.cell.size();
    }
    uint32_t slot_end =
        free_lo + ((delta_count + static_cast<uint32_t>(entries.size())) * static_cast<uint32_t>(kLeafSlotSize));
    if (slot_end + need_recs > free_hi) {
        return false; // does not fit -> caller folds
    }
    (void)need_slots;

    uint32_t cur_hi = free_hi;
    for (size_t j = 0; j < entries.size(); ++j) {
        const leaf_entry &e       = entries[j];
        auto              reclen  = static_cast<uint32_t>(e.key.size() + e.cell.size());
        uint32_t          rec_off = cur_hi - reclen;
        std::memcpy(out + rec_off, e.key.data(), e.key.size()); // NOLINT(bugprone-not-null-terminated-result)
        std::memcpy(out + (rec_off + e.key.size()), e.cell.data(),
                    e.cell.size()); // NOLINT(bugprone-not-null-terminated-result)
        uint8_t *slot = out + (free_lo + ((delta_count + static_cast<uint32_t>(j)) * kLeafSlotSize));
        frame_put_u32(slot, 0, rec_off);
        frame_put_u32(slot, 4, static_cast<uint32_t>(e.key.size()));
        frame_put_u32(slot, 8, static_cast<uint32_t>(e.cell.size()));
        cur_hi = rec_off;
    }
    frame_put_u32(out, fh::kFreeHi, cur_hi);
    frame_put_u32(out, fh::kDeltaCount, delta_count + static_cast<uint32_t>(entries.size()));
    LeafFrameView view(out, page_bytes);
    Slice         lower = view.count() == 0 ? view.delta_key(0) : view.key(0);
    Slice         upper = view.count() == 0 ? view.delta_key(0) : view.key(view.count() - 1);
    for (uint32_t index = 0; index < view.delta_count(); ++index) {
        Slice key = view.delta_key(index);
        if (key.compare(lower) < 0) {
            lower = key;
        }
        if (key.compare(upper) > 0) {
            upper = key;
        }
    }
    (void)frame_set_fences(out, page_bytes, &lower, &upper);
    return true;
}

// ── overflow_frame_build ────────────────────────────────────────────

void overflow_frame_build(uint8_t *f, uint32_t page_bytes, uint64_t self_page_id, uint64_t next_page_id,
                          const uint8_t *payload, uint32_t chunk_len)
{
    std::memset(f, 0, page_bytes);
    frame_put_u32(f, fh::kMagic, kFrameMagicOverflow);
    f[fh::kType]          = static_cast<uint8_t>(page_type::kOverflowFrame);
    f[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);
    frame_put_u32(f, fh::kSlotCount, chunk_len);
    frame_put_u64(f, fh::kSelfpage_id, self_page_id);
    frame_put_u64(f, fh::kRightSibling, next_page_id);
    if (chunk_len > 0) {
        std::memcpy(f + kFrameHeaderSize, payload, chunk_len);
    }
    stamp_trailer(f, page_bytes);
}

} // namespace crowdb::tree
