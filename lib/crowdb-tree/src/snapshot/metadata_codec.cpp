// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/metadata_codec.h"

#include "crowdb-common/crc32c.h"
#include "crowdb-tree/maptable/mapping_slot.h"

#include <algorithm>
#include <array>

namespace crowdb::tree::snapshot
{
namespace
{
template <class T> void put(std::span<uint8_t> output, size_t offset, T value)
{
    for (size_t i = 0; i < sizeof(T); ++i) {
        output[offset + i] = static_cast<uint8_t>(value >> (8 * i));
    }
}
} // namespace

Status encode_segment_into(const SegmentImageHeader &header, std::span<const uint64_t> words, std::span<uint8_t> output,
                           uint32_t *body_crc)
{
    if (words.size() != header.slot_count || header.live_count > header.slot_count || body_crc == nullptr ||
        output.size() < segment_image_encoded_size(header.slot_count)) {
        return Status::invalid_argument();
    }
    bool refs = false;
    for (const auto word : words) {
        if (!slot_word::is_empty(word) && !slot_word::is_unloaded(word)) {
            return Status::invalid_argument();
        }
        refs = refs || slot_word::is_page_ref(word);
    }
    put(output, 0, uint32_t{0x534D5443});
    put(output, 4, refs ? uint16_t{2} : uint16_t{1});
    put(output, 6, refs ? uint16_t{1} : uint16_t{0});
    put(output, 8, header.seg_idx);
    put(output, 12, header.generation);
    put(output, 20, header.slot_count);
    put(output, 24, header.live_count);
    put(output, 28, common::crc32c(output.data(), 28));
    for (size_t i = 0; i < words.size(); ++i) {
        put(output, 32 + (i * sizeof(uint64_t)), words[i]);
    }
    *body_crc = common::crc32c(output.data() + 32, words.size_bytes());
    put(output, 32 + words.size_bytes(), *body_crc);
    return Status::Ok();
}

Status encode_anchor_into(const CommitAnchor &anchor, std::span<uint8_t> output)
{
    if (output.size() < kAnchorBytes) {
        return Status::invalid_argument();
    }
    std::ranges::fill(output, uint8_t{0});
    put(output, 0, anchor.magic);
    put(output, 4, anchor.format_version);
    put(output, 8, anchor.snapshot_seq);
    put(output, 16, anchor.root_page_id);
    put(output, 24, anchor.last_applied_slot);
    put(output, 32, anchor.next_page_id);
    put(output, 40, anchor.segment_slots);
    put(output, 44, anchor.segdir_addr);
    put(output, 52, anchor.segdir_len);
    put(output, 56, anchor.segdir_crc);
    put(output, 60, anchor.leaf_count);
    put(output, 68, anchor.inner_count);
    put(output, 76, common::crc32c(output.data(), 76));
    return Status::Ok();
}

uint32_t DirectoryEncoder::logical_bytes() const noexcept
{
    return entries_.size() > (UINT32_MAX - 20) / 32 ? 0 : static_cast<uint32_t>(20 + (entries_.size() * 32));
}

Status DirectoryEncoder::next(std::span<uint8_t> output, size_t *written)
{
    if (written == nullptr || output.empty() || logical_bytes() == 0) {
        return Status::invalid_argument();
    }
    *written              = 0;
    const size_t body_end = 16 + (entries_.size() * 32);
    while (*written < output.size() && position_ < logical_bytes()) {
        std::array<uint8_t, 32> piece{};
        size_t                  length = 0;
        size_t                  offset = 0;
        bool                    body   = false;
        if (position_ < 16) {
            put(std::span(piece), 0, uint32_t{0x44535443});
            put(std::span(piece), 4, uint16_t{1});
            put(std::span(piece), 8, static_cast<uint32_t>(entries_.size()));
            put(std::span(piece), 12, common::crc32c(piece.data(), 12));
            length = 16;
            offset = position_;
        }
        else if (position_ < body_end) {
            const auto &entry = entries_[(position_ - 16) / 32];
            put(std::span(piece), 0, entry.seg_idx);
            put(std::span(piece), 8, entry.generation);
            put(std::span(piece), 16, entry.image_addr);
            put(std::span(piece), 24, entry.image_len);
            put(std::span(piece), 28, entry.image_crc);
            length = 32;
            offset = (position_ - 16) % 32;
            body   = true;
        }
        else {
            put(std::span(piece), 0, body_crc_);
            length = 4;
            offset = position_ - body_end;
        }
        const auto bytes = std::min(length - offset, output.size() - *written);
        std::copy_n(piece.data() + offset, bytes, output.data() + *written);
        if (body) {
            body_crc_ = common::crc32c_update(body_crc_, piece.data() + offset, bytes);
        }
        full_crc_ = common::crc32c_update(full_crc_, piece.data() + offset, bytes);
        position_ += bytes;
        *written += bytes;
    }
    return Status::Ok();
}
} // namespace crowdb::tree::snapshot
