// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/metadata_decode.h"

#include "crowdb-common/crc32c.h"
#include "crowdb-tree/maptable/mapping_slot.h"

#include <algorithm>

namespace crowdb::tree::snapshot
{
namespace
{
template <class T> T get(std::span<const uint8_t> input, size_t offset)
{
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(input[offset + i]) << (8 * i);
    }
    return value;
}

bool valid_word(uint64_t word, uint16_t version)
{
    if (slot_word::is_empty(word)) {
        return true;
    }
    if (slot_word::is_page_ref(word)) {
        return version == 2 && slot_word::page_ref_iu_count(word) != 0;
    }
    return slot_word::is_byte_location(word) && slot_word::unloaded_iu_count(word) != 0;
}
} // namespace

Status decode_segment_into(std::span<const uint8_t> input, SegmentImageHeader *header, std::span<uint64_t> words)
{
    if (header == nullptr) {
        return Status::invalid_argument();
    }
    if (input.size() < 32 || get<uint32_t>(input, 0) != 0x534D5443 ||
        get<uint32_t>(input, 28) != common::crc32c(input.data(), 28)) {
        return Status::corruption();
    }
    const SegmentImageHeader decoded{.format_version = get<uint16_t>(input, 4),
                                     .flags          = get<uint16_t>(input, 6),
                                     .seg_idx        = get<uint32_t>(input, 8),
                                     .generation     = get<uint64_t>(input, 12),
                                     .slot_count     = get<uint32_t>(input, 20),
                                     .live_count     = get<uint32_t>(input, 24)};
    if (((decoded.format_version != 1 || decoded.flags != 0) && (decoded.format_version != 2 || decoded.flags != 1)) ||
        decoded.live_count > decoded.slot_count || input.size() < segment_image_encoded_size(decoded.slot_count)) {
        return Status::corruption();
    }
    if (words.size() < decoded.slot_count) {
        return Status::invalid_argument();
    }
    const size_t body_bytes = static_cast<size_t>(decoded.slot_count) * sizeof(uint64_t);
    const auto   body_crc   = get<uint32_t>(input, 32 + body_bytes);
    if (body_crc != common::crc32c(input.data() + 32, body_bytes)) {
        return Status::corruption();
    }
    uint32_t live = 0;
    for (uint32_t i = 0; i < decoded.slot_count; ++i) {
        const auto word = get<uint64_t>(input, 32 + (static_cast<size_t>(i) * sizeof(uint64_t)));
        if (!valid_word(word, decoded.format_version)) {
            return Status::corruption();
        }
        live += slot_word::is_empty(word) ? 0 : 1;
    }
    if (live != decoded.live_count) {
        return Status::corruption();
    }
    for (uint32_t i = 0; i < decoded.slot_count; ++i) {
        words[i] = get<uint64_t>(input, 32 + (static_cast<size_t>(i) * sizeof(uint64_t)));
    }
    *header          = decoded;
    header->body_crc = body_crc;
    return Status::Ok();
}

Status directory_entry_count(std::span<const uint8_t> header, uint32_t logical_bytes, uint32_t *count)
{
    if (count == nullptr) {
        return Status::invalid_argument();
    }
    if (header.size() < 16 || get<uint32_t>(header, 0) != 0x44535443 || get<uint16_t>(header, 4) != 1 ||
        get<uint16_t>(header, 6) != 0 || get<uint32_t>(header, 12) != common::crc32c(header.data(), 12)) {
        return Status::corruption();
    }
    const auto entries = get<uint32_t>(header, 8);
    if (20 + (uint64_t{entries} * 32) != logical_bytes) {
        return Status::corruption();
    }
    *count = entries;
    return Status::Ok();
}

DirectoryDecoder::DirectoryDecoder(std::span<DirEntry> entries, uint32_t logical_bytes)
    : entries_(entries),
      logical_bytes_(logical_bytes)
{
    if (logical_bytes < 20 || (logical_bytes - 20) % 32 != 0) {
        error_ = Code::kCorruption;
    }
    else if (entries.size() < (logical_bytes - 20) / 32) {
        error_ = Code::kInvalidArgument;
    }
}

Status DirectoryDecoder::finish() const
{
    if (error_ == Code::kInvalidArgument) {
        return Status::invalid_argument();
    }
    return complete() ? Status::Ok() : Status::corruption();
}

Status DirectoryDecoder::finish_piece(size_t length)
{
    if (length == 16) {
        uint32_t count = 0;
        return directory_entry_count(piece_, logical_bytes_, &count);
    }
    if (length == 4) {
        return get<uint32_t>(piece_, 0) == body_crc_ ? Status::Ok() : Status::corruption();
    }
    if (get<uint32_t>(piece_, 4) != 0) {
        return Status::corruption();
    }
    const size_t index = ((position_ - 16) / 32) - 1;
    entries_[index]    = {.seg_idx    = get<uint32_t>(piece_, 0),
                          .generation = get<uint64_t>(piece_, 8),
                          .image_addr = get<uint64_t>(piece_, 16),
                          .image_len  = get<uint32_t>(piece_, 24),
                          .image_crc  = get<uint32_t>(piece_, 28)};
    return Status::Ok();
}

Status DirectoryDecoder::next(std::span<const uint8_t> input)
{
    if (error_ != Code::kOk) {
        return finish();
    }
    if (input.size() > logical_bytes_ - position_) {
        error_ = Code::kCorruption;
        return finish();
    }
    const uint32_t body_end = logical_bytes_ - 4;
    while (!input.empty()) {
        const bool header = position_ < 16;
        const bool body   = !header && position_ < body_end;
        size_t     length = 0;
        size_t     offset = 0;
        if (header) {
            length = 16;
            offset = position_;
        }
        else if (body) {
            length = 32;
            offset = (position_ - 16) % 32;
        }
        else {
            length = 4;
            offset = position_ - body_end;
        }
        const size_t bytes = std::min(length - offset, input.size());
        std::copy_n(input.data(), bytes, piece_.data() + offset);
        full_crc_ = common::crc32c_update(full_crc_, input.data(), bytes);
        if (body) {
            body_crc_ = common::crc32c_update(body_crc_, input.data(), bytes);
        }
        position_ += static_cast<uint32_t>(bytes);
        input = input.subspan(bytes);
        if (offset + bytes == length) {
            const auto status = finish_piece(length);
            if (!status.ok()) {
                error_ = status.code();
                return status;
            }
        }
    }
    return Status::Ok();
}
} // namespace crowdb::tree::snapshot
