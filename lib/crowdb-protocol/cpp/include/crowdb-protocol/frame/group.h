// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-protocol/frame/base.h"
#include "crowdb-protocol/frame/encoding.h"
#include "crowdb-protocol/frame/parsing.h"

#include <limits>

namespace crowdb::protocol
{

struct ParsedAppendGroup
{
    uint64_t logical_length  = 0;
    uint64_t physical_length = 0;
    uint32_t frame_count     = 0;
};

/// Constant-sized validation state for bounded readers and tail scanners.
class AppendGroupValidator
{
  public:
    [[nodiscard]] FrameError start(uint64_t offset, const ParsedFrame &head)
    {
        if (head.header.magic != FrameMagic::StreamV2 || head.group.role != FrameRole::Head || head.group.value == 0 ||
            head.group.count == 0 ||
            head.group.count != (head.group.value / kMaxFrameGroupPayloadBytes) +
                                    (head.group.value % kMaxFrameGroupPayloadBytes != 0 ? 1 : 0)) {
            return FrameError::InvalidGroup;
        }
        const size_t physical = framed_physical_length(static_cast<size_t>(head.group.value), kFrameGroupHeaderBytes);
        if (head.group.value > std::numeric_limits<size_t>::max() || physical == 0 ||
            physical > std::numeric_limits<uint64_t>::max() - offset) {
            return FrameError::LengthOverflow;
        }
        head_offset_ = offset;
        next_offset_ = offset;
        length_      = head.group.value;
        count_       = head.group.count;
        observed_    = 0;
        write_time_  = head.header.write_time_ms;
        chunk_       = head.chunk_id;
        return push(offset, head);
    }

    [[nodiscard]] FrameError push(uint64_t offset, const ParsedFrame &frame)
    {
        if (observed_ >= count_ || offset != next_offset_ || frame.header.magic != FrameMagic::StreamV2 ||
            frame.chunk_id != chunk_ || frame.header.write_time_ms != write_time_ ||
            frame.header.payload_offset != kFrameGroupHeaderBytes ||
            frame.physical_length != kFrameGroupHeaderBytes + frame.payload.size() + kFrameFooterBytes) {
            return FrameError::InvalidGroup;
        }
        const FrameRole role      = observed_ == 0 ? FrameRole::Head : FrameRole::Follower;
        const uint64_t  value     = observed_ == 0 ? length_ : head_offset_;
        const uint32_t  number    = observed_ == 0 ? count_ : observed_;
        const uint64_t  remaining = length_ - (static_cast<uint64_t>(observed_) * kMaxFrameGroupPayloadBytes);
        if (frame.group.role != role || frame.group.value != value || frame.group.count != number ||
            frame.payload.size() != std::min<uint64_t>(remaining, kMaxFrameGroupPayloadBytes)) {
            return FrameError::InvalidGroup;
        }
        if (frame.physical_length > std::numeric_limits<uint64_t>::max() - next_offset_) {
            return FrameError::LengthOverflow;
        }
        next_offset_ += frame.physical_length;
        ++observed_;
        return FrameError::Ok;
    }

    [[nodiscard]] bool complete() const
    {
        return count_ != 0 && observed_ == count_;
    }

    [[nodiscard]] FrameError finish(ParsedAppendGroup *out) const
    {
        if (!complete()) {
            return FrameError::InvalidGroup;
        }
        if (out != nullptr) {
            *out = {.logical_length = length_, .physical_length = next_offset_ - head_offset_, .frame_count = count_};
        }
        return FrameError::Ok;
    }

  private:
    uint64_t     head_offset_ = 0;
    uint64_t     next_offset_ = 0;
    uint64_t     length_      = 0;
    uint64_t     write_time_  = 0;
    uint32_t     count_       = 0;
    uint32_t     observed_    = 0;
    FrameChunkId chunk_;
};

[[nodiscard]] inline FrameError encode_group_frame(FrameChunkId chunk, std::span<const uint8_t> payload,
                                                   uint64_t write_time, FrameGroupFields group,
                                                   std::vector<uint8_t> *out)
{
    if (out == nullptr || payload.empty() || payload.size() > kMaxFrameGroupPayloadBytes ||
        (group.role != FrameRole::Head && group.role != FrameRole::Follower) || group.count == 0 ||
        (group.role == FrameRole::Head && group.value == 0)) {
        return FrameError::InvalidGroup;
    }
    out->clear();
    out->reserve(kFrameGroupHeaderBytes + payload.size() + kFrameFooterBytes);
    append_u16_le(out, static_cast<uint16_t>(FrameMagic::StreamV2));
    append_u16_le(out, kFrameGroupHeaderBytes);
    append_u16_le(out, static_cast<uint16_t>(payload.size()));
    append_u64_le(out, write_time);
    out->push_back(static_cast<uint8_t>(group.role));
    append_u64_le(out, group.value);
    append_u32_le(out, group.count);
    out->insert(out->end(), payload.begin(), payload.end());
    const uint32_t crc = crowdb::common::crc32c(out->data(), out->size());
    append_u32_le(out, crc);
    append_u64_be(out, chunk.high);
    append_u64_be(out, chunk.low);
    return FrameError::Ok;
}

[[nodiscard]] inline FrameError encode_append_group(FrameChunkId chunk, uint64_t head_offset,
                                                    std::span<const uint8_t> payload, uint64_t write_time,
                                                    std::vector<uint8_t> *out)
{
    const size_t physical = framed_physical_length(payload.size(), kFrameGroupHeaderBytes);
    const size_t count =
        (payload.size() / kMaxFrameGroupPayloadBytes) + (payload.size() % kMaxFrameGroupPayloadBytes != 0 ? 1 : 0);
    if (out == nullptr || payload.empty() || physical == 0 || count > std::numeric_limits<uint32_t>::max() ||
        physical > std::numeric_limits<uint64_t>::max() - head_offset) {
        return FrameError::InvalidGroup;
    }
    out->clear();
    out->reserve(physical);
    size_t offset = 0;
    for (size_t ordinal = 0; ordinal < count; ++ordinal) {
        const size_t           length = std::min(kMaxFrameGroupPayloadBytes, payload.size() - offset);
        const FrameGroupFields group{.role  = ordinal == 0 ? FrameRole::Head : FrameRole::Follower,
                                     .value = ordinal == 0 ? payload.size() : head_offset,
                                     .count = static_cast<uint32_t>(ordinal == 0 ? count : ordinal)};
        std::vector<uint8_t>   frame;
        const FrameError error = encode_group_frame(chunk, payload.subspan(offset, length), write_time, group, &frame);
        if (error != FrameError::Ok) {
            return error;
        }
        out->insert(out->end(), frame.begin(), frame.end());
        offset += length;
    }
    return FrameError::Ok;
}

[[nodiscard]] inline FrameError parse_append_group(std::span<const uint8_t> bytes, FrameChunkId chunk,
                                                   uint64_t head_offset, ParsedAppendGroup *out)
{
    ParsedFrame head;
    FrameError  error = parse_frame(bytes, chunk, &head);
    if (error != FrameError::Ok) {
        return error;
    }
    AppendGroupValidator validator;
    error = validator.start(head_offset, head);
    if (error != FrameError::Ok) {
        return error;
    }
    size_t cursor = head.physical_length;
    while (!validator.complete()) {
        ParsedFrame frame;
        error = parse_frame(bytes.subspan(cursor), chunk, &frame);
        if (error != FrameError::Ok) {
            return error;
        }
        if (cursor > std::numeric_limits<uint64_t>::max() - head_offset) {
            return FrameError::LengthOverflow;
        }
        error = validator.push(head_offset + cursor, frame);
        if (error != FrameError::Ok) {
            return error;
        }
        cursor += frame.physical_length;
    }
    return validator.finish(out);
}

} // namespace crowdb::protocol
