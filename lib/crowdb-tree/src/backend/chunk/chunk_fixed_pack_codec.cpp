// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_fixed_pack.h"
#include "chunk_frame.h"
#include "crowdb-common/crc32c.h"

#include <algorithm>

namespace crowdb::tree::detail
{
namespace
{
void little(std::span<uint8_t> out, uint64_t value)
{
    for (size_t index = 0; index < out.size(); ++index) {
        out[index] = static_cast<uint8_t>(value >> (index * 8));
    }
}

void big(std::span<uint8_t> out, uint64_t value)
{
    for (size_t index = 0; index < out.size(); ++index) {
        out[index] = static_cast<uint8_t>(value >> ((out.size() - index - 1) * 8));
    }
}
} // namespace

Status encode_fixed_pack(ChunkId id, PagePurpose purpose, std::span<const uint8_t> input, std::span<uint8_t> framed)
{
    using namespace crowdb::protocol;
    if (input.empty() || input.size() > kMaxChunkPackBytes || !valid_page_purpose(purpose) ||
        chunk_purpose(id) != purpose || chunk_framed_length(input.size()) != framed.size()) {
        return Status::invalid_argument("invalid bounded chunk pack");
    }
    while (!input.empty()) {
        const auto length   = std::min(input.size(), kMaxFrameV2PayloadBytes);
        const auto physical = kFrameV2HeaderBytes + length + kFrameFooterBytes;
        auto       frame    = framed.first(physical);
        little(frame.first(2), static_cast<uint16_t>(chunk_page_frame_magic(purpose)));
        little(frame.subspan(2, 2), kFrameV2HeaderBytes);
        little(frame.subspan(4, 2), length);
        little(frame.subspan(6, 8), 0);
        frame[14] = 0;
        std::copy_n(input.begin(), length, frame.begin() + kFrameV2HeaderBytes);
        const auto footer = kFrameV2HeaderBytes + length;
        little(frame.subspan(footer, 4), crowdb::common::crc32c(frame.data(), footer));
        big(frame.subspan(footer + 4, 8), id.high);
        big(frame.subspan(footer + 12, 8), id.low);
        input  = input.subspan(length);
        framed = framed.subspan(physical);
    }
    return Status::Ok();
}

Status decode_fixed_pack(const ChunkFixedPackRef &ref, std::span<const uint8_t> framed, std::span<uint8_t> output)
{
    using namespace crowdb::protocol;
    if (output.size() != ref.length || chunk_framed_length(ref.length) != framed.size() ||
        chunk_purpose(ref.chunk) != ref.purpose || !valid_page_purpose(ref.purpose)) {
        return Status::corruption("fixed chunk pack identity or geometry mismatch");
    }
    const auto whole = output;
    while (!framed.empty()) {
        ParsedFrame frame;
        auto        error = parse_frame(framed, {.high = ref.chunk.high, .low = ref.chunk.low}, &frame);
        if (error != FrameError::Ok || frame.header.magic != chunk_page_frame_magic(ref.purpose) ||
            frame.payload.empty() || frame.payload.size() > output.size()) {
            return Status::corruption("fixed chunk pack frame validation failed");
        }
        std::copy(frame.payload.begin(), frame.payload.end(), output.begin());
        output = output.subspan(frame.payload.size());
        framed = framed.subspan(frame.physical_length);
    }
    if (!output.empty() || crowdb::common::crc32c(whole.data(), whole.size()) != ref.checksum) {
        return Status::corruption("fixed chunk pack payload checksum mismatch");
    }
    return Status::Ok();
}
} // namespace crowdb::tree::detail
