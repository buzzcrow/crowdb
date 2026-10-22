// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_purpose.h"
#include "crowdb-protocol/frame.h"

namespace crowdb::tree::detail
{

inline constexpr uint8_t  kLegacyChunkFrameRevision = 1;
inline constexpr uint8_t  kChunkFrameRevision       = 2;
inline constexpr uint32_t kMaxChunkPackBytes        = 256U * 1024U * 1024U;

[[nodiscard]] inline crowdb::protocol::FrameMagic chunk_page_frame_magic(PagePurpose purpose,
                                                                         uint8_t     revision = kChunkFrameRevision)
{
    if (revision == kLegacyChunkFrameRevision) {
        return purpose == PagePurpose::kPageIndex ? crowdb::protocol::FrameMagic::PageIndexV1
                                                  : crowdb::protocol::FrameMagic::BtreePageV1;
    }
    return purpose == PagePurpose::kPageIndex ? crowdb::protocol::FrameMagic::PageIndexV2
                                              : crowdb::protocol::FrameMagic::BtreePageV2;
}

[[nodiscard]] inline size_t chunk_framed_length(size_t logical, uint8_t revision = kChunkFrameRevision)
{
    if (revision != kLegacyChunkFrameRevision && revision != kChunkFrameRevision) {
        return 0;
    }
    return crowdb::protocol::framed_physical_length(logical, revision == kChunkFrameRevision
                                                                 ? crowdb::protocol::kFrameV2HeaderBytes
                                                                 : crowdb::protocol::kFrameHeaderPrefixBytes);
}

} // namespace crowdb::tree::detail
