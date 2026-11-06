// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/published_byte_export.h"

#include <array>

namespace crowdb::tree
{
struct PublishedByteExport::State
{
    ~State()
    {
        if (metadata) {
            metadata->seal();
        }
    }
    enum class Phase : uint8_t { Header, Records, Trailer, Done };
    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<PublishedSnapshotContext>      snapshot;
    std::shared_ptr<PublishedSnapshotEntries>      entries;
    std::shared_ptr<PublishedNativeCursor>         native;
    snapshot_format                                format = snapshot_format::kPortable;
    SnapshotByteInfo                               info;
    uint64_t                                       count = 0;
    uint64_t                                       root  = kInvalidPageId;
    std::array<uint8_t, 33>                        header{};
    size_t                                         header_size = 0;
    std::array<uint8_t, 17>                        fields{};
    std::array<uint8_t, 4>                         trailer{};
    std::optional<PublishedScanEntry>              entry;
    std::optional<NativeFrameView>                 frame;
    bool                                           source_done  = false;
    Phase                                          phase        = Phase::Header;
    unsigned                                       part         = 0;
    size_t                                         piece_offset = 0;
    uint32_t                                       emitted_crc  = 0;
    std::atomic<bool>                              busy{false};
    Status                                         failure;

    void   encode_header();
    Status preflight(const KeyRange &range);
    Status portable_preflight(const KeyRange &range);
    Status native_preflight();
    Status next_piece(std::span<const uint8_t> *out);
    Status record_piece(std::span<const uint8_t> *out);
    void   consumed_piece();
    Status next(SnapshotChunk *out, bool *done);
};
} // namespace crowdb::tree
