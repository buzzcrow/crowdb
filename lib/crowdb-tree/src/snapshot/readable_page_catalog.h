// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/readable_page_frames.h"

#include <map>

namespace crowdb::tree
{
// A fixed readable graph used for validation. Resident frames and protected
// durable blobs both retain independent owners; this catalog promises no
// checkpoint durability.
struct ReadablePageCatalog
{
    static constexpr bool observe_retirement = true;

    bool retire_step() noexcept
    {
        return frames.retire_step();
    }

    using Entry  = std::pair<const uint64_t, DurableCapturedFrame>;
    using Frames = ReadablePageFrames;

    explicit ReadablePageCatalog(const std::shared_ptr<detail::AllocationReservation> &metadata) : frames(metadata)
    {
    }

    Frames   frames;
    uint64_t root_page_id = kInvalidPageId;
    uint64_t next_page_id = 0;
    uint64_t coverage     = 0;
    uint64_t generation   = 0;
    uint64_t backend_id   = 0;
};

} // namespace crowdb::tree
