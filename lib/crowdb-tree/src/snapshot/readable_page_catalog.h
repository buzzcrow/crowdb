// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_writeback.h"

#include <map>

namespace crowdb::tree
{
struct StoredOverflowReference
{
    uint64_t head  = kInvalidPageId;
    uint64_t bytes = 0;
};

using StoredOverflowReferences = detail::ReservedVector<StoredOverflowReference>;

struct ReadablePageFrame
{
    uint64_t                          page_id        = kInvalidPageId;
    page_type                         type           = page_type::kLeafBase;
    uint32_t                          frame_bytes    = 0;
    uint32_t                          logical_bytes  = 0;
    uint64_t                          next_overflow  = kInvalidPageId;
    uint32_t                          overflow_bytes = 0;
    std::shared_ptr<const BackingRef> backing;
    // Every encoded leaf cell, including overwritten in-frame delta history.
    // An empty owned list proves no stored overflow references; null is unknown.
    std::shared_ptr<const StoredOverflowReferences> stored_overflow = nullptr;
    std::optional<FinalPageSummary>                 summary         = std::nullopt;
};

using DurableCapturedFrame = ReadablePageFrame;

// A fixed readable graph used for validation. Resident frames and protected
// durable blobs both retain independent owners; this catalog promises no
// checkpoint durability.
struct ReadablePageCatalog
{
    static constexpr bool observe_retirement = true;

    bool retire_step() noexcept
    {
        for (size_t i = 0; i < 64 && !frames.empty(); ++i) {
            frames.erase(frames.begin());
        }
        return frames.empty();
    }

    using Entry  = std::pair<const uint64_t, DurableCapturedFrame>;
    using Frames = std::map<uint64_t, DurableCapturedFrame, std::less<>, detail::ReservationAllocator<Entry>>;

    explicit ReadablePageCatalog(const std::shared_ptr<detail::AllocationReservation> &metadata)
        : frames(std::less<>{}, detail::ReservationAllocator<Entry>(metadata))
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
