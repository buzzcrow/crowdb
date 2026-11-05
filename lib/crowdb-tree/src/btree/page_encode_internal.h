// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_encode.h"

namespace crowdb::tree::detail
{
struct FinalLeafEntry
{
    Slice       key;
    LogicalCell cell;
    uint64_t    overflow_offset    = 0;
    uint64_t    overflow_frames    = 0;
    uint32_t    encoded_cell_bytes = 0;
};

struct FinalPageEncodeState
{
    explicit FinalPageEncodeState(const PageEncodeLimits &limits)
        : entries(ReservationAllocator<FinalLeafEntry>(limits.metadata)),
          reused(ReservationAllocator<ReusedOverflowDependency>(limits.metadata)),
          limits(limits)
    {
    }

    std::shared_ptr<const PublishedPage>                                                  page;
    std::vector<FinalLeafEntry, ReservationAllocator<FinalLeafEntry>>                     entries;
    std::vector<ReusedOverflowDependency, ReservationAllocator<ReusedOverflowDependency>> reused;
    PageEncodeLimits                                                                      limits;
    InnerEncodeFences                                                                     fences;
    FinalPageSummary                                                                      summary;
    PageEncodeCounters                                                                    counters;
    ReservedOverflowIds                                                                   ids;
    std::optional<LogicalPageCursor>                                                      cursor;
    CapturedEncodeContext                                                                 context;
    uint64_t                                                                              gc_floor     = 0;
    size_t                                                                                visited      = 0;
    uint64_t                                                                              pid          = kInvalidPageId;
    uint64_t                                                                              next_page_id = 0;
    size_t                                                                                overflow_entry = 0;
    uint64_t                                                                              overflow_index = 0;
    bool                                                                                  complete       = false;
    bool                                                                                  prepared       = false;
};

Status prepare_leaf(FinalPageEncodeState &state, uint64_t coverage, uint64_t gc_floor,
                    const CapturedEncodeContext &context, PageEncodePreparation *result);
Status advance_leaf(FinalPageEncodeState &state, PageEncodePreparation *result);
Status prepare_inner(FinalPageEncodeState &state, const CapturedEncodeContext &context, PageEncodePreparation *result);
void   encode_primary(FinalPageEncodeState &state, uint8_t *output);
} // namespace crowdb::tree::detail
