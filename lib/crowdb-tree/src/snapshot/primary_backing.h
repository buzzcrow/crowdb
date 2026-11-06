// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/captured_write.h"

namespace crowdb::tree
{
// This receipt owns one primary and its exact overflow dependencies, never an
// entire checkpoint. Route and base must match. A higher GC floor remains
// compatible when the exact frame summary proves that no tombstones remain.
struct PrimaryBackingMetadata
{
    DurableCapturedFrame frame;
    PageKeyRange         range;
    uint64_t             coverage      = 0;
    uint64_t             gc_floor      = 0;
    uint64_t             right_sibling = kInvalidPageId;
    // Allows a resident route to be identified without retaining its cache
    // payload.
    std::weak_ptr<const InnerPageState> inner_route;
};

struct PrimaryReuseOutput
{
    explicit PrimaryReuseOutput(const std::shared_ptr<AllocationBudget> &metadata)
        : dependencies(detail::ReservationAllocator<ReusedOverflowDependency>(metadata))
    {
    }

    std::shared_ptr<const PrimaryBackingMetadata>    primary;
    detail::ReservedVector<ReusedOverflowDependency> dependencies;
};

std::shared_ptr<const PrimaryBackingMetadata>
make_primary_backing(const DurableCapturedFrame &frame, const PageState &page, uint64_t coverage, uint64_t gc_floor,
                     const std::shared_ptr<detail::AllocationReservation> &metadata);

std::shared_ptr<const PrimaryBackingMetadata>
make_inner_primary_backing(const DurableCapturedFrame &frame, const PublishedPage &page, uint64_t coverage,
                           uint64_t gc_floor, const std::shared_ptr<detail::AllocationReservation> &metadata);

bool can_reuse_inner_primary(const PublishedPage &page, uint64_t pid, const InnerEncodeFences &fences, uint64_t backend,
                             uint32_t frame_bytes) noexcept;

bool can_reuse_primary(const PublishedPage &page, uint64_t pid, const PageKeyRange &range, uint64_t gc_floor,
                       uint64_t backend, uint32_t frame_bytes) noexcept;
} // namespace crowdb::tree
