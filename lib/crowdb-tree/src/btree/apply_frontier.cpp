// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "apply_frontier.h"

#include "allocation_bound.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
void publish_max(std::atomic<uint64_t> &target, uint64_t value) noexcept
{
    auto previous = target.load(std::memory_order_acquire);
    while (previous < value &&
           !target.compare_exchange_weak(previous, value, std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
}
} // namespace

ApplyFrontier::ApplyFrontier(uint64_t initial, size_t window,
                             const std::shared_ptr<detail::AllocationReservation> &metadata)
    : completed_(window, detail::ReservationAllocator<std::atomic<uint64_t>>(metadata)),
      contiguous_(initial),
      max_seen_(initial)
{
}

Status ApplyFrontier::create(uint64_t initial, size_t window,
                             const std::shared_ptr<detail::AllocationReservation> &metadata,
                             std::shared_ptr<ApplyFrontier>                       *out)
{
    if (out == nullptr || !allocation_bound(window)) {
        return Status::invalid_argument();
    }
    try {
        *out = detail::reserved_construct<ApplyFrontier>(metadata, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) ApplyFrontier(initial, window, metadata);
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

std::optional<size_t> ApplyFrontier::allocation_bound(size_t window) noexcept
{
    if (window == 0 || window > kMaxWindow) {
        return {};
    }
    return detail::AllocationBound(detail::shared_allocation_bound<ApplyFrontier>())
        .add_product(window, sizeof(std::atomic<uint64_t>))
        .value();
}

Status ApplyFrontier::check_slot(uint64_t slot) const noexcept
{
    const auto frontier = contiguous_.load(std::memory_order_acquire);
    return slot > frontier && slot - frontier > completed_.size() ? Status::resource_exhausted() : Status::Ok();
}

Status ApplyFrontier::complete(uint64_t slot) noexcept
{
    auto status = check_slot(slot);
    if (!status.ok()) {
        return status;
    }
    publish_max(max_seen_, slot);
    if (slot > contiguous_.load(std::memory_order_acquire)) {
        // Each location holds the latest completed slot in its ring residue.
        // A later slot can enter this residue only after the frontier passed
        // its predecessor, so an old duplicate must never clear the location.
        publish_max(completed_[slot % completed_.size()], slot);
    }
    advance();
    return Status::Ok();
}

void ApplyFrontier::force_advance(uint64_t slot) noexcept
{
    publish_max(max_seen_, slot);
    publish_max(contiguous_, slot);
    advance();
}

void ApplyFrontier::reset_quiescent(uint64_t slot) noexcept
{
    for (auto &completed : completed_) {
        completed.store(0, std::memory_order_relaxed);
    }
    max_seen_.store(slot, std::memory_order_release);
    contiguous_.store(slot, std::memory_order_release);
}

void ApplyFrontier::advance() noexcept
{
    auto frontier = contiguous_.load(std::memory_order_acquire);
    // Later producers may keep extending the frontier. Each caller helps at
    // most the window it observed, rather than draining an endless stream.
    const auto end = frontier + std::min<uint64_t>(completed_.size(), UINT64_MAX - frontier);
    while (frontier < end) {
        const auto next = frontier + 1;
        if (completed_[next % completed_.size()].load(std::memory_order_acquire) != next) {
            return;
        }
        // The acquire of each completed cell and this release carry the
        // corresponding batch's source writes to a captured flush frontier.
        if (contiguous_.compare_exchange_weak(frontier, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
            frontier = next;
        }
    }
}
} // namespace crowdb::tree
