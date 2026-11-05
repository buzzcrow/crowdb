// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "publication_lane.h"

#include "published_state.h"

namespace crowdb::tree
{
Status PublicationCoordinator::prepare_lane(size_t                                                capacity,
                                            const std::shared_ptr<detail::AllocationReservation> &metadata,
                                            std::shared_ptr<PublicationLane>                     *out)
{
    if (capacity == 0 || capacity > capacity_ || !metadata || out == nullptr || *out) {
        return Status::invalid_argument("publication progress needs a bounded prepaid window");
    }
    if (closed_.load(std::memory_order_acquire) || lane_prepared_.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable("publication progress window already prepared or closed");
    }
    try {
        *out = detail::reserved_construct<PublicationLane>(metadata, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) PublicationLane(identity_, capacity);
        });
    }
    catch (const std::bad_alloc &) {
        lane_prepared_.store(false, std::memory_order_release);
        return Status::resource_exhausted("publication progress control credit unavailable");
    }
    return Status::Ok();
}

bool PublicationCoordinator::try_submit(const std::shared_ptr<PublicationRequest> &request,
                                        const std::shared_ptr<PublicationLane>    &lane)
{
    if (!lane) {
        return try_submit(request);
    }
    if (!request || lane->publisher_ != identity_ || closed_.load(std::memory_order_acquire) ||
        request->submitted_.exchange(true, std::memory_order_acq_rel)) {
        return false;
    }
    auto queued = lane->queued_.load(std::memory_order_relaxed);
    do {
        if (queued == lane->capacity_) {
            request->submitted_.store(false, std::memory_order_release);
            return false;
        }
    } while (!lane->queued_.compare_exchange_weak(queued, queued + 1, std::memory_order_acq_rel));
    request->lane_ = lane;
    enqueue(request);
    return true;
}
} // namespace crowdb::tree
