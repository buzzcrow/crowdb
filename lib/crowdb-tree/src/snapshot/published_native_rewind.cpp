// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_native_internal.h"

namespace crowdb::tree
{
Status PublishedNativeCursor::State::restart()
{
    const auto &view  = snapshot->view();
    auto        lease = detail::reserved_shared<ReservedOverflowIds>(metadata);
    lease->first      = view->next_page_id;
    lease->count      = layout->new_overflow_frames();
    auto reserve      = [lease, next = view->next_page_id, end = info.next_page_id](uint64_t             count,
                                                                                    ReservedOverflowIds *out) mutable {
        if (count > end - next) {
            return Status::corruption("native encoder exceeds its planned PID interval");
        }
        *out = {.first = next, .count = count, .owner = lease};
        next += count;
        return Status::Ok();
    };
    auto status = CapturedFrameStream::create(view, loader, snapshot->binding()->cpu(), snapshot->lane(), limits,
                                              std::move(reserve), &stream);
    if (status.ok()) {
        emitted   = 0;
        generated = false;
        old       = layout->old_frames().begin();
    }
    return status;
}

Status PublishedNativeCursor::rewind()
{
    if (state_->busy.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable("native cursor already has an active window");
    }

    struct Leave
    {
        std::atomic<bool> &busy;

        ~Leave()
        {
            busy.store(false, std::memory_order_release);
        }
    } leave{state_->busy};

    if (!state_->failure.ok()) {
        return state_->failure;
    }
    if (!state_->generated || state_->old != state_->layout->old_frames().end()) {
        return Status::unavailable("native rewind requires a completed pass");
    }
    // Release CPU-tail metadata from the first pass before spending the same
    // fixed stream reserve again. No additional PID publication is performed.
    state_->snapshot->lane()->drain();
    state_->stream.reset();
    try {
        return state_->restart();
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted("native rewind allocation failed");
    }
}
} // namespace crowdb::tree
