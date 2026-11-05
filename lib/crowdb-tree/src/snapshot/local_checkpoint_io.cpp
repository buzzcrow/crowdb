// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/local_checkpoint_internal.h"

namespace crowdb::tree
{
void LocalCheckpointOperation::State::IoStep::completed(void *context, Status status) noexcept
{
    auto &step = *static_cast<IoStep *>(context);
    if (step.claimed.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    step.completion_status = std::move(status);
    step.credit.reset();
    step.events.fetch_or(1, std::memory_order_acq_rel);
    step.publish();
}

void LocalCheckpointOperation::State::IoStep::returned(Status status) noexcept
{
    submission_status = std::move(status);
    events.fetch_or(2, std::memory_order_acq_rel);
    publish();
}

void LocalCheckpointOperation::State::IoStep::publish() noexcept
{
    const auto observed = events.load(std::memory_order_acquire);
    if ((observed & 2U) == 0 || (submission_status.ok() && (observed & 1U) == 0) ||
        reported.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    owner->event(submission_status.ok() ? completion_status.without_message() : submission_status.without_message());
}

void LocalCheckpointOperation::State::io(bool barrier, uint64_t address, size_t bytes, Stage next, bool anchor)
{
    auto step        = detail::reserved_shared<IoStep>(budget.bytes, budget);
    step->owner      = shared_from_this();
    step->window     = window;
    step->checkpoint = image;
    stage            = next;
    waiting          = true;
    event_ready.store(false, std::memory_order_release);
    if (anchor) {
        registry->touched(image);
        anchor_touched = true;
    }
    AsyncCompletion callback{.context     = step.get(),
                             .complete_fn = &IoStep::completed,
                             .owner       = step,
                             .allocations = budget.backend,
                             .cause       = budget.cause};
    Status          status;
    // A callback may finish the operation on another worker after submit
    // returns; keep the backend itself alive across the entire call.
    auto backend = store;
    try {
        if (barrier) {
            status = backend->submit_fsync(std::move(callback));
        }
        else {
            backend->submit_typed_write(PagePurpose::kPageIndex, address, window->bytes.data(), bytes,
                                        std::move(callback));
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    catch (...) {
        status = Status::io_error();
    }
    step->returned(std::move(status));
}
} // namespace crowdb::tree
