// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/local_recovery_internal.h"

namespace crowdb::tree
{
void LocalRecoveryOperation::State::ReadStep::completed(void *context, Status status) noexcept
{
    auto &step = *static_cast<ReadStep *>(context);
    if (step.claimed.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    step.completion_status = std::move(status);
    step.credit.reset();
    step.events.fetch_or(1, std::memory_order_acq_rel);
    step.publish();
}

void LocalRecoveryOperation::State::ReadStep::returned(Status status) noexcept
{
    submission_status = std::move(status);
    events.fetch_or(2, std::memory_order_acq_rel);
    publish();
}

void LocalRecoveryOperation::State::ReadStep::publish() noexcept
{
    const auto observed = events.load(std::memory_order_acquire);
    if ((observed & 2U) == 0 || (submission_status.ok() && (observed & 1U) == 0) ||
        reported.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    owner->event(submission_status.ok() ? completion_status.without_message() : submission_status.without_message());
}

void LocalRecoveryOperation::State::read(uint64_t address, size_t bytes, Stage next)
{
    if (bytes > window->bytes.size()) {
        finish(Status::resource_exhausted());
        return;
    }
    auto step    = detail::reserved_shared<ReadStep>(budget.bytes, budget);
    step->owner  = shared_from_this();
    step->window = window;
    read_bytes   = bytes;
    stage        = next;
    waiting      = true;
    event_ready.store(false, std::memory_order_release);
    Status status;
    auto   backend = store;
    try {
        backend->submit_read(
            address, window->bytes.data(), bytes,
            {.context = step.get(), .complete_fn = &ReadStep::completed, .owner = step, .allocations = budget.backend});
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    catch (...) {
        status = Status::io_error();
    }
    step->returned(std::move(status));
}

void LocalRecoveryOperation::State::protect(uint64_t address, uint64_t bytes, Stage next)
{
    if (!valid_extent(address, bytes)) {
        finish(Status::corruption());
        return;
    }
    stage   = next;
    waiting = true;
    event_ready.store(false, std::memory_order_release);
    auto status = ledger->recover(address, bytes, budget.bytes, [self = shared_from_this()](ExtentLedgerResult result) {
        self->recovered = std::move(result.extent);
        self->event(std::move(result.status));
    });
    if (!status.ok()) {
        event(std::move(status));
    }
}

bool LocalRecoveryOperation::State::valid_extent(uint64_t address, uint64_t bytes) const noexcept
{
    return address >= domain.region_begin && bytes != 0 && address % domain.iu_size == 0 &&
           bytes % domain.iu_size == 0 && address <= readable_bytes && bytes <= readable_bytes - address;
}
} // namespace crowdb::tree
