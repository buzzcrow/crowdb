// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/local_checkpoint_internal.h"

#include <cassert>

namespace crowdb::tree
{
void LocalCheckpointOperation::State::wake() noexcept
{
    wake_requested.store(true, std::memory_order_release);
    if (lifecycle.load(std::memory_order_acquire) != 2 || scheduled.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    auto self = weak_from_this().lock();
    assert(self);
    for (;;) {
        queued_owner = self;
        if (budget.cpu_progress) {
            budget.cpu_progress->request(cpu_node, self);
            return;
        }
        Status status;
        try {
            status = runtime->try_submit(lane, &State::run, this);
        }
        catch (...) {
            status = Status::resource_exhausted();
        }
        if (status.ok()) {
            return;
        }
        queued_owner.reset();
        dispatch_failure = std::move(status);
        if (!waiting || event_ready.load(std::memory_order_acquire)) {
            finish(dispatch_failure.without_message());
            scheduled.exchange(false, std::memory_order_acq_rel);
            return;
        }
        scheduled.exchange(false, std::memory_order_acq_rel);
        if (!event_ready.load(std::memory_order_acquire) || scheduled.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
    }
}

void LocalCheckpointOperation::State::run(void *context) noexcept
{
    auto self = std::move(static_cast<State *>(context)->queued_owner);
    self->wake_requested.exchange(false, std::memory_order_acq_rel);
    try {
        self->pump();
    }
    catch (const std::bad_alloc &) {
        self->finish(Status::resource_exhausted());
    }
    catch (...) {
        self->finish(Status::internal_error());
    }
    self->scheduled.exchange(false, std::memory_order_acq_rel);
    if (self->wake_requested.load(std::memory_order_acquire) && self->lifecycle.load(std::memory_order_acquire) == 2) {
        self->wake();
    }
}

void LocalCheckpointOperation::State::event(Status status) noexcept
{
    event_status = std::move(status);
    event_ready.store(true, std::memory_order_release);
    wake();
}

void LocalCheckpointOperation::State::pump()
{
    if (waiting) {
        if (!event_ready.load(std::memory_order_acquire)) {
            return;
        }
        waiting = false;
        if (!dispatch_failure.ok() || !event_status.ok()) {
            finish(dispatch_failure.ok() ? event_status.without_message() : dispatch_failure.without_message());
            return;
        }
    }
    if (!anchor_touched && stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable());
        return;
    }
    switch (stage) {
    case Stage::Begin:
        begin();
        break;
    case Stage::Segment:
        segment();
        break;
    case Stage::SegmentReserved:
        segment_entry.image_addr = reserved.address;
        image->metadata->segments.emplace_back(segment_entry, reserved);
        directory.push_back(segment_entry);
        io(false, reserved.address, segment_bytes, Stage::SegmentWritten);
        break;
    case Stage::SegmentWritten:
        stage = Stage::Segment;
        wake();
        break;
    case Stage::DirectoryReserved:
        image->metadata->extents.push_back(reserved);
        directory_address         = reserved.address;
        image->anchor.segdir_addr = reserved.address;
        directory_window();
        break;
    case Stage::DirectoryWrite:
        directory_window();
        break;
    case Stage::DirectoryWritten:
        if (!encoder) {
            finish(Status::internal_error());
            break;
        }
        if (encoder->complete()) {
            image->anchor.segdir_crc = encoder->crc();
            io(true, 0, 0, Stage::BeforeAnchor);
        }
        else {
            stage = Stage::DirectoryWrite;
            wake();
        }
        break;
    case Stage::BeforeAnchor:
        anchor();
        break;
    case Stage::AnchorWritten:
        io(true, 0, 0, Stage::Commit);
        break;
    case Stage::Commit:
        finish(Status::Ok());
        break;
    }
}
} // namespace crowdb::tree
