// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/local_recovery_internal.h"

#include <cassert>

namespace crowdb::tree
{
void LocalRecoveryOperation::State::wake() noexcept
{
    wake_requested.store(true, std::memory_order_release);
    if (lifecycle.load(std::memory_order_acquire) != 2 || scheduled.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    auto self = weak_from_this().lock();
    assert(self);
    for (;;) {
        queued_owner = self;
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

void LocalRecoveryOperation::State::run(void *context) noexcept
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

void LocalRecoveryOperation::State::event(Status status) noexcept
{
    event_status = std::move(status);
    event_ready.store(true, std::memory_order_release);
    wake();
}

void LocalRecoveryOperation::State::pump()
{
    if (waiting) {
        if (!event_ready.load(std::memory_order_acquire)) {
            return;
        }
        waiting = false;
        if (!dispatch_failure.ok() ||
            (!event_status.ok() && (stage != Stage::AnchorRead || event_status.code() != Code::kNotFound))) {
            finish(dispatch_failure.ok() ? event_status.without_message() : dispatch_failure.without_message());
            return;
        }
    }
    if (stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable());
        return;
    }
    switch (stage) {
    case Stage::NextAnchor:
        next_anchor();
        break;
    case Stage::AnchorRead:
        anchor_read();
        break;
    case Stage::DirectoryProtected:
        image->metadata->extents.push_back(recovered);
        read_directory();
        break;
    case Stage::DirectoryRead:
        directory_read();
        break;
    case Stage::Segment:
        segment();
        break;
    case Stage::SegmentProtected:
        image->metadata->segments.emplace_back(directory[segment_index], recovered);
        read(recovered.address, recovered.length, Stage::SegmentRead);
        break;
    case Stage::SegmentRead:
        segment_read();
        break;
    case Stage::PageExtent:
        page_extent();
        break;
    case Stage::PageProtected:
        page_protected();
        break;
    case Stage::NextFrame:
        next_frame();
        break;
    case Stage::FrameRead:
        frame_read();
        break;
    case Stage::Dependencies:
        dependencies();
        break;
    case Stage::Activated:
        finish(LocalCheckpointRegistry::create(ledger, budget.bytes, anchors, &report->registry));
        break;
    }
}
} // namespace crowdb::tree
