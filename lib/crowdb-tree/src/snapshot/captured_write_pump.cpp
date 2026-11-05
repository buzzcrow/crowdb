// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write_internal.h"
#include "snapshot/primary_backing.h"
#include "snapshot/stored_overflow.h"

#include <cassert>

namespace crowdb::tree
{
void CapturedWriteOperation::State::wake() noexcept
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
        if (!waiting || child_ready.load(std::memory_order_acquire)) {
            finish(dispatch_failure.without_message());
            scheduled.exchange(false, std::memory_order_acq_rel);
            return;
        }
        // Keep the receiver/operation until the accepted child drains. A
        // closed runtime cannot justify freeing a still-active write window.
        cancel_child();
        scheduled.exchange(false, std::memory_order_acq_rel);
        if (!child_ready.load(std::memory_order_acquire) || scheduled.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
    }
}

void CapturedWriteOperation::State::run(void *context) noexcept
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

void CapturedWriteOperation::State::pump()
{
    if (waiting) {
        if (stopped.load(std::memory_order_acquire)) {
            cancel_child();
        }
        if (!child_ready.load(std::memory_order_acquire)) {
            return;
        }
        waiting = false;
        attempt.reset();
        if (!dispatch_failure.ok()) {
            finish(std::move(dispatch_failure));
            return;
        }
        if (!child_status.ok()) {
            finish(std::move(child_status));
            return;
        }
    }
    if (stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable());
        return;
    }
    switch (stage) {
    case Stage::Next:
        begin_child(stream->sender(), Stage::Frame);
        break;
    case Stage::Frame:
        received_frame();
        break;
    case Stage::Dependencies:
        reuse_step();
        break;
    case Stage::Write:
        received_write();
        break;
    case Stage::ResidentWrite:
        received_resident_write();
        break;
    case Stage::RelocateRead:
        received_relocated_read();
        break;
    }
}

void CapturedWriteOperation::State::received_frame()
{
    const auto &result = stream->result();
    if (!result.frame) {
        finish(result.complete ? Status::Ok() : Status::corruption());
        return;
    }
    frame      = result.frame;
    last_frame = result.complete;
    descriptor = {
        .page_id = frame->info.page_id, .type = frame->info.type, .frame_bytes = frame->info.bytes, .backing = {}};
    descriptor.summary = frame->summary;
    if (!dependencies) {
        dependencies     = detail::reserved_shared<Dependencies>(budget.bytes, budget.bytes);
        overflow_backing = detail::reserved_shared<OverflowBackingSet>(budget.bytes, budget.bytes);
    }
    if (frame->reuse) {
        descriptor = frame->reuse->primary->frame;
    }
    else if (descriptor.type == page_type::kOverflowFrame) {
        OverflowFrameView overflow(frame->buffer->bytes.data(), descriptor.frame_bytes);
        descriptor.next_overflow  = overflow.next_page_id();
        descriptor.overflow_bytes = overflow.chunk_len();
        dependencies->ids         = frame->info.dependencies;
    }
    else if (descriptor.type == page_type::kLeafBase) {
        const auto status =
            capture_stored_overflow(LeafFrameView(frame->buffer->bytes.data(), descriptor.frame_bytes),
                                    limits.frames.encode.max_entries, budget.bytes, &descriptor.stored_overflow);
        if (!status.ok()) {
            finish(status);
            return;
        }
    }
    dependency_index = 0;
    dependency_pid   = kInvalidPageId;
    stage            = Stage::Dependencies;
    wake();
}

Status CapturedWriteOperation::State::insert(DurableCapturedFrame value, bool reused)
{
    if (!value.backing || !value.backing->reloadable() || value.page_id >= MappingView::kPageLimit ||
        value.backing->backend_id == 0 || (pages->backend_id != 0 && pages->backend_id != value.backing->backend_id)) {
        return Status::corruption("captured write references a different or unprotected backend");
    }
    auto old = pages->frames.find(value.page_id);
    if (old != pages->frames.end()) {
        return reused && captured_write_detail::same_durable_frame(old->second, value)
                 ? Status::Ok()
                 : Status::corruption("duplicate captured PID");
    }
    if (pages->frames.size() == limits.max_frames) {
        return Status::resource_exhausted();
    }
    pages->backend_id       = value.backing->backend_id;
    pages->next_page_id     = std::max(pages->next_page_id, value.page_id + 1);
    const auto reused_bytes = reused ? value.backing->length : 0;
    pages->frames.emplace(value.page_id, std::move(value));
    record_work(budget.work_metrics, TreeWorkCounter::ReusedFrames, static_cast<uint64_t>(reused), budget.cause);
    record_work(budget.work_metrics, TreeWorkCounter::ReusedBackingBytes, reused_bytes, budget.cause);
    report.reused_frames += static_cast<uint64_t>(reused);
    return Status::Ok();
}

void CapturedWriteOperation::State::reuse_step()
{
    if (!frame.has_value()) {
        finish(Status::internal_error());
        return;
    }
    if (dependency_index == frame->reused_overflow.size()) {
        begin_write();
        return;
    }
    if (dependency_steps++ == limits.max_dependency_steps) {
        finish(Status::resource_exhausted());
        return;
    }
    if (dependency_pid == kInvalidPageId) {
        const auto &ref  = frame->reused_overflow[dependency_index];
        dependency_pid   = ref.head;
        dependency_bytes = ref.bytes;
    }
    DurableCapturedFrame old;
    const auto          &captured = frame->reused_overflow[dependency_index].backing;
    if (captured) {
        const auto found = captured->pages.find(dependency_pid);
        if (found == captured->pages.end()) {
            finish(Status::corruption("captured primary has no referenced overflow PID"));
            return;
        }
        const auto &page = found->second;
        old              = {.page_id        = dependency_pid,
                            .type           = page_type::kOverflowFrame,
                            .frame_bytes    = page.frame_bytes,
                            .logical_bytes  = page.logical_bytes,
                            .next_overflow  = page.next,
                            .overflow_bytes = page.bytes,
                            .backing        = page.backing};
    }
    else {
        if (!inherited) {
            finish(Status::unavailable("captured overflow requires its protected durable mapping"));
            return;
        }
        const auto found = inherited->frames.find(dependency_pid);
        if (found == inherited->frames.end()) {
            finish(Status::corruption("captured overflow PID is absent"));
            return;
        }
        old = found->second;
    }
    if (old.page_id != dependency_pid || old.type != page_type::kOverflowFrame || old.overflow_bytes == 0 ||
        old.overflow_bytes > dependency_bytes ||
        (old.next_overflow == kInvalidPageId) != (old.overflow_bytes == dependency_bytes)) {
        finish(Status::corruption("captured overflow topology does not match the cell length"));
        return;
    }
    if (old.backing && old.backing->storage == BackingStorage::kResidentFrame) {
        use_resident_dependency(std::move(old));
        return;
    }
    if (limits.frames.exclusion.overlaps(*old.backing)) {
        use_relocated_dependency(std::move(old));
        return;
    }
    // A dirty primary adopts exact physical extents into its own dependency
    // group. Retaining an old overflow owner would also keep deleted values
    // from the old leaf alive. Clean primaries keep their existing exact graph.
    if (limits.frames.exclusion.length != 0 || (!frame->reuse && old.backing->physical_owner)) {
        reuse_durable_dependency(std::move(old));
        return;
    }
    auto status = insert(old, true);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    attach_dependency(old);
}

} // namespace crowdb::tree
