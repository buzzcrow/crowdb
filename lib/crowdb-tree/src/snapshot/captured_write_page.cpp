// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write_internal.h"
#include "snapshot/primary_backing.h"

namespace crowdb::tree
{
void CapturedWriteOperation::State::begin_write()
{
    if (!frame.has_value()) {
        finish(Status::internal_error());
        return;
    }
    if (frame->reuse) {
        if (limits.frames.exclusion.length != 0) {
            auto rebound = rebind_primary();
            if (!rebound.ok()) {
                finish(std::move(rebound));
                return;
            }
        }
        auto status = insert(descriptor, true);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        finish_frame();
        return;
    }
    PageWriteInput input{.info                = frame->info,
                         .frame               = frame->buffer->bytes,
                         .owner               = frame->buffer,
                         .reload_dependencies = dependencies};
    auto status = PageWriteOperation::create(store, runtime, lane, std::move(input), limits.write, budget,
                                             reserve_page(descriptor.type == page_type::kOverflowFrame), &write);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    begin_child(write->sender(), Stage::Write);
}

PageWriteOperation::ReserveExtent CapturedWriteOperation::State::reserve_page(bool protect_dependency)
{
    const auto weak = weak_from_this();
    return [weak, protect_dependency](uint64_t length, PageWriteOperation::ExtentComplete done) {
        const auto self = weak.lock();
        if (!self) {
            done(Status::unavailable(), {});
            return;
        }
        auto event      = detail::reserved_shared<Reservation>(self->budget.bytes);
        event->complete = std::move(done);
        if (protect_dependency) {
            event->dependencies = self->dependencies;
        }
        self->reserve(length, [event](Status status, ReservedWriteExtent extent) {
            if (event->claimed.exchange(true, std::memory_order_acq_rel)) {
                return;
            }
            try {
                if (status.ok() && event->dependencies) {
                    event->dependencies->owners.push_back(extent.owner);
                }
            }
            catch (const std::bad_alloc &) {
                status = Status::resource_exhausted();
            }
            event->complete(std::move(status), std::move(extent));
        });
    };
}

void CapturedWriteOperation::State::received_write()
{
    const auto &written      = write->result();
    descriptor.backing       = written.backing;
    descriptor.logical_bytes = written.logical_bytes;
    if (descriptor.type == page_type::kOverflowFrame) {
        overflow_backing->pages.emplace(descriptor.page_id,
                                        OverflowBackingPage{.backing       = descriptor.backing,
                                                            .next          = descriptor.next_overflow,
                                                            .bytes         = descriptor.overflow_bytes,
                                                            .frame_bytes   = descriptor.frame_bytes,
                                                            .logical_bytes = descriptor.logical_bytes});
    }
    else if (descriptor.type == page_type::kLeafBase) {
        auto primary       = detail::reserved_shared<BackingRef>(budget.bytes, *descriptor.backing);
        primary->overflow  = overflow_backing;
        descriptor.backing = std::move(primary);
    }
    auto status = insert(descriptor, false);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    ++report.written_frames;
    report.written_bytes += written.physical_bytes;
    finish_frame();
}

void CapturedWriteOperation::State::finish_frame()
{
    if (descriptor.type != page_type::kOverflowFrame) {
        dependencies.reset();
        overflow_backing.reset();
    }
    write.reset();
    frame.reset();
    if (last_frame) {
        finish(Status::Ok());
        return;
    }
    stage = Stage::Next;
    wake();
}
} // namespace crowdb::tree
