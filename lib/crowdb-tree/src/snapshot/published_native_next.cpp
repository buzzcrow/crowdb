// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "snapshot/native_frame_read.h"
#include "snapshot/operation_wait.h"
#include "snapshot/published_native_internal.h"

namespace crowdb::tree
{
namespace
{
struct NativeOutput
{
    NativeOutput(std::span<const uint8_t> frame, const std::shared_ptr<detail::AllocationReservation> &metadata)
        : bytes(frame.begin(), frame.end(), detail::ReservationAllocator<uint8_t, AllocationKind::kFrame>(metadata))
    {
    }

    detail::ReservedVector<uint8_t, AllocationKind::kFrame> bytes;
};

struct ActiveLoader
{
    std::shared_ptr<SnapshotPageLoader> loader;

    ~ActiveLoader()
    {
        loader->end();
    }
};
} // namespace

Status PublishedNativeCursor::State::generated_frame(const SnapshotReadWindow       &window,
                                                     std::optional<NativeFrameView> *out)
{
    auto status = loader->begin(window.loader);
    if (!status.ok()) {
        return status;
    }
    ActiveLoader active{loader};
    status = detail::wait_snapshot_operation(stream);
    if (!status.ok()) {
        return stream->result().status;
    }
    const auto &result = stream->result();
    generated          = result.complete;
    if (result.frame) {
        const auto &frame = *result.frame;
        if (!frame.buffer || frame.info.bytes == 0 || frame.info.bytes > frame.buffer->bytes.size()) {
            return Status::corruption("native encoder returned an invalid byte window");
        }
        // The encoder deliberately reuses one buffer. Copy only this frame
        // into an independently charged owner before advancing that buffer.
        auto bytes = detail::reserved_shared<NativeOutput>(
            window.budget.reservation(), std::span<const uint8_t>(frame.buffer->bytes.data(), frame.info.bytes),
            window.budget.reservation());
        *out = NativeFrameView{.page_id      = frame.info.page_id,
                               .bytes        = bytes->bytes,
                               .owner        = std::move(bytes),
                               .durable_addr = kNoAddr,
                               .durable_plen = 0,
                               .inherited    = false};
    }
    else if (!generated) {
        return Status::corruption("native encoder returned neither a frame nor completion");
    }
    if (generated) {
        stream.reset();
    }
    return Status::Ok();
}

Status PublishedNativeCursor::State::old_frame(const SnapshotReadWindow &window, std::optional<NativeFrameView> *out)
{
    std::shared_ptr<NativeFrameRead> read;
    auto status = NativeFrameRead::create(old->first, old->second, window.backend, snapshot->binding()->cpu(),
                                          snapshot->lane(), snapshot->load_limits(), window.budget, &read);
    if (status.ok()) {
        status = detail::wait_snapshot_operation(read);
        if (!status.ok()) {
            return read->result().status;
        }
    }
    if (status.ok()) {
        if (!read->result().frame) {
            return Status::corruption("native old-frame reader produced no frame");
        }
        *out = read->result().frame;
        ++old;
    }
    return status;
}

Status PublishedNativeCursor::State::next(std::optional<NativeFrameView> *out, bool *complete)
{
    if (!failure.ok()) {
        return failure;
    }
    if (generated && old == layout->old_frames().end()) {
        if (emitted != info.frame_count) {
            return Status::corruption("native output count differs from its layout proof");
        }
        *complete = true;
        return Status::Ok();
    }
    auto bound = detail::AllocationBound(read_bound)
                     .add(NativeFrameRead::allocation_bound(snapshot->load_limits()))
                     .add(detail::shared_allocation_bound<NativeOutput>())
                     .add(limits.encode.frame_bytes)
                     .value();
    if (!bound) {
        return Status::resource_exhausted("native frame window bound overflow");
    }
    SnapshotReadWindow window;
    auto               status = snapshot->prepare_window(*bound, &window);
    if (!status.ok()) {
        return status;
    }
    if (!generated) {
        status = generated_frame(window, out);
    }
    else {
        status = old_frame(window, out);
    }
    if (!status.ok()) {
        return status;
    }
    if (*out) {
        if (emitted == info.frame_count) {
            out->reset();
            return Status::corruption("native output exceeds its planned frame count");
        }
        ++emitted;
    }
    *complete = generated && old == layout->old_frames().end();
    if (*complete && emitted != info.frame_count) {
        out->reset();
        return Status::corruption("native output omits a planned frame");
    }
    window.seal();
    return Status::Ok();
}

Status PublishedNativeCursor::next(std::optional<NativeFrameView> *out, bool *complete)
{
    if (out == nullptr || complete == nullptr) {
        return Status::invalid_argument("native cursor requires frame and completion outputs");
    }
    out->reset();
    *complete = false;
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

    try {
        auto status = state_->next(out, complete);
        if (!status.ok()) {
            state_->failure = status.without_message();
        }
        return status;
    }
    catch (const std::bad_alloc &) {
        state_->failure = Status::resource_exhausted();
        return state_->failure;
    }
}
} // namespace crowdb::tree
