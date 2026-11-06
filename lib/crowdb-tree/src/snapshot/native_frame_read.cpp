// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/native_frame_read.h"

#include "btree/allocation_bound.h"
#include "btree/page_load_internal.h"
#include "btree/page_load_lifetime.h"
#include "btree/recovered_frame_identity.h"
#include "crowdb-tree/maptable/compressor.h"

namespace crowdb::tree
{
namespace
{
struct NativeReadBuffer
{
    NativeReadBuffer(size_t bytes, const std::shared_ptr<detail::AllocationReservation> &metadata,
                     std::shared_ptr<const BackingRef> extent)
        : frame(bytes, detail::ReservationAllocator<uint8_t, AllocationKind::kFrame>(metadata)),
          backing(std::move(extent))
    {
    }

    detail::ReservedVector<uint8_t, AllocationKind::kFrame> frame;
    std::shared_ptr<const BackingRef>                       backing;
};
} // namespace

struct NativeFrameRead::State : PageLoadProgress, std::enable_shared_from_this<State>
{
    State(const PageLoadBudget &budget, uint64_t started_at_ns)
        : PageLoadProgress(budget, started_at_ns),
          metadata(budget.reservation()),
          blob(detail::ReservationAllocator<uint8_t>(metadata))
    {
    }

    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<AsyncPageStore>                store;
    std::shared_ptr<detail::FlushRuntime>          runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>    lane;
    std::shared_ptr<State>                         queued_owner;
    std::shared_ptr<State>                         lifetime;
    std::shared_ptr<NativeReadBuffer>              buffer;
    AlignedLoadBytes                               blob;
    OverflowBackingPage                            descriptor;
    uint64_t                                       pid = kInvalidPageId;
    std::atomic<bool>                              started{false};
    std::atomic<bool>                              terminal{false};
    std::atomic<bool>                              stopped{false};
    detail::CallbackComplete                       complete = nullptr;
    void                                          *context  = nullptr;
    NativeFrameReadResult                          report;

    void finish(Status status) noexcept
    {
        if (terminal.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        release_if_idle();
        if (stopped.load(std::memory_order_acquire)) {
            status = Status::unavailable();
        }
        auto outcome = status.ok() ? TreeOutcome::Success : TreeOutcome::Error;
        if (stopped.load(std::memory_order_acquire)) {
            outcome = TreeOutcome::Cancelled;
        }
        (void)metrics.finish(outcome, tree_metrics_now_ns());
        report.status = std::move(status);
        if (!report.status.ok()) {
            report.frame.reset();
        }
        auto keep = std::move(lifetime);
        complete(context, report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError,
                 report.status.without_message());
    }

    static void start(void *context) noexcept
    {
        auto self = std::move(static_cast<State *>(context)->queued_owner);
        try {
            if (self->stopped.load(std::memory_order_acquire)) {
                self->finish(Status::unavailable("native frame read stopped"));
                return;
            }
            if (self->descriptor.backing->storage == BackingStorage::kResidentFrame) {
                self->decode_frame();
                return;
            }
            self->blob.resize(self->descriptor.backing->length);
            self->buffer = detail::reserved_shared<NativeReadBuffer>(self->metadata, self->descriptor.frame_bytes,
                                                                     self->metadata, self->descriptor.backing);
            submit_page_read(self, self->descriptor.backing, self->blob.data(), self->blob.size());
        }
        catch (const std::bad_alloc &) {
            self->finish(Status::resource_exhausted("native frame read allocation failed"));
        }
        catch (...) {
            self->finish(Status::io_error("native frame submission threw"));
        }
    }

    static void decode(void *context) noexcept
    {
        auto self = std::move(static_cast<State *>(context)->queued_owner);
        try {
            self->decode_frame();
        }
        catch (const std::bad_alloc &) {
            self->finish(Status::resource_exhausted("native frame decode allocation failed"));
        }
        catch (...) {
            self->finish(Status::internal_error("native frame decode threw"));
        }
    }

    void decode_frame()
    {
        const auto                 &expected = descriptor;
        std::span<const uint8_t>    bytes;
        std::shared_ptr<const void> owner;
        if (expected.backing->storage == BackingStorage::kResidentFrame) {
            bytes = expected.backing->resident_frame;
            owner = expected.backing;
        }
        else {
            const auto raw     = durable_blob_raw_len(blob.data(), blob.size());
            const auto logical = durable_blob_logical_len(blob.data(), blob.size());
            if (raw != expected.frame_bytes || logical != expected.logical_bytes) {
                finish(Status::corruption("native blob length changed from its captured descriptor"));
                return;
            }
            auto status = decode_durable_page(blob.data(), blob.size(), buffer->frame.data(), expected.frame_bytes);
            if (!status.ok()) {
                finish(std::move(status));
                return;
            }
            bytes = buffer->frame;
            owner = buffer;
        }
        const auto *frame = bytes.data();
        if (!frame_validate(frame, expected.frame_bytes) || frame_page_type(frame) != page_type::kOverflowFrame ||
            !recovered_frame_identity_matches(*expected.backing, page_type::kOverflowFrame,
                                              frame_u64(frame, fh::kSelfpage_id), pid)) {
            finish(Status::corruption("native captured overflow frame has invalid CRC, type or PID"));
            return;
        }
        const OverflowFrameView overflow(frame, expected.frame_bytes);
        if (overflow.next_page_id() != expected.next || overflow.chunk_len() != expected.bytes) {
            finish(Status::corruption("native overflow topology changed from its captured descriptor"));
            return;
        }
        // Native transfer emits canonical identities. The local decoded window
        // is already owned and charged; the legacy durable bytes stay immutable.
        if (frame_u64(frame, fh::kSelfpage_id) == kInvalidPageId) {
            frame_put_u64(buffer->frame.data(), fh::kSelfpage_id, pid);
            frame_restamp_crc(buffer->frame.data(), expected.frame_bytes);
        }
        report.frame = NativeFrameView{.page_id      = pid,
                                       .bytes        = bytes,
                                       .owner        = std::move(owner),
                                       .durable_addr = kNoAddr,
                                       .durable_plen = 0,
                                       .inherited    = false};
        finish(Status::Ok());
    }
};

std::optional<size_t> NativeFrameRead::allocation_bound(PageLoadLimits limits) noexcept
{
    return detail::AllocationBound(
               detail::shared_allocation_bound<State>() + detail::shared_allocation_bound<NativeFrameRead>() +
               detail::shared_allocation_bound<NativeReadBuffer>() + detail::shared_allocation_bound<PageReadTicket>())
        .add(limits.max_blob_bytes)
        .add(chunk_load_window_bound(limits))
        .add(AlignedLoadBytes::kAlignment - 1)
        .add(limits.max_frame_bytes)
        .value();
}

Status NativeFrameRead::create(uint64_t pid, OverflowBackingPage page, std::shared_ptr<AsyncPageStore> store,
                               std::shared_ptr<detail::FlushRuntime>       runtime,
                               std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadLimits limits,
                               PageLoadBudget budget, std::shared_ptr<NativeFrameRead> *out)
{
    const auto             started_at_ns = tree_metrics_now_ns();
    std::shared_ptr<State> state;
    auto                   status = [&]() -> Status {
        try {
            if (out == nullptr || *out || pid == kInvalidPageId || !runtime || !lane || !budget.reservation() ||
                budget.reservation()->category() == detail::BudgetClass::kIo || !page.backing ||
                !page.backing->readable() || page.backing->encoding != 0 || page.bytes == 0 ||
                page.frame_bytes <= kFrameHeaderSize + kFrameTrailerSize ||
                page.bytes > overflow_chunk_cap(page.frame_bytes) || page.next == pid) {
                return Status::invalid_argument("native frame read requires a protected bounded descriptor");
            }
            const bool resident = page.backing->storage == BackingStorage::kResidentFrame;
            if (resident) {
                if (page.backing->durability != BackingDurability::kResident ||
                    page.backing->resident_frame.size() != page.frame_bytes || page.logical_bytes != page.frame_bytes) {
                    return Status::invalid_argument("native resident descriptor has inconsistent physical geometry");
                }
            }
            else if (!store || !budget.io || budget.io->category() != detail::BudgetClass::kIo ||
                     budget.reservation()->account() != budget.io->account() ||
                     page.logical_bytes < kDurableBlobHeader || page.logical_bytes > page.backing->length) {
                return Status::invalid_argument(
                    "native durable frame read needs matching I/O credit and blob geometry");
            }
            if (page.backing->length > limits.max_blob_bytes || page.frame_bytes > limits.max_frame_bytes ||
                !chunk_load_allowed(*page.backing, limits)) {
                return Status::resource_exhausted("native captured frame exceeds read bounds");
            }
            auto progress = budget;
            if (resident) {
                progress.io.reset();
            }
            state             = detail::reserved_shared<State>(budget.reservation(), progress, started_at_ns);
            state->pid        = pid;
            state->descriptor = std::move(page);
            state->store      = std::move(store);
            state->runtime    = std::move(runtime);
            state->lane       = std::move(lane);
            *out              = detail::reserved_construct<NativeFrameRead>(budget.reservation(), [&](auto *storage) {
                ::new (static_cast<void *>(storage)) NativeFrameRead(state);
            });
            return Status::Ok();
        }
        catch (const std::bad_alloc &) {
            return Status::resource_exhausted("native frame read admission failed");
        }
    }();
    if (!status.ok()) {
        if (state) {
            (void)state->metrics.finish(TreeOutcome::Error, tree_metrics_now_ns());
        }
        else {
            record_page_load_terminal(budget, started_at_ns, status);
        }
    }
    return status;
}

detail::CallbackSender NativeFrameRead::sender() noexcept
{
    return {this, &NativeFrameRead::submit};
}

const NativeFrameReadResult &NativeFrameRead::result() const noexcept
{
    return state_->report;
}

void NativeFrameRead::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
}

void NativeFrameRead::submit(void *source, detail::CallbackComplete callback, void *context)
{
    auto state = static_cast<NativeFrameRead *>(source)->state_;
    if (state->started.exchange(true, std::memory_order_acq_rel)) {
        callback(context, detail::CallbackSignal::kError, Status::unavailable("native frame read is single-use"));
        return;
    }
    state->complete = callback;
    state->context  = context;
    state->lifetime = state;
    queue_page_load(state, &State::start);
}
} // namespace crowdb::tree
