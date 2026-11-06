// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "backend/chunk/chunk_backing.h"
#include "backend/local/scheduled_page_store.h"
#include "page_writeback_internal.h"

#include <bit>
#include <cassert>

namespace crowdb::tree
{
namespace
{
size_t padded_blob_bound(PageWriteLimits limits) noexcept
{
    const auto raw = limits.max_frame_bytes + kDurableBlobHeader;
    return ((raw + limits.iu_size - 1) / limits.iu_size) * limits.iu_size;
}

} // namespace

std::optional<size_t> PageWriteOperation::allocation_bound(PageWriteLimits limits) noexcept
{
    if (limits.max_frame_bytes < kFrameHeaderSize + kFrameTrailerSize ||
        limits.max_frame_bytes > UINT32_MAX - kDurableBlobHeader || limits.iu_size == 0 ||
        !std::has_single_bit(limits.buffer_alignment) ||
        (limits.iu_size > 1 && (limits.buffer_alignment < limits.iu_size || limits.buffer_alignment < 4096)) ||
        (limits.compression != compress_algo::kNone && limits.compression != compress_algo::kLz4)) {
        return {};
    }
    return detail::AllocationBound(
               detail::shared_allocation_bound<State>() + detail::shared_allocation_bound<PageWriteOperation>() +
               detail::shared_allocation_bound<BackingRef>() + detail::shared_allocation_bound<State::Protection>())
        .add(padded_blob_bound(limits))
        .add(limits.buffer_alignment - 1)
        .add(durable_encode_scratch_bytes(limits.compression))
        .add(limits.chunk_backed ? detail::ChunkFixedPackStore::write_allocation_bound(padded_blob_bound(limits))
                                 : std::optional<size_t>{0})
        .add(limits.chunk_backed ? detail::shared_allocation_bound<detail::AllocationReservation>() : 0)
        .add(limits.chunk_backed ? chunk_backing_allocation_bound() : 0)
        .value();
}

size_t PageWriteOperation::retained_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<BackingRef>() + detail::shared_allocation_bound<State::Protection>();
}

PageWriteOperation::State::State(PageWriteInput source, PageWriteLimits bounds, PageLoadBudget credits)
    : input(std::move(source)),
      limits(bounds),
      budget(std::move(credits)),
      io_credit(budget.io, 1),
      io_resources(budget.reservation(), TreeResourceMetric::WriteInflightBytes),
      blob(padded_blob_bound(limits) + limits.buffer_alignment - 1,
           detail::ReservationAllocator<uint8_t, AllocationKind::kIo>(budget.reservation())),
      scratch(durable_encode_scratch_bytes(limits.compression),
              detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(budget.reservation())),
      backing(detail::reserved_shared<BackingRef>(budget.reservation())),
      protection(detail::reserved_shared<Protection>(budget.reservation()))
{
    void      *data     = blob.data();
    auto       capacity = blob.size();
    const auto bytes    = padded_blob_bound(limits);
    auto      *address  = std::align(limits.buffer_alignment, bytes, data, capacity);
    assert(address != nullptr);
    aligned                  = {static_cast<uint8_t *>(address), bytes};
    protection->dependencies = input.reload_dependencies;
    backing->owner           = protection;
    if (limits.chunk_backed) {
        const auto bound = detail::ChunkFixedPackStore::write_allocation_bound(padded_blob_bound(limits));
        if (!bound || !budget.reservation()) {
            throw std::bad_alloc();
        }
        chunk_credit = detail::AllocationReservation::create(budget.reservation(), *bound);
    }
}

Status PageWriteOperation::create(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                                  std::shared_ptr<detail::FlushRuntime::Lane> lane, PageWriteInput input,
                                  PageWriteLimits limits, PageLoadBudget budget, ReserveExtent reserve,
                                  std::shared_ptr<PageWriteOperation> *out)
{
    if (!store || !runtime || !lane || !reserve || out == nullptr || !allocation_bound(limits) || !input.owner ||
        input.frame.size() != input.info.bytes || input.frame.size() > limits.max_frame_bytes ||
        input.frame.size() < kFrameHeaderSize + kFrameTrailerSize ||
        static_cast<bool>(budget.reservation()) != static_cast<bool>(budget.io) ||
        (budget.reservation() && ((budget.reservation()->category() != detail::BudgetClass::kRouting &&
                                   budget.reservation()->category() != detail::BudgetClass::kReclaim) ||
                                  budget.io->category() != detail::BudgetClass::kIo ||
                                  budget.reservation()->account() != budget.io->account()))) {
        return Status::invalid_argument("page write requires owned input and matching byte/I/O credit");
    }
    if (limits.chunk_backed ? dynamic_cast<ScheduledPageStore *>(store.get()) == nullptr
                            : !store->write_completion_is_reloadable()) {
        return Status::not_supported("backend write needs checkpoint/seal before it is independently reloadable");
    }
    try {
        auto state     = detail::reserved_shared<State>(budget.reservation(), std::move(input), limits, budget);
        state->store   = std::move(store);
        state->runtime = std::move(runtime);
        state->lane    = std::move(lane);
        state->reserve = std::move(reserve);
        *out           = detail::reserved_construct<PageWriteOperation>(budget.reservation(), [&](auto *storage) {
            ::new (static_cast<void *>(storage)) PageWriteOperation(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

detail::CallbackSender PageWriteOperation::sender() noexcept
{
    return {this, &PageWriteOperation::submit};
}

const PageWriteResult &PageWriteOperation::result() const noexcept
{
    return state_->report;
}

void PageWriteOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
}

void PageWriteOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<PageWriteOperation *>(source)->state_->start(complete, context);
}

void PageWriteOperation::State::start(detail::CallbackComplete callback, void *context) noexcept
{
    if (started.exchange(true, std::memory_order_acq_rel)) {
        callback(context, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    pending  = weak_from_this().lock();
    complete = callback;
    receiver = context;
    if (budget.cpu_progress) {
        budget.cpu_progress->request(encode_node, pending);
        return;
    }
    Status status;
    try {
        status = runtime->try_submit(lane, &State::encode, this);
    }
    catch (...) {
        status = Status::resource_exhausted();
    }
    if (!status.ok()) {
        finish(std::move(status));
    }
}

void PageWriteOperation::State::encode(void *context) noexcept
{
    auto self = static_cast<State *>(context)->pending;
    assert(self);
    if (self->stopped.load(std::memory_order_acquire)) {
        self->finish(Status::unavailable());
        return;
    }
    try {
        self->prepare();
    }
    catch (const std::bad_alloc &) {
        self->finish(Status::resource_exhausted());
    }
    catch (...) {
        self->finish(Status::io_error());
    }
}

void PageWriteOperation::State::written(void *context, Status status) noexcept
{
    auto *state = static_cast<State *>(context);
    if (state->io_complete.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    if (status.ok() && state->backing->chunk) {
        state->backing->physical_metrics = state->backing->chunk->physical_metrics;
    }
    state->io_resources.reset();
    state->io_credit.reset();
    state->finish(std::move(status));
}

void PageWriteOperation::State::finish(Status status) noexcept
{
    if (finished.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    if (chunk_credit) {
        chunk_credit->seal();
    }
    const bool cancelled = stopped.load(std::memory_order_acquire);
    report.status        = cancelled ? Status::unavailable() : std::move(status);
    if (report.status.ok()) {
        report.backing = backing;
    }
    if (!io_submitted) {
        io_credit.reset();
    }
    input.owner.reset();
    input.info.dependencies.reset();
    input.reload_dependencies.reset();
    auto self   = std::move(pending);
    auto signal = report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (cancelled) {
        signal = detail::CallbackSignal::kStopped;
    }
    complete(receiver, signal, report.status.without_message());
}
} // namespace crowdb::tree
