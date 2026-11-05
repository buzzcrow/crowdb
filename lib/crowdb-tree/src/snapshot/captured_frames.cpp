// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/page_encode_internal.h"
#include "captured_frames_internal.h"
#include "primary_backing.h"

#include <algorithm>
#include <cassert>

namespace crowdb::tree
{
CapturedFrameStream::State::State(std::shared_ptr<const PublishedTreeState> captured,
                                  std::shared_ptr<PageLoader> transport, std::shared_ptr<detail::FlushRuntime> cpu,
                                  std::shared_ptr<detail::FlushRuntime::Lane> queue, CapturedFrameLimits bounds,
                                  ReserveOverflow reserve)
    : view(std::move(captured)),
      loader(std::move(transport)),
      runtime(std::move(cpu)),
      lane(std::move(queue)),
      limits(std::move(bounds)),
      reserve_overflow(std::move(reserve)),
      stack(detail::ReservationAllocator<Path>(limits.encode.metadata)),
      buffer(limits.plan_only ? nullptr
                              : detail::reserved_shared<CapturedFrameBuffer>(limits.encode.metadata, limits.encode))
{
    stack.reserve(limits.max_depth);
    overflow_end = view ? view->next_page_id : 0;
}

Status CapturedFrameStream::create(std::shared_ptr<const PublishedTreeState> captured,
                                   std::shared_ptr<PageLoader> loader, std::shared_ptr<detail::FlushRuntime> runtime,
                                   std::shared_ptr<detail::FlushRuntime::Lane> lane, CapturedFrameLimits limits,
                                   ReserveOverflow reserve_overflow, std::shared_ptr<CapturedFrameStream> *out)
{
    if (!captured || !runtime || !lane || out == nullptr || limits.max_depth == 0 || limits.max_depth > 64 ||
        limits.max_pages == 0 || limits.max_pages > MappingView::kPageLimit ||
        limits.encode.frame_bytes <= kFrameHeaderSize + kFrameTrailerSize || limits.encode.max_sources == 0 ||
        limits.encode.max_entries == 0 || limits.encode.max_work_entries == 0 ||
        captured->gc_floor > captured->coverage || captured->next_page_id > MappingView::kPageLimit) {
        return Status::invalid_argument("captured frame stream requires a fixed view and bounded work");
    }
    if (captured->pending_handoff || captured->flush_admission_owner != 0 ||
        (captured->sources && !captured->sources->tables.empty())) {
        return Status::unavailable("snapshot must capture a complete handoff "
                                   "without retaining L0 sources");
    }
    try {
        const auto metadata = limits.encode.metadata;
        auto       state =
            detail::reserved_shared<State>(metadata, std::move(captured), std::move(loader), std::move(runtime),
                                           std::move(lane), std::move(limits), std::move(reserve_overflow));
        *out = detail::reserved_construct<CapturedFrameStream>(metadata, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) CapturedFrameStream(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

Status CapturedFrameStream::create(PageEncodeInput input, std::shared_ptr<PageLoader> loader,
                                   std::shared_ptr<detail::FlushRuntime>       runtime,
                                   std::shared_ptr<detail::FlushRuntime::Lane> lane, CapturedFrameLimits limits,
                                   ReserveOverflow reserve_overflow, std::shared_ptr<CapturedFrameStream> *out)
{
    if (!input.page || input.page_id >= input.next_page_id || input.next_page_id > MappingView::kPageLimit ||
        input.gc_floor > input.coverage || !runtime || !lane || out == nullptr || limits.max_depth == 0 ||
        limits.max_depth > 64 || limits.max_pages == 0 || limits.max_pages > MappingView::kPageLimit ||
        limits.encode.frame_bytes <= kFrameHeaderSize + kFrameTrailerSize || limits.encode.max_sources == 0 ||
        limits.encode.max_entries == 0 || limits.encode.max_work_entries == 0 || limits.selected_page ||
        (input.page->level != 0 && !input.context.inner_fences)) {
        return Status::invalid_argument("final page stream requires one owned output and bounded work");
    }
    try {
        const auto metadata = limits.encode.metadata;
        limits.max_depth    = 1;
        auto state = detail::reserved_shared<State>(metadata, nullptr, std::move(loader), std::move(runtime),
                                                    std::move(lane), std::move(limits), std::move(reserve_overflow));
        state->overflow_end = input.next_page_id;
        state->single_page  = std::move(input);
        *out                = detail::reserved_construct<CapturedFrameStream>(metadata, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) CapturedFrameStream(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

std::optional<size_t> CapturedFrameStream::allocation_bound(const CapturedFrameLimits &limits) noexcept
{
    using namespace detail;
    const auto page =
        AllocationBound(PageState::allocation_bound())
            .add_product(limits.encode.max_sources, sizeof(BaseRef) + sizeof(std::shared_ptr<const OverlayBatch>))
            .value();
    if (!page) {
        return {};
    }
    return AllocationBound(shared_allocation_bound<State>() + shared_allocation_bound<CapturedFrameStream>() +
                           shared_allocation_bound<State::Load>() + shared_allocation_bound<FinalPageEncodeState>() +
                           shared_allocation_bound<FinalPageEncoder>())
        .add_product(limits.max_depth, sizeof(State::Path) + shared_allocation_bound<PublishedPage>())
        .add_product(2, std::max(*page, limits.page_metadata_bytes))
        .add(limits.encode.max_metadata_bytes)
        .add(shared_allocation_bound<PrimaryReuseOutput>())
        .add(limits.encode.max_metadata_bytes)
        .add(LogicalPageCursor::allocation_bound(limits.encode.max_sources))
        .add(limits.plan_only ? 0 : shared_allocation_bound<CapturedFrameBuffer>() + limits.encode.frame_bytes)
        .value();
}

detail::CallbackSender CapturedFrameStream::sender() noexcept
{
    return {this, &CapturedFrameStream::submit};
}

void CapturedFrameStream::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<CapturedFrameStream *>(source)->state_->start(complete, context);
}

const CapturedFrameResult &CapturedFrameStream::result() const noexcept
{
    return state_->report;
}

const CapturedFrameStats &CapturedFrameStream::stats() const noexcept
{
    return state_->counters;
}

void CapturedFrameStream::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void CapturedFrameStream::State::start(detail::CallbackComplete callback, void *receiver) noexcept
{
    uint8_t idle = 0;
    if (!lifecycle.compare_exchange_strong(idle, 1, std::memory_order_acq_rel)) {
        callback(receiver, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    complete      = callback;
    context       = receiver;
    lifetime      = weak_from_this().lock();
    begin_pending = true;
    lifecycle.store(2, std::memory_order_release);
    wake();
}

void CapturedFrameStream::State::wake() noexcept
{
    wake_requested.store(true, std::memory_order_release);
    if (lifecycle.load(std::memory_order_acquire) != 2 || scheduled.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    auto self = weak_from_this().lock();
    assert(self);
    for (;;) {
        queued_owner = self;
        if (limits.cpu_progress) {
            limits.cpu_progress->request(cpu_node, self);
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
        // No worker owns this state after rejected submission. Keep scheduled
        // set through the callback so a reentrant next cannot race cleanup.
        if (lifecycle.load(std::memory_order_acquire) == 2) {
            finish(std::move(status), true);
        }
        scheduled.exchange(false, std::memory_order_acq_rel);
        if (lifecycle.load(std::memory_order_acquire) != 2 || scheduled.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
    }
}

void CapturedFrameStream::State::run(void *context) noexcept
{
    auto self = std::move(static_cast<State *>(context)->queued_owner);
    assert(self);
    self->wake_requested.exchange(false, std::memory_order_acq_rel);
    try {
        self->pump();
    }
    catch (const std::bad_alloc &) {
        self->finish(Status::resource_exhausted(), true);
    }
    catch (...) {
        self->finish(Status::internal_error(), true);
    }
    self->scheduled.exchange(false, std::memory_order_acq_rel);
    if (self->wake_requested.load(std::memory_order_acquire) && self->lifecycle.load(std::memory_order_acquire) == 2) {
        self->wake();
    }
}

void CapturedFrameStream::State::pump()
{
    if (lifecycle.load(std::memory_order_acquire) != 2) {
        return;
    }
    ++counters.cpu_steps;
    if (begin_pending) {
        begin_pending = false;
        report        = {};
        if (buffer && buffer.use_count() != 1) {
            finish(Status::unavailable("previous snapshot frame is still pinned"));
            return;
        }
    }
    if (stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable(), true);
        return;
    }
    if (!failure.ok()) {
        finish(failure);
        return;
    }
    if (load) {
        if (!load->ready.load(std::memory_order_acquire)) {
            return;
        }
        loaded();
        if (lifecycle.load(std::memory_order_acquire) != 2 || begin_pending) {
            return;
        }
    }
    step();
}

void CapturedFrameStream::State::finish(Status status, bool fatal) noexcept
{
    if (fatal) {
        // Only the status code is retained, avoiding a fallible string copy
        // on allocation-failure and cancellation paths.
        failure = status.without_message();
        encoder.reset();
        stack.clear();
        load.reset();
        report.frame.reset();
        report.plan.reset();
        report.complete = false;
    }
    report.status  = std::move(status);
    auto  self     = std::move(lifetime);
    auto  callback = complete;
    auto *receiver = context;
    auto  signal   = report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (stopped.load(std::memory_order_acquire)) {
        signal = detail::CallbackSignal::kStopped;
    }
    auto outcome = report.status.without_message();
    lifecycle.store(0, std::memory_order_release);
    callback(receiver, signal, std::move(outcome));
}
} // namespace crowdb::tree
