// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/scheduled_page_store.h"
#include "btree/allocation_bound.h"
#include "btree/writeback_metrics.h"
#include "snapshot/captured_write_internal.h"

namespace crowdb::tree
{
namespace
{
bool valid_write_limits(const CapturedWriteLimits &limits, const PageLoadBudget &budget)
{
    return limits.max_frames != 0 && !limits.frames.plan_only && limits.max_dependency_steps != 0 &&
           limits.frames.encode.frame_bytes <= limits.write.max_frame_bytes &&
           PageWriteOperation::allocation_bound(limits.write).has_value() &&
           static_cast<bool>(budget.bytes) == static_cast<bool>(budget.io) &&
           (!limits.frames.encode.metadata || limits.frames.encode.metadata == budget.bytes) &&
           (!budget.bytes ||
            ((budget.bytes->category() == detail::BudgetClass::kRouting ||
              budget.bytes->category() == detail::BudgetClass::kReclaim) &&
             budget.io->category() == detail::BudgetClass::kIo && budget.bytes->account() == budget.io->account()));
}
} // namespace

std::optional<size_t> CapturedWriteOperation::allocation_bound(CapturedWriteLimits limits, size_t frame_count_upper,
                                                               size_t dependency_entries,
                                                               size_t reference_heads) noexcept
{
    using detail::shared_allocation_bound;
    if (frame_count_upper > limits.max_frames || dependency_entries > limits.max_dependency_steps) {
        return {};
    }
    // Ordered-map node linkage/alignment is covered independently of payload.
    // Owner vectors count dependency uses, including shared raw chains. The
    // three-pointer allowance covers old and new capacity during growth.
    constexpr size_t node_overhead = 8 * sizeof(void *);
    return detail::AllocationBound(
               shared_allocation_bound<State>() + shared_allocation_bound<CapturedWriteOperation>() +
               shared_allocation_bound<CapturedDurablePages>() + shared_allocation_bound<State::Reservation>())
        .add_product(frame_count_upper,
                     sizeof(CapturedDurablePages::Entry) + sizeof(State::ResidentEntry) + (2 * node_overhead) +
                         shared_allocation_bound<State::Dependencies>() +
                         shared_allocation_bound<OverflowBackingSet>() + shared_allocation_bound<BackingRef>() +
                         shared_allocation_bound<StoredOverflowReferences>() +
                         PageWriteOperation::retained_allocation_bound() + sizeof(OverflowBackingSet::Entry) +
                         node_overhead + (3 * sizeof(std::shared_ptr<const void>)))
        .add_product(dependency_entries, sizeof(OverflowBackingSet::Entry) + node_overhead +
                                             (3 * sizeof(std::shared_ptr<const void>)) +
                                             shared_allocation_bound<BackingRef>())
        .add(WritebackMetrics::allocation_bound(frame_count_upper))
        .add_product(reference_heads, sizeof(StoredOverflowReference))
        .value();
}

Status CapturedWriteOperation::create(std::shared_ptr<const PublishedTreeState> captured,
                                      std::shared_ptr<PageLoader> loader, std::shared_ptr<AsyncPageStore> store,
                                      std::shared_ptr<detail::FlushRuntime>       runtime,
                                      std::shared_ptr<detail::FlushRuntime::Lane> lane, CapturedWriteLimits limits,
                                      PageLoadBudget budget, CapturedFrameStream::ReserveOverflow reserve_overflow,
                                      PageWriteOperation::ReserveExtent           reserve_extent,
                                      std::shared_ptr<const CapturedDurablePages> inherited,
                                      std::shared_ptr<CapturedWriteOperation>    *out)
{
    if (!captured || !store || !runtime || !lane || !reserve_extent || out == nullptr ||
        !valid_write_limits(limits, budget)) {
        return Status::invalid_argument();
    }
    try {
        limits.frames.encode.metadata = budget.bytes;
        limits.frames.cpu_progress    = budget.cpu_progress;
        if (budget.work_metrics) {
            limits.frames.encode.work_metrics = budget.work_metrics;
            limits.frames.encode.cause        = budget.cause;
        }
        std::shared_ptr<CapturedFrameStream> stream;
        auto status = CapturedFrameStream::create(captured, std::move(loader), runtime, lane, limits.frames,
                                                  std::move(reserve_overflow), &stream);
        if (!status.ok()) {
            return status;
        }
        auto pages          = detail::reserved_shared<CapturedDurablePages>(budget.bytes, budget.bytes);
        pages->root_page_id = captured->root_page_id;
        pages->next_page_id = captured->next_page_id;
        pages->coverage     = captured->coverage;
        pages->generation   = captured->generation;
        return create(std::move(stream), std::move(pages), std::move(store), std::move(runtime), std::move(lane),
                      std::move(limits), std::move(budget), std::move(reserve_extent), std::move(inherited), out);
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
}

Status CapturedWriteOperation::create(std::shared_ptr<CapturedFrameStream>        stream,
                                      std::shared_ptr<CapturedDurablePages>       pages,
                                      std::shared_ptr<AsyncPageStore>             store,
                                      std::shared_ptr<detail::FlushRuntime>       runtime,
                                      std::shared_ptr<detail::FlushRuntime::Lane> lane, CapturedWriteLimits limits,
                                      PageLoadBudget budget, PageWriteOperation::ReserveExtent reserve_extent,
                                      std::shared_ptr<const CapturedDurablePages> inherited,
                                      std::shared_ptr<CapturedWriteOperation>    *out)
{
    if (!stream || !pages || !pages->frames.empty() || !store || !runtime || !lane || !reserve_extent ||
        out == nullptr || !valid_write_limits(limits, budget)) {
        return Status::invalid_argument();
    }
    if (limits.write.chunk_backed ? dynamic_cast<ScheduledPageStore *>(store.get()) == nullptr
                                  : !store->write_completion_is_reloadable()) {
        return Status::not_supported();
    }
    try {
        limits.frames.encode.metadata = budget.bytes;
        limits.frames.cpu_progress    = budget.cpu_progress;
        if (budget.work_metrics) {
            limits.frames.encode.work_metrics = budget.work_metrics;
            limits.frames.encode.cause        = budget.cause;
        }
        auto state       = detail::reserved_shared<State>(budget.bytes, budget.bytes);
        state->store     = std::move(store);
        state->runtime   = std::move(runtime);
        state->lane      = std::move(lane);
        state->limits    = std::move(limits);
        state->budget    = std::move(budget);
        state->reserve   = std::move(reserve_extent);
        state->inherited = std::move(inherited);
        state->pages     = std::move(pages);
        state->stream    = std::move(stream);
        *out             = detail::reserved_construct<CapturedWriteOperation>(state->budget.bytes, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) CapturedWriteOperation(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

detail::CallbackSender CapturedWriteOperation::sender() noexcept
{
    return {this, &CapturedWriteOperation::submit};
}

const CapturedWriteResult &CapturedWriteOperation::result() const noexcept
{
    return state_->report;
}

void CapturedWriteOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void CapturedWriteOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<CapturedWriteOperation *>(source)->state_->start(complete, context);
}

void CapturedWriteOperation::State::start(detail::CallbackComplete callback, void *argument) noexcept
{
    uint8_t idle = 0;
    if (!lifecycle.compare_exchange_strong(idle, 1, std::memory_order_acq_rel)) {
        callback(argument, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    complete = callback;
    context  = argument;
    lifecycle.store(2, std::memory_order_release);
    wake();
}

void CapturedWriteOperation::State::Receiver::set_value() noexcept
{
    auto self = std::move(owner);
    self->child_done(Status::Ok());
}

void CapturedWriteOperation::State::Receiver::set_error(Status status) noexcept
{
    auto self = std::move(owner);
    self->child_done(std::move(status));
}

void CapturedWriteOperation::State::Receiver::set_stopped() noexcept
{
    auto self = std::move(owner);
    self->child_done(Status::unavailable());
}

void CapturedWriteOperation::State::child_done(Status status) noexcept
{
    child_status = std::move(status);
    child_ready.store(true, std::memory_order_release);
    wake();
}

void CapturedWriteOperation::State::begin_child(detail::CallbackSender sender, Stage next)
{
    stage   = next;
    waiting = true;
    child_ready.store(false, std::memory_order_release);
    attempt.emplace(sender, shared_from_this());
    stdexec::start(attempt->connected);
}

void CapturedWriteOperation::State::cancel_child() noexcept
{
    if (stage == Stage::Frame && stream) {
        stream->request_stop();
    }
    else if (stage == Stage::RelocateRead && relocation_read) {
        relocation_read->request_stop();
    }
    else if ((stage == Stage::Write || stage == Stage::ResidentWrite) && write) {
        write->request_stop();
    }
}

void CapturedWriteOperation::State::finish(Status status) noexcept
{
    if (lifecycle.exchange(3, std::memory_order_acq_rel) == 3) {
        return;
    }
    const bool cancelled = stopped.load(std::memory_order_acquire);
    report.status        = cancelled ? Status::unavailable() : std::move(status);
    if (report.status.ok()) {
        report.pages = std::move(pages);
    }
    pages.reset();
    descriptor.backing.reset();
    resident_dependency.backing.reset();
    resident_sources.clear();
    descriptor.stored_overflow.reset();
    frame.reset();
    dependencies.reset();
    overflow_backing.reset();
    stream.reset();
    write.reset();
    relocation_read.reset();
    inherited.reset();
    auto signal = report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (cancelled) {
        signal = detail::CallbackSignal::kStopped;
    }
    complete(context, signal, report.status.without_message());
}
} // namespace crowdb::tree
