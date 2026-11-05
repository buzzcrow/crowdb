// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/recovered_state_internal.h"
#include "snapshot/snapshot_baseline.h"

#include <cassert>

namespace crowdb::tree
{
RecoveredStateOperation::State::State(std::shared_ptr<detail::AllocationReservation> credit)
    : metadata(std::move(credit)),
      path(detail::ReservationAllocator<Path>(metadata)),
      reached(std::less<>{}, detail::ReservationAllocator<uint64_t>(metadata))
{
}

std::optional<size_t> RecoveredStateOperation::allocation_bound(size_t frames, size_t primary_pages,
                                                                size_t max_key_bytes) noexcept
{
    using detail::shared_allocation_bound;
    const auto page = detail::AllocationBound(shared_allocation_bound<PublishedPage>())
                          .add(PageState::allocation_bound())
                          .add(sizeof(BaseRef))
                          .add(MappingView::path_copy_bound())
                          .add_product(2, OwnedKey::allocation_overhead())
                          .add_product(2, max_key_bytes)
                          .value();
    if (!page || primary_pages > frames || frames > RecoveredStateLimits{}.max_pages) {
        return {};
    }
    return detail::AllocationBound(shared_allocation_bound<State>() +
                                   shared_allocation_bound<RecoveredStateOperation>() +
                                   shared_allocation_bound<PublishedTreeState>() + PublishedSources::allocation_bound())
        .add(shared_allocation_bound<SnapshotBaseline>())
        .add(detail::FlushRuntime::progress_allocation_bound())
        .add_product(frames, shared_allocation_bound<State::Load>())
        .add_product(RecoveredStateLimits{}.max_depth, sizeof(State::Path))
        .add_product(frames, shared_allocation_bound<uint64_t>())
        .add_product(primary_pages, *page)
        .value();
}

Status RecoveredStateOperation::create(std::shared_ptr<const CapturedDurablePages> captured,
                                       std::shared_ptr<PageLoader>                 loader,
                                       std::shared_ptr<detail::FlushRuntime>       runtime,
                                       std::shared_ptr<detail::FlushRuntime::Lane> lane, RecoveredStateLimits limits,
                                       std::shared_ptr<detail::AllocationReservation> metadata,
                                       std::shared_ptr<RecoveredStateOperation>      *out)
{
    auto status = create_readable(captured, std::move(loader), std::move(runtime), std::move(lane), limits,
                                  std::move(metadata), out);
    if (status.ok()) {
        (*out)->state_->require_durable = true;
        auto baseline                   = detail::reserved_shared<SnapshotBaseline>((*out)->state_->metadata);
        baseline->pages                 = std::move(captured);
        baseline->coverage              = baseline->pages->coverage;
        baseline->generation            = (*out)->state_->building->generation;
        baseline->backend_id            = baseline->pages->backend_id;
        (*out)->state_->building->snapshot_baseline = std::move(baseline);
    }
    return status;
}

Status RecoveredStateOperation::create_readable(std::shared_ptr<const ReadablePageCatalog>     captured,
                                                std::shared_ptr<PageLoader>                    loader,
                                                std::shared_ptr<detail::FlushRuntime>          runtime,
                                                std::shared_ptr<detail::FlushRuntime::Lane>    lane,
                                                RecoveredStateLimits                           limits,
                                                std::shared_ptr<detail::AllocationReservation> metadata,
                                                std::shared_ptr<RecoveredStateOperation>      *out)
{
    if (!captured || !loader || !runtime || !lane || out == nullptr || limits.max_pages == 0 || limits.max_depth == 0 ||
        limits.max_depth > 64 || limits.max_children == 0 || limits.max_children > 4096 ||
        limits.max_leaf_entries == 0 || limits.max_step_entries == 0 || limits.max_dependency_steps == 0 ||
        captured->next_page_id > MappingView::kPageLimit) {
        return Status::invalid_argument();
    }
    if (captured->frames.size() > limits.max_pages) {
        return Status::resource_exhausted();
    }
    try {
        auto state      = detail::reserved_shared<State>(metadata, metadata);
        state->captured = std::move(captured);
        state->loader   = std::move(loader);
        state->runtime  = std::move(runtime);
        state->lane     = std::move(lane);
        state->limits   = limits;
        state->path.reserve(limits.max_depth);
        state->building = detail::reserved_shared<PublishedTreeState>(metadata, metadata, state->captured->coverage);
        state->building->root_page_id = state->captured->root_page_id;
        state->building->next_page_id = state->captured->next_page_id;
        state->building->generation   = state->captured->generation == 0 ? 1 : state->captured->generation;
        state->building->coverage     = state->captured->coverage;
        state->building->summary.emplace();
        state->building->summary->covered_slot = state->captured->coverage;
        state->position                        = state->captured->frames.begin();
        const auto status =
            state->runtime->reserve_progress(state->lane, &State::run, state.get(), metadata, &state->progress);
        if (!status.ok()) {
            return status;
        }
        *out = detail::reserved_construct<RecoveredStateOperation>(metadata, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) RecoveredStateOperation(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

detail::CallbackSender RecoveredStateOperation::sender() noexcept
{
    return {this, &RecoveredStateOperation::submit};
}

const RecoveredStateResult &RecoveredStateOperation::result() const noexcept
{
    return state_->report;
}

void RecoveredStateOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void RecoveredStateOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<RecoveredStateOperation *>(source)->state_->start(complete, context);
}

void RecoveredStateOperation::State::start(detail::CallbackComplete callback, void *argument) noexcept
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

void RecoveredStateOperation::State::wake() noexcept
{
    if (lifecycle.load(std::memory_order_acquire) == 2) {
        auto self = weak_from_this().lock();
        assert(self);
        progress->request(std::move(self));
    }
}

void RecoveredStateOperation::State::run(void *context) noexcept
{
    auto *self = static_cast<State *>(context);
    if (self->lifecycle.load(std::memory_order_acquire) != 2) {
        return;
    }
    try {
        self->pump();
    }
    catch (const std::bad_alloc &) {
        self->finish(Status::resource_exhausted());
    }
    catch (...) {
        self->finish(Status::internal_error());
    }
}

void RecoveredStateOperation::State::finish(Status status) noexcept
{
    if (lifecycle.exchange(3, std::memory_order_acq_rel) == 3) {
        return;
    }
    const bool cancelled = stopped.load(std::memory_order_acquire);
    report.status        = cancelled ? Status::unavailable() : std::move(status);
    if (report.status.ok()) {
        report.state = std::move(building);
    }
    building.reset();
    path.clear();
    reached.clear();
    load.reset();
    captured.reset();
    loader.reset();
    auto signal = report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (cancelled) {
        signal = detail::CallbackSignal::kStopped;
    }
    complete(context, signal, report.status.without_message());
}
} // namespace crowdb::tree
