// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/page_cache_residency.h"
#include "btree/writeback_rebase.h"
#include "snapshot/captured_rebase_internal.h"
#include "snapshot/primary_backing.h"

#include <algorithm>

namespace crowdb::tree
{
Status CapturedRebaseOperation::create(
    PublicationCoordinator &publisher, std::shared_ptr<const PublishedTreeState> captured,
    std::shared_ptr<const CapturedDurablePages> pages, std::shared_ptr<detail::FlushRuntime> runtime,
    std::shared_ptr<detail::FlushRuntime::Lane> lane, std::shared_ptr<detail::AllocationReservation> metadata,
    std::shared_ptr<CapturedRebaseOperation> *out, size_t max_sources,
    std::shared_ptr<WritebackMetrics> writeback_metrics, std::shared_ptr<ApplyDispatcher> cpu_progress,
    std::shared_ptr<PublicationLane> publication, TreeWorkCause cause)
try {
    if (!captured || !pages || !runtime || !lane || out == nullptr || *out || max_sources == 0 ||
        captured->generation != pages->generation || captured->coverage != pages->coverage) {
        return Status::invalid_argument();
    }
    auto state      = detail::reserved_shared<State>(metadata, publisher);
    state->captured = std::move(captured);
    state->pages    = std::move(pages);
    state->next     = state->pages->frames.begin();
    return initialize(std::move(state), std::move(runtime), std::move(lane), std::move(metadata), out, max_sources,
                      std::move(writeback_metrics), std::move(cpu_progress), std::move(publication), cause);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status CapturedRebaseOperation::initialize(std::shared_ptr<State> state, std::shared_ptr<detail::FlushRuntime> runtime,
                                           std::shared_ptr<detail::FlushRuntime::Lane>    lane,
                                           std::shared_ptr<detail::AllocationReservation> metadata,
                                           std::shared_ptr<CapturedRebaseOperation> *out, size_t max_sources,
                                           std::shared_ptr<WritebackMetrics> writeback_metrics,
                                           std::shared_ptr<ApplyDispatcher>  cpu_progress,
                                           std::shared_ptr<PublicationLane> publication, TreeWorkCause cause)
{
    state->runtime           = std::move(runtime);
    state->metadata          = std::move(metadata);
    state->max_sources       = max_sources;
    state->writeback_metrics = std::move(writeback_metrics);
    state->cpu_progress      = std::move(cpu_progress);
    state->publication       = std::move(publication);
    state->cause             = cause;
    if (!state->cpu_progress) {
        auto status = state->runtime->reserve_progress(std::move(lane), &State::run, state.get(), state->metadata,
                                                       &state->progress);
        if (!status.ok()) {
            return status;
        }
    }
    *out = detail::reserved_construct<CapturedRebaseOperation>(state->metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) CapturedRebaseOperation(std::move(state));
    });
    return Status::Ok();
}

std::optional<size_t> CapturedRebaseOperation::allocation_bound(size_t primary_frames, size_t max_sources,
                                                                bool track_residency, size_t rebase_batches) noexcept
{
    using detail::shared_allocation_bound;
    if (max_sources == 0) {
        return {};
    }
    const auto per_page = detail::AllocationBound(detail::shared_allocation_bound<PublishedPage>())
                              .add(shared_allocation_bound<PublishedPage>() + PageState::allocation_bound() +
                                   shared_allocation_bound<PrimaryBackingMetadata>() + sizeof(BaseRef))
                              .add_product(max_sources, sizeof(std::shared_ptr<const OverlayBatch>))
                              .add(track_residency ? PageCacheResidency::page_allocation_bound(1) : 0)
                              .value();
    if (!per_page) {
        return {};
    }
    return detail::AllocationBound(
               PublicationCoordinator::empty_commit_reserve_bytes() + shared_allocation_bound<PublicationRequest>() +
               shared_allocation_bound<InstallRecord>() + shared_allocation_bound<State>() +
               shared_allocation_bound<CapturedRebaseOperation>() + detail::FlushRuntime::progress_allocation_bound())
        .add_product(primary_frames, *per_page)
        .add_product(std::min(primary_frames, rebase_batches),
                     MappingView::path_copy_bound() + PublicationCoordinator::empty_commit_reserve_bytes() +
                         shared_allocation_bound<PublicationRequest>() + shared_allocation_bound<InstallRecord>() +
                         (MappingView::kFanout * sizeof(PageReplacement)))
        .value();
}

detail::CallbackSender CapturedRebaseOperation::sender() noexcept
{
    return {this, &CapturedRebaseOperation::submit};
}

const CapturedRebaseResult &CapturedRebaseOperation::result() const noexcept
{
    return state_->report;
}

void CapturedRebaseOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void CapturedRebaseOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto   &state = *static_cast<CapturedRebaseOperation *>(source)->state_;
    uint8_t idle  = 0;
    if (!state.lifecycle.compare_exchange_strong(idle, 1, std::memory_order_acq_rel)) {
        complete(context, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    state.complete = complete;
    state.context  = context;
    state.lifecycle.store(2, std::memory_order_release);
    state.wake();
}

void CapturedRebaseOperation::State::wake() noexcept
{
    if (lifecycle.load(std::memory_order_acquire) == 2) {
        auto self = weak_from_this().lock();
        if (cpu_progress) {
            cpu_progress->request(cpu_node, std::move(self));
        }
        else {
            progress->request(std::move(self));
        }
    }
}

void CapturedRebaseOperation::State::run(void *context) noexcept
{
    auto &state = *static_cast<State *>(context);
    if (state.lifecycle.load(std::memory_order_acquire) != 2) {
        return;
    }
    try {
        state.step();
    }
    catch (const std::bad_alloc &) {
        state.finish(Status::resource_exhausted());
    }
    catch (...) {
        state.finish(Status::internal_error());
    }
}

void CapturedRebaseOperation::State::finish(Status status) noexcept
{
    if (lifecycle.exchange(3, std::memory_order_acq_rel) == 3) {
        return;
    }
    report.status = std::move(status);
    request.reset();
    summary_view.reset();
    captured.reset();
    pages.reset();
    complete(context, report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError,
             report.status.without_message());
}
} // namespace crowdb::tree
