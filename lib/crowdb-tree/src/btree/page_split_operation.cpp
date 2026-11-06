// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_split_operation.h"

#include "btree/allocation_bound.h"
#include "btree/maintenance_operation_internal.h"

namespace crowdb::tree
{
Status PageSplitOperation::create(PublicationCoordinator &publisher, detail::FlushRuntime &runtime,
                                  std::shared_ptr<detail::FlushRuntime::Lane> lane,
                                  std::shared_ptr<const PublishedTreeState> view, uint64_t pid,
                                  PageMaintenanceOperationOptions options, std::shared_ptr<PageLoader> loader,
                                  std::shared_ptr<ApplyDispatcher> progress, std::shared_ptr<const void> resources,
                                  TreeWorkCause cause, std::shared_ptr<PageSplitOperation> *out)
try {
    const auto page = view ? view->mapping.get(pid) : nullptr;
    if (!page || !page->leaf || page->level != 0 || !lane || !progress || !options.prepaid ||
        options.prepaid->category() != detail::BudgetClass::kReclaim || options.publication.metadata ||
        options.acquire || out == nullptr || *out) {
        return Status::invalid_argument("fixed split requires a leaf and complete owned progress credit");
    }
    auto credit = std::exchange(options.prepaid, {});
    if (view->sources && !view->sources->tables.empty()) {
        auto fixed     = detail::reserved_shared<PublishedTreeState>(credit, *view);
        auto sources   = detail::reserved_shared<PublishedSources>(credit, credit);
        sources->floor = fixed->coverage;
        fixed->sources = std::move(sources);
        view           = std::move(fixed);
    }
    options.max_candidates = options.max_steps = options.max_visits = options.visits_per_step = 1;
    options.max_stale_replans                                                                 = 0;
    auto factory = detail::reserved_shared<detail::MaintenanceFactory>(
        credit, publisher, runtime, std::move(lane), std::move(options), std::move(loader), std::move(progress));
    auto state = detail::reserved_shared<detail::MaintenanceOperation>(credit, std::move(factory), view->generation,
                                                                       view->stats.dirty_pages,
                                                                       FlushBatch::MaintenanceComplete{}, credit);
    if (state->activity_slot == PublicationCoordinator::kMaintenanceSlots) {
        return Status::unavailable("checkpoint split activity admission unavailable");
    }
    state->target_view = std::move(view);
    state->target_pid  = pid;
    state->cause       = cause;
    *out               = detail::reserved_construct<PageSplitOperation>(credit, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) PageSplitOperation(std::move(state), std::move(resources));
    });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

size_t PageSplitOperation::control_allocation_bound() noexcept
{
    // The maintenance reserve already includes its factory and operation.
    return detail::shared_allocation_bound<PageSplitOperation>() +
           detail::shared_allocation_bound<PublishedTreeState>() + detail::shared_allocation_bound<PublishedSources>();
}

detail::CallbackSender PageSplitOperation::sender() noexcept
{
    return {this, &PageSplitOperation::submit};
}

void PageSplitOperation::request_stop() noexcept
{
    state_->target_stopped.store(true, std::memory_order_release);
}

uint64_t PageSplitOperation::commit_sequence() const noexcept
{
    return state_->target_commit_seq;
}

uint64_t PageSplitOperation::l1_sequence() const noexcept
{
    return state_->target_l1_seq;
}

std::span<const uint64_t> PageSplitOperation::output_leaf_pids() const noexcept
{
    return state_->outputs;
}

const std::shared_ptr<const PublishedTreeState> &PageSplitOperation::captured_view() const noexcept
{
    return state_->target_view;
}

void PageSplitOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto &self  = *static_cast<PageSplitOperation *>(source);
    auto  state = self.state_;
    if (state->target_started.exchange(true, std::memory_order_acq_rel)) {
        complete(context, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    state->done = [complete, context](Status status, uint64_t) {
        complete(context, status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError,
                 std::move(status));
    };
    state->lifetime = state;
    state->schedule();
}
} // namespace crowdb::tree
