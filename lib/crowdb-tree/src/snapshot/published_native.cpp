// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/checkpoint_backend.h"
#include "backend/local/local_backend_state.h"
#include "btree/allocation_bound.h"
#include "btree/tree_core.h"
#include "snapshot/native_frame_read.h"
#include "snapshot/native_plan.h"
#include "snapshot/operation_wait.h"
#include "snapshot/published_native_internal.h"
#include "snapshot/published_snapshot_budget.h"

namespace crowdb::tree
{
namespace
{
Status plan_native(const std::shared_ptr<PublishedSnapshotContext> &snapshot, CapturedFrameLimits frames,
                   NativeLayoutLimits limits, uint64_t backend_id, uint64_t incarnation, size_t load_bound,
                   std::shared_ptr<NativeLayoutPlanner> *out)
{
    frames.plan_only = true;
    const auto bound = detail::AllocationBound(load_bound)
                           .add(CapturedFrameStream::allocation_bound(frames))
                           .add(NativeLayoutPlanner::allocation_bound(snapshot->view()->next_page_id, limits))
                           .add(NativePlanOperation::allocation_bound())
                           .value();
    if (!bound) {
        return Status::resource_exhausted("native layout admission bound overflow");
    }
    SnapshotReadWindow window;
    auto               status = snapshot->prepare_window(*bound, &window);
    if (!status.ok()) {
        return status;
    }
    frames.encode.metadata = window.budget.reservation();
    auto planner = detail::reserved_shared<NativeLayoutPlanner>(window.budget.reservation(), snapshot->view(), limits,
                                                                window.budget.reservation(), backend_id, incarnation);
    std::shared_ptr<CapturedFrameStream> stream;
    status = CapturedFrameStream::create(snapshot->view(), window.loader, snapshot->binding()->cpu(), snapshot->lane(),
                                         frames, {}, &stream);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<NativePlanOperation> plan;
    status = NativePlanOperation::create(std::move(stream), planner, snapshot->binding()->cpu(), snapshot->lane(),
                                         window.budget.reservation(), &plan);
    if (status.ok()) {
        status = detail::wait_snapshot_operation(plan);
        if (!status.ok()) {
            status = plan->result();
        }
    }
    if (status.ok()) {
        *out = std::move(planner);
    }
    window.seal();
    return status;
}

std::optional<size_t> loader_bound(const PublishedSnapshotContext &snapshot, size_t depth, size_t sources)
{
    // This traversal loads primary frames only; old overflow frames have
    // their own one-frame reader and are never materialized as full values.
    auto loads               = snapshot.load_limits();
    loads.max_overflow_pages = 1;
    return AsyncStorePageLoader::read_reserve_bound(loads, depth, snapshot.binding()->cpu()->worker_count(), sources, 0,
                                                    0);
}
} // namespace

std::optional<size_t> PublishedNativeCursor::State::control_bound(const CapturedFrameLimits &limits) noexcept
{
    using detail::shared_allocation_bound;
    return detail::AllocationBound(
               shared_allocation_bound<State>() + shared_allocation_bound<PublishedNativeCursor>() +
               sizeof(NativeFrameIterator) + shared_allocation_bound<detail::AllocationReservation>() +
               shared_allocation_bound<SnapshotPageLoader>() + shared_allocation_bound<ReservedOverflowIds>() +
               shared_allocation_bound<PublicationRequest>() + shared_allocation_bound<InstallRecord>() +
               TreeOperationLifetime::ticket_allocation_bound() + PublicationCoordinator::empty_commit_reserve_bytes())
        .add(CapturedFrameStream::allocation_bound(limits))
        .value();
}

Status PublishedNativeCursor::create(const std::shared_ptr<TreeCore> &core, NativeLayoutLimits limits,
                                     std::shared_ptr<PublishedNativeCursor> *out)
try {
    if (!core || out == nullptr || *out || !NativeLayoutPlanner::allocation_bound(0, limits)) {
        return Status::invalid_argument("native cursor requires bounded admission and an empty output");
    }
    auto status = core->flush();
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    status = core->capture_snapshot(&snapshot);
    if (!status.ok()) {
        return status;
    }
    CapturedFrameLimits frames;
    frames.encode.frame_bytes  = core->config().frame_bytes;
    frames.encode.work_metrics = core->publisher().work_metrics();
    frames.encode.cause        = TreeWorkCause::Snapshot;
    frames.encode.inline_value_bytes =
        core->config().max_inline_value != 0 ? core->config().max_inline_value : frames.encode.frame_bytes / 4;
    const auto reads           = snapshot->read_limits();
    frames.encode.max_sources  = reads.max_page_sources;
    frames.page_metadata_bytes = reads.page_metadata_bytes;
    const auto root            = snapshot->view()->mapping.get(snapshot->view()->root_page_id);
    frames.max_depth           = root ? root->level + 1ULL : 1;
    frames.max_pages           = limits.max_frames;
    if (frames.max_depth > 64) {
        return Status::resource_exhausted("native root exceeds traversal depth bound");
    }
    const auto leaves = snapshot_leaf_window(*snapshot->view(), frames.encode.max_entries);
    const auto loads =
        leaves ? loader_bound(*snapshot, frames.max_depth, std::max(frames.encode.max_sources, *leaves)) : std::nullopt;
    const auto controls = State::control_bound(frames);
    if (!loads || !controls) {
        return Status::resource_exhausted("native cursor admission bound overflow");
    }
    uint64_t backend_id  = 0;
    uint64_t incarnation = 0;
    if (const auto &local = core->local_backend()) {
        backend_id  = local->ledger->limits().backend_id;
        incarnation = local->ledger->limits().generation;
    }
    else if (const auto &chunk = core->chunk_backend()) {
        backend_id  = chunk->backend_identity();
        incarnation = chunk->incarnation();
    }
    std::shared_ptr<NativeLayoutPlanner> layout;
    status = plan_native(snapshot, frames, limits, backend_id, incarnation, *loads, &layout);
    if (!status.ok()) {
        return status;
    }
    auto credit =
        detail::ResourceBudget::try_reserve(snapshot->binding()->account(), detail::BudgetClass::kRouting, *controls);
    if (!credit) {
        return Status::resource_exhausted("native cursor control reserve unavailable");
    }
    auto metadata                 = detail::AllocationReservation::create(std::move(*credit));
    auto state                    = detail::reserved_shared<State>(metadata);
    state->metadata               = metadata;
    state->snapshot               = std::move(snapshot);
    state->layout                 = std::move(layout);
    state->old                    = state->layout->old_frames().begin();
    state->limits                 = std::move(frames);
    state->limits.encode.metadata = metadata;
    state->read_bound             = *loads;
    state->loader                 = detail::reserved_shared<SnapshotPageLoader>(metadata);
    const auto &view              = state->snapshot->view();
    const auto  count             = state->layout->new_overflow_frames();
    const auto  end               = view->next_page_id + count;
    state->info                   = {.root_page_id      = view->root_page_id,
                                     .at_slot           = view->coverage,
                                     .next_page_id      = end,
                                     .frame_count       = state->layout->frame_count(),
                                     .frame_count_known = true};
    status                        = state->restart();
    if (!status.ok()) {
        return status;
    }
    auto result = detail::reserved_construct<PublishedNativeCursor>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) PublishedNativeCursor(state); });
    // All persistent controls are prepared before the shared PID authority
    // advances. A changed publication rejects this exact layout proof.
    status = core->reserve_snapshot_ids(*view, count, metadata);
    if (status.ok()) {
        *out = std::move(result);
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("native cursor allocation failed");
}

const NativeStreamInfo &PublishedNativeCursor::info() const noexcept
{
    return state_->info;
}

const std::shared_ptr<detail::AllocationReservation> &PublishedNativeCursor::metadata() const noexcept
{
    return state_->metadata;
}
} // namespace crowdb::tree
