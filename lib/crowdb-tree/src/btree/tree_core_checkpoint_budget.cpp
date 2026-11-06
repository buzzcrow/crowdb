// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/checkpoint_backend.h"
#include "backend/local/block_account.h"
#include "btree/allocation_bound.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "snapshot/captured_rebase.h"
#include "snapshot/native_frame_read.h"
#include "snapshot/writeback_selection.h"

#include <algorithm>
#include <bit>

namespace crowdb::tree::checkpoint_detail
{
Credits::~Credits()
{
    if (snapshot) {
        snapshot->seal();
    }
    detail::seal_backend_budget(budget.backend);
    if (budget.reservation()) {
        budget.reservation()->seal();
    }
    if (owns_io && budget.io) {
        budget.io->seal();
    }
}

Preparation::~Preparation()
{
    if (control) {
        control->seal();
    }
    if (finish) {
        finish->seal();
    }
}

CheckpointOptions defaults(const TreeCore &core)
{
    CheckpointOptions options;
    const auto       &config                = core.config();
    options.write.frames.encode.frame_bytes = config.frame_bytes;
    options.write.frames.encode.inline_value_bytes =
        config.max_inline_value != 0 ? config.max_inline_value : config.frame_bytes / 4;
    options.write.write.max_frame_bytes = config.frame_bytes;
    options.write.write.compression     = config.compression;
    if (core.local_backend()) {
        options.write.write.iu_size          = core.local_backend()->ledger->limits().iu_size;
        options.write.write.buffer_alignment = std::bit_ceil(std::max<size_t>(4096, options.write.write.iu_size));
    }
    if (core.chunk_backend()) {
        options.write.write.chunk_backed     = true;
        options.write.write.iu_size          = core.chunk_backend()->iu_size;
        options.write.write.buffer_alignment = std::bit_ceil(std::max<size_t>(4096, options.write.write.iu_size));
    }
    return options;
}

Status Preparation::plan_work(const PublishedTreeState &view, const CapturedWritePlan *plan, WorkPlan *out,
                              const CapturedWriteLimits *write_limits) const
{
    using namespace detail;
    const auto &writing = write_limits != nullptr ? *write_limits : options.write;
    if (plan != nullptr && plan->reused_primary_frames > plan->primary_frames) {
        return Status::invalid_argument("checkpoint reuse count exceeds its primary count");
    }
    auto frames = writing.frames;
    if (options.select_oldest_page) {
        auto status = select_writeback_page(view, frames, &frames.selected_page);
        if (!status.ok()) {
            return status;
        }
        if (!frames.selected_page) {
            return Status::unavailable("writeback selection has no candidate");
        }
    }
    const auto   root  = view.mapping.get(view.root_page_id);
    const size_t depth = write_limits == nullptr && root && !frames.selected_page ? root->level + 1ULL : 1;
    if (depth > frames.max_depth) {
        return Status::resource_exhausted("checkpoint route depth limit");
    }
    frames.max_depth           = depth;
    frames.page_metadata_bytes = view.mapping.copy_bounds().max_leaf_bytes;
    frames.plan_only           = plan == nullptr;
    auto loads                 = core->load_limits();
    loads.max_inflight         = 1;
    // Captured encoders borrow inline values and retain typed raw overflow
    // dependencies. They never request an assembled overflow value.
    loads.max_overflow_pages   = 1;
    const auto workers         = core->binding()->cpu()->worker_count();
    const auto retained_frames = frames.encode.max_sources;
    auto       bound = AllocationBound(CapturedFrameStream::allocation_bound(frames).value_or(SIZE_MAX))
                           .add(AsyncStorePageLoader::read_reserve_bound(loads, depth, workers, retained_frames, 0, 0))
                           .add(shared_allocation_bound<AllocationReservation>());
    if (plan == nullptr) {
        bound.add(CapturedLayoutPlanner::allocation_bound(view.next_page_id, options.metadata.max_segments));
    }
    else {
        if (options.mode == CheckpointMode::Durable) {
            if (core->chunk_backend()) {
                bound.add(
                    core->chunk_backend()->checkpoint_allocation_bound(plan->frame_count_upper, plan->reference_heads));
            }
            else {
                bound.add(LocalCheckpointOperation::allocation_bound(plan->segments, writing.write.iu_size));
            }
        }
        const auto reservation = core->chunk_backend() ? core->chunk_backend()->reservation_allocation_bound()
                                                       : ExtentLedger::reservation_metadata_bound();
        auto       write       = writing;
        write.frames           = frames;
        bound
            .add(CapturedWriteOperation::allocation_bound(write, plan->frame_count_upper,
                                                          plan->reused_dependency_entries, plan->reference_heads))
            .add(CapturedRebaseOperation::allocation_bound(
                plan->primary_frames - plan->reused_primary_frames, frames.encode.max_sources,
                core->publisher().tracks_cache_residency(), plan->rebase_batches))
            .add(PageWriteOperation::allocation_bound(writing.write))
            .add_product(plan->primary_frames, reservation)
            .add_product(plan->new_overflow_frames, reservation)
            .add_product(plan->resident_dependency_entries, reservation)
            .add_product(plan->relocated_dependency_entries, reservation)
            .add(shared_allocation_bound<ReservedOverflowIds>());
    }
    if (plan != nullptr && plan->relocated_dependency_entries != 0) {
        // Completed decoder callbacks may still own their controls while the
        // next worker begins a read. Each worker can retain one such tail.
        bound.add_product(workers + 1, NativeFrameRead::allocation_bound(loads).value_or(SIZE_MAX));
    }
    const auto bytes = bound.value();
    if (!bytes) {
        return Status::resource_exhausted("checkpoint reservation bound overflow");
    }
    const auto write_calls = plan != nullptr ? AllocationBound(plan->frame_count_upper)
                                                   .add(plan->resident_dependency_entries)
                                                   .add(plan->relocated_dependency_entries)
                                                   .add(plan->segments)
                                                   .add(2)
                                                   .value()
                                             : std::optional<size_t>{0};
    if (!write_calls) {
        return Status::resource_exhausted("backend checkpoint write count overflow");
    }
    const auto max_write =
        AllocationBound(writing.write.max_frame_bytes).add(kDurableBlobHeader).add(writing.write.iu_size).value();
    if (!max_write) {
        return Status::resource_exhausted("backend write size overflow");
    }
    const BackendOperationLimits backend{.max_io_bytes = std::max(loads.max_blob_bytes, *max_write),
                                         .write_calls  = *write_calls,
                                         .workers      = workers,
                                         .barriers     = plan != nullptr && options.mode == CheckpointMode::Durable};
    const auto                   backend_bytes = backend_operation_bound(core->async_backend(), backend);
    if (!backend_bytes || !AllocationBound(*bytes).add(*backend_bytes).value()) {
        return Status::resource_exhausted("complete checkpoint window bound overflow");
    }
    *out = {.bytes = *bytes, .backend_bytes = *backend_bytes, .loads = loads, .backend = backend};
    return Status::Ok();
}

Status Preparation::bind_work(const WorkPlan &plan, detail::ResourceBudget::Credit bytes,
                              detail::ResourceBudget::Credit backend, detail::ResourceBudget::Credit io,
                              CheckpointWork *out, std::shared_ptr<detail::AllocationReservation> shared_io)
{
    using namespace detail;
    const auto account = core->binding()->account();
    if (out == nullptr || bytes.amount() != plan.bytes || bytes.allocated() ||
        !bytes.belongs_to(account, BudgetClass::kReclaim) || backend.amount() != plan.backend_bytes ||
        backend.allocated() || !backend.belongs_to(account, BudgetClass::kRouting) ||
        (shared_io ? shared_io->account() != account || shared_io->category() != BudgetClass::kIo
                   : io.amount() != 1 || io.allocated() || !io.belongs_to(account, BudgetClass::kIo))) {
        return Status::invalid_argument("checkpoint needs its complete prepaid partitions");
    }
    auto phase          = reserved_shared<Credits>(core->read_limits().metadata);
    phase->budget.bytes = AllocationReservation::create(std::move(bytes));
    phase->owns_io      = !shared_io;
    phase->budget.io = shared_io ? std::move(shared_io)
                                 : reserved_shared<AllocationReservation>(phase->budget.reservation(), std::move(io));
    phase->budget.work_metrics = core->publisher().work_metrics();
    phase->budget.cause        = options.cause;
    phase->budget.cpu_progress = cpu_progress;
    auto status =
        bind_backend_operation(core->async_backend(), plan.backend, std::move(backend), &phase->budget.backend);
    if (!status.ok()) {
        return status;
    }
    out->budget = phase->budget;
    out->loader = reserved_shared<AsyncStorePageLoader>(
        phase->budget.reservation(), ticket->backend(core->async_backend()), core->binding()->cpu(),
        core->binding()->lane(), plan.loads, OverflowBackingResolver{}, phase->budget);
    out->owner = std::move(phase);
    return Status::Ok();
}

Status Preparation::work(const PublishedTreeState &view, const CapturedWritePlan *plan, CheckpointWork *out,
                         size_t extra_bytes, std::shared_ptr<detail::AllocationReservation> shared_io,
                         const CapturedWriteLimits *write_limits)
{
    using namespace detail;
    WorkPlan bound;
    auto     status = plan_work(view, plan, &bound, write_limits);
    if (!status.ok()) {
        return status;
    }
    const auto complete_bytes = AllocationBound(bound.bytes).add(extra_bytes).value();
    if (!complete_bytes) {
        return Status::resource_exhausted("checkpoint extra bound overflow");
    }
    bound.bytes = *complete_bytes;
    // Match handoff's routing -> reclaim -> I/O acquisition order. Backend
    // allocations keep their stable Routing category through final ownership.
    auto backend = ResourceBudget::try_reserve(core->binding()->account(), BudgetClass::kRouting, bound.backend_bytes);
    auto bytes   = ResourceBudget::try_reserve(core->binding()->account(), BudgetClass::kReclaim, bound.bytes);
    auto io      = shared_io ? std::optional<ResourceBudget::Credit>(ResourceBudget::Credit{})
                             : ResourceBudget::try_reserve(core->binding()->account(), BudgetClass::kIo, 1);
    if (!backend || !bytes || !io) {
        return Status::resource_exhausted("checkpoint window credit unavailable");
    }
    return bind_work(bound, std::move(*bytes), std::move(*backend), std::move(*io), out, std::move(shared_io));
}
} // namespace crowdb::tree::checkpoint_detail
