// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/writeback_metrics.h"
#include "snapshot/checkpoint_operation_internal.h"
#include "snapshot/writeback_selection.h"

namespace crowdb::tree
{
void CheckpointOperation::State::preflight()
{
    auto current = publisher.capture();
    if ((fixed_coverage && current->coverage != *fixed_coverage) ||
        (fixed_generation && current->generation != *fixed_generation) ||
        (structure_l1_seq && current->l1_seq != *structure_l1_seq)) {
        finish(Status::unavailable("checkpoint fixed frontier or structural view changed"));
        return;
    }
    fixed_coverage   = current->coverage;
    fixed_generation = current->generation;
    structure_l1_seq.reset();
    if (current->pending_handoff || current->flush_admission_owner != 0 || current->checkpoint_inflight ||
        backend_needs_recovery()) {
        finish(Status::unavailable());
        return;
    }
    if (options.select_oldest_page) {
        auto status = select_writeback_page(*current, options.write.frames, &options.write.frames.selected_page);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (!options.write.frames.selected_page) {
            report.captured_coverage   = current->coverage;
            report.writeback_completed = true;
            finish(Status::Ok());
            return;
        }
    }
    if (current->checkpoint_token == UINT64_MAX) {
        finish(Status::resource_exhausted());
        return;
    }
    report.checkpoint_token = current->checkpoint_token + 1;
    const auto record       = InstallToken{.generation = current->generation, .sequence = report.checkpoint_token};
    capture_request->transaction.record     = detail::reserved_shared<InstallRecord>(control_metadata, record);
    finish_request->transaction.record      = detail::reserved_shared<InstallRecord>(finish_metadata, record);
    capture_request->transaction.checkpoint = {.action = CheckpointAction::Capture, .token = report.checkpoint_token};
    auto fixed                              = detail::reserved_shared<PublishedTreeState>(control_metadata, *current);
    // Source writers may continue above this frontier. The immutable L1 map
    // and Capture's expected L1 sequence keep this layout proof exact.
    auto sources   = detail::reserved_shared<PublishedSources>(control_metadata, control_metadata);
    sources->floor = fixed->coverage;
    fixed->sources = std::move(sources);
    view           = std::move(fixed);
    prepare_phase(false);
}

void CheckpointOperation::State::prepare_phase(bool writing)
{
    if (prepare_work_async) {
        prepared_work = {};
        begin_child(prepare_work_async(*view, writing ? &plan : nullptr, &prepared_work),
                    writing ? Stage::PrepareWrite : Stage::PrepareLayout);
        return;
    }
    if (prepare_work) {
        auto status = prepare_work(*view, writing ? &plan : nullptr, &prepared_work);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        phase_ready(writing);
        return;
    }
    if (writing) {
        begin_materialization();
    }
    else {
        begin_layout();
    }
}

void CheckpointOperation::State::phase_ready(bool writing)
{
    budget     = std::move(prepared_work.budget);
    loader     = std::move(prepared_work.loader);
    work_owner = std::move(prepared_work.owner);
    if (writing) {
        begin_materialization();
    }
    else {
        begin_layout();
    }
}

void CheckpointOperation::State::begin_layout()
{
    const auto   root  = view->mapping.get(view->root_page_id);
    const size_t depth = root && !options.write.frames.selected_page ? root->level + 1ULL : 1;
    if (depth > max_layout_depth) {
        finish(Status::resource_exhausted());
        return;
    }
    options.write.frames.max_depth        = depth;
    options.write.frames.reuse_backend_id = backend_identity();
    auto limits                           = options.write.frames;
    limits.max_depth                      = depth;
    limits.plan_only                      = true;
    limits.encode.metadata                = budget.bytes;
    limits.cpu_progress                   = budget.cpu_progress;
    planner     = detail::reserved_unique<CapturedLayoutPlanner>(budget.bytes, view->next_page_id, backend_identity(),
                                                                 options.write, options.metadata.max_segments,
                                                                 inherited_pages(), budget.bytes);
    auto status = CapturedFrameStream::create(view, loader, runtime, lane, std::move(limits), {}, &planning_stream);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    begin_child(planning_stream->sender(), Stage::PlanPage);
}

void CheckpointOperation::State::planned_page()
{
    const auto &result = planning_stream->result();
    last_plan          = result.complete;
    if (!result.plan) {
        if (last_plan) {
            planned();
        }
        else {
            finish(Status::corruption("layout stream produced no plan"));
        }
        return;
    }
    // A split window leaves its unvisited suffix explicitly marked. Complete
    // that layout proof even when the suffix already fits a physical frame.
    const auto page = view->mapping.get(result.plan->page_id);
    if (prepare_structure && page && page->leaf && page->metrics.split_candidate) {
        begin_structure(result.plan->page_id);
        return;
    }
    auto status = planner->accept(*result.plan);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    stage = Stage::PlanDependencies;
    wake();
}

void CheckpointOperation::State::plan_dependencies()
{
    auto status = planner->step();
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    if (planner->pending()) {
        wake();
        return;
    }
    if (last_plan) {
        planned();
        return;
    }
    begin_child(planning_stream->sender(), Stage::PlanPage);
}

void CheckpointOperation::State::planned()
{
    auto status = planner->finish();
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    plan = planner->result();
    planning_stream.reset();
    planner.reset();
    if (prepare_work || prepare_work_async) {
        loader.reset();
        budget = {};
        work_owner.reset();
    }
    prepare_phase(true);
}

void CheckpointOperation::State::begin_materialization()
{
    writeback_metrics = detail::reserved_shared<WritebackMetrics>(budget.bytes, publisher.work_metrics(), budget.bytes,
                                                                  plan.primary_frames);
    options.write.frames.writeback_metrics = writeback_metrics;
    capture();
}
} // namespace crowdb::tree
