// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/checkpoint_operation_internal.h"

namespace crowdb::tree
{
void CheckpointOperation::State::begin_structure(uint64_t pid)
{
    // Drop the rejected proof and all its phase grants before admitting the
    // next bounded split/load window. The fixed L1 view is control-owned.
    planning_stream.reset();
    planner.reset();
    loader.reset();
    budget = {};
    work_owner.reset();
    const auto current = publisher.capture();
    if (current->l1_seq != view->l1_seq || current->generation != view->generation ||
        current->coverage != view->coverage || current->pending_handoff || current->flush_admission_owner != 0 ||
        current->checkpoint_inflight || backend_needs_recovery()) {
        finish(Status::unavailable("checkpoint split input changed"));
        return;
    }
    auto status = prepare_structure(view, pid, &structure);
    if (!status.ok() || !structure) {
        finish(status.ok() ? Status::internal_error("checkpoint split operation absent") : std::move(status));
        return;
    }
    // The split window owns any source-free descriptor it needed to create.
    // Reusing the checkpoint's fixed control credit would accumulate deferred
    // descriptor retirement over a long sequence of suffix windows.
    view = structure->captured_view();
    begin_child(structure->sender(), Stage::Structure);
}

void CheckpointOperation::State::structured()
{
    structure_l1_seq = structure->l1_sequence();
    if (*structure_l1_seq <= view->l1_seq) {
        finish(Status::internal_error("checkpoint split did not publish progress"));
        return;
    }
    ++report.structural_windows;
    std::optional<uint64_t> next;
    auto                    current = publisher.capture();
    // A full checkpoint must finish the split suffix before its layout proof.
    // Single-page writeback keeps its existing bounded page selection.
    if (!options.write.frames.selected_page) {
        if (current->l1_seq == *structure_l1_seq && current->generation == view->generation &&
            current->coverage == view->coverage) {
            for (const auto pid : structure->output_leaf_pids()) {
                const auto page = current->mapping.get(pid);
                if (page && page->level == 0 && page->leaf && page->metrics.split_candidate) {
                    next = pid;
                    break;
                }
            }
        }
    }
    structure.reset();
    if (next) {
        view = std::move(current);
        begin_structure(*next);
        return;
    }
    view.reset();
    // No second handoff: future L0 writers stay above the initial frontier.
    // A concurrent L1 writer invalidates this attempt instead of extending it.
    preflight();
}
} // namespace crowdb::tree
