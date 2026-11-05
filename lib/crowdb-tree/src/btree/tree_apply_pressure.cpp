// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_apply_operation_internal.h"
#include "tree_core.h"

namespace crowdb::tree
{
void TreeApplyOperation::State::start_pressure()
{
    auto core = weak_core.lock();
    if (!core || (!core->local_backend() && !core->chunk_backend()) || !replay_input) {
        finish(Status::invalid_argument("chosen replay pressure requires an owned local backend"));
        return;
    }
    const auto view = core->publisher().capture();
    if (view->stats.dirty_pages == 0 && !view->stats.checkpoint_mapping_pending &&
        frontier->contiguous().load(std::memory_order_acquire) <= view->coverage) {
        // No current input needs persistence. Retained old readers or completed
        // retirement tails may still own the paid gap payload.
        waiting = Wait::kApplyCredit;
        credit_attempt.emplace(this, metadata->account(), detail::BudgetClass::kGapRepair,
                               replay_input->bound.staging_bytes + replay_input->bound.source_bytes, work);
        stdexec::start(credit_attempt->connected);
        return;
    }
    observed = progress->sequence();
    (void)source_rotation->request_pressure();
    await_progress(observed);
}

} // namespace crowdb::tree
