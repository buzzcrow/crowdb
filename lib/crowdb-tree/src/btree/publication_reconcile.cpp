// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "publication_generation_internal.h"
#include "tree_state_reconcile.h"

namespace crowdb::tree
{
PublicationResult PublicationCoordinator::reconcile(const PublicationRequest &request)
{
    const auto &transaction = request.transaction;
    if (transaction.record || !transaction.pages.empty() || transaction.root_page_id || transaction.sources ||
        transaction.coverage || !transaction.required_installs.empty() || transaction.checkpoint ||
        transaction.handoff || transaction.flush_admission || transaction.next_page_id || transaction.merge_pair ||
        transaction.summary || request.reconciliation_slots == 0 || request.reconciliation_slots > 64) {
        return {.status = Status::invalid_argument("reconciliation requires a standalone bounded request")};
    }
    const auto *current = root_.load(std::memory_order_acquire);
    return {.status     = request.reconciliation->step_ledger(request.reconciliation_slots, *current->state,
                                                              checkpoint_->pending, checkpoint_->merge),
            .commit_seq = current->state->commit_seq};
}
} // namespace crowdb::tree
