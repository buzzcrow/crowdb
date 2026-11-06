// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "publication_checkpoint.h"

#include "tree_metrics.h"

namespace crowdb::tree
{
Status validate_checkpoint(const PublicationTransaction &transaction, const PublishedTreeState &current,
                           const CheckpointPendingMetrics &pending)
{
    if (!transaction.checkpoint) {
        return Status::Ok();
    }
    const bool flush_capture =
        transaction.checkpoint->action == CheckpointAction::Capture && transaction.flush_admission_owner != 0 &&
        transaction.flush_admission_owner == current.flush_admission_owner && transaction.coverage &&
        transaction.sources && transaction.sources->floor == *transaction.coverage;
    if (!flush_capture && (!transaction.pages.empty() || transaction.root_page_id || transaction.sources ||
                           transaction.coverage || transaction.handoff || transaction.merge_pair)) {
        return Status::invalid_argument("checkpoint action cannot include logical mutations");
    }
    const auto &checkpoint = *transaction.checkpoint;
    const auto  action     = checkpoint.action;
    const auto  token      = checkpoint.token;
    if (action == CheckpointAction::Capture) {
        if ((!flush_capture && (current.pending_handoff || current.flush_admission_owner != 0)) ||
            current.sources->inheritance) {
            return Status::unavailable("checkpoint capture waits for complete handoff coverage");
        }
        if (!transaction.expected_coverage) {
            return Status::invalid_argument("checkpoint capture needs an explicit handoff frontier");
        }
        return pending.can_capture(token) ? Status::Ok()
                                          : Status::unavailable("checkpoint capture token is stale or busy");
    }
    if (action != CheckpointAction::Commit && action != CheckpointAction::Abort) {
        return Status::invalid_argument("invalid checkpoint action");
    }
    if (transaction.next_page_id) {
        return Status::invalid_argument("checkpoint completion cannot reserve page identifiers");
    }
    return pending.can_finish(token) ? Status::Ok() : Status::unavailable("checkpoint completion token is stale");
}

} // namespace crowdb::tree
