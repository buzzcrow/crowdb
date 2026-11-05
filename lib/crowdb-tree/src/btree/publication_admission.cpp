// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "publication_admission.h"

namespace crowdb::tree
{
Status validate_flush_admission(const PublishedTreeState &current, const PublicationTransaction &transaction)
{
    if (transaction.flush_admission) {
        const auto &admission = *transaction.flush_admission;
        if (admission.owner == 0 || admission.owner != transaction.flush_admission_owner ||
            !transaction.pages.empty() || transaction.sources || transaction.coverage || transaction.root_page_id ||
            transaction.handoff || transaction.checkpoint || transaction.next_page_id) {
            return Status::invalid_argument("flush admission must be a standalone ownership transaction");
        }
        if (admission.action == FlushAdmissionAction::Acquire) {
            if (!transaction.expected_commit_seq) {
                return Status::invalid_argument("flush reserve must name its captured publication sequence");
            }
            if (current.flush_admission_owner != 0 || current.pending_handoff || current.checkpoint_inflight) {
                return Status::unavailable("another flush owns publication admission");
            }
        }
        else if (admission.action == FlushAdmissionAction::Release) {
            if (current.flush_admission_owner != admission.owner || current.pending_handoff) {
                return Status::unavailable("flush admission cannot release another owner or a partial handoff");
            }
        }
        else {
            return Status::invalid_argument("invalid flush admission action");
        }
        return Status::Ok();
    }
    if (current.flush_admission_owner != 0) {
        const bool persistent_covered = transaction.coverage && transaction.checkpoint &&
                                        transaction.checkpoint->action == CheckpointAction::Capture &&
                                      *transaction.coverage == current.coverage && !current.pending_handoff;
        if (transaction.coverage && !persistent_covered &&
            (!transaction.handoff || *transaction.coverage <= current.coverage)) {
            return Status::invalid_argument("admitted coverage must complete its forward handoff");
        }
        const bool checkpoint_completion =
            transaction.checkpoint && transaction.checkpoint->action != CheckpointAction::Capture;
        if (transaction.flush_admission_owner != current.flush_admission_owner && !checkpoint_completion) {
            return Status::unavailable("publication waits for the admitted flush boundary");
        }
    }
    else if (transaction.flush_admission_owner != 0) {
        return Status::unavailable("flush no longer owns publication admission");
    }
    return Status::Ok();
}
} // namespace crowdb::tree
