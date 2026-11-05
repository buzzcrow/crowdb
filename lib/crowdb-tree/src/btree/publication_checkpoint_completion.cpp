// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "published_state.h"

namespace crowdb::tree
{
bool PublicationCoordinator::try_submit_checkpoint_completion(const std::shared_ptr<PublicationRequest> &request)
{
    if (!request || !request->transaction.checkpoint ||
        (request->transaction.checkpoint->action != CheckpointAction::Commit &&
         request->transaction.checkpoint->action != CheckpointAction::Abort)) {
        return false;
    }
    const auto view = capture();
    if (!view->checkpoint_inflight || view->checkpoint_token != request->transaction.checkpoint->token ||
        request->submitted_.exchange(true, std::memory_order_acq_rel)) {
        return false;
    }
    if (checkpoint_completion_.exchange(true, std::memory_order_acq_rel)) {
        request->submitted_.store(false, std::memory_order_release);
        return false;
    }
    request->completion_slot_ = true;
    enqueue(request);
    return true;
}
} // namespace crowdb::tree
