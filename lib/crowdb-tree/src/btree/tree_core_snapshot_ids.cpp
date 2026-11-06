// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_internal.h"
#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
Status TreeCore::reserve_snapshot_ids(const PublishedTreeState &expected, uint64_t count,
                                      const std::shared_ptr<detail::AllocationReservation> &metadata)
try {
    if (!metadata || metadata->account() != binding()->account() || metadata->category() == detail::BudgetClass::kIo ||
        expected.next_page_id > MappingView::kPageLimit || count > MappingView::kPageLimit - expected.next_page_id) {
        return Status::invalid_argument("snapshot PID lease requires bounded same-domain metadata");
    }
    detail::SnapshotWaitCompletion                 completion;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto  request     = detail::reserved_shared<PublicationRequest>(metadata, metadata);
    auto &transaction = request->transaction;
    transaction.record =
        detail::reserved_shared<InstallRecord>(metadata, InstallToken{.generation = expected.generation});
    transaction.expected_commit_seq   = expected.commit_seq;
    transaction.expected_coverage     = expected.coverage;
    transaction.expected_next_page_id = expected.next_page_id;
    transaction.next_page_id          = expected.next_page_id + count;
    request->on_done                  = [&completion](PublicationResult result) {
        completion.status = std::move(result.status);
        completion.done.signal();
    };
    {
        State::Ingress ingress(*state_);
        if (ingress.owner == nullptr) {
            return Status::unavailable("Tree snapshot PID admission closed");
        }
        auto status = state_->lifetime->acquire(metadata, &ticket);
        if (!status.ok()) {
            return status;
        }
        if (!state_->publisher->try_submit(request)) {
            return Status::unavailable("snapshot PID publication queue is full");
        }
    }
    completion.done.wait();
    return completion.status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot PID lease allocation failed");
}
} // namespace crowdb::tree
