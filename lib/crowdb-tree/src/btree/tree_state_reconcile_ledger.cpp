// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "logical_page.h"
#include "tree_state_reconcile.h"

namespace crowdb::tree
{
Status TreeStateReconciliation::step_ledger(size_t max_slots, const PublishedTreeState &current,
                                            const CheckpointPendingMetrics &pending, const MergePairMetrics &merge)
{
    if (view_.get() != &current) {
        return Status::unavailable("reconciliation publication changed");
    }
    if (started_ && !ledger_mode_) {
        return Status::invalid_argument("ledger reconciliation requires a fresh cursor");
    }
    ledger_mode_ = true;
    pending_     = &pending;
    merge_       = &merge;
    Status result;
    try {
        result = step(max_slots);
    }
    catch (...) {
        pending_ = nullptr;
        merge_   = nullptr;
        throw;
    }
    pending_ = nullptr;
    merge_   = nullptr;
    return result;
}

void TreeStateReconciliation::inspect_membership(uint64_t pid, const PublishedPage &page)
{
    pending_pages_ += static_cast<uint64_t>(pending_->contains(pid));
    if (page.metrics.merge_candidate != merge_->contains_page(pid) || page.merge_outgoing != merge_->pair(pid)) {
        status_ = Status::corruption("merge membership disagrees with the publication ledger");
        return;
    }
    if (!page.merge_outgoing) {
        return;
    }
    const auto &pair   = *page.merge_outgoing;
    const auto  right  = view_->mapping.get(pair.right);
    const auto  parent = view_->mapping.get(pair.parent);
    if (pair.left != pid || !page.leaf || !right || !right->leaf || !parent || parent->level == 0 ||
        page.leaf->right_sibling != pair.right || page.leaf->content_id != pair.left_content ||
        right->leaf->content_id != pair.right_content || page.leaf->route_generation != pair.left_route ||
        right->leaf->route_generation != pair.right_route || page.metrics.split_candidate ||
        right->metrics.split_candidate) {
        status_ = Status::corruption("canonical merge pair refers to a different logical version");
        return;
    }
    ++merge_pairs_;
    merge_ready_ += static_cast<uint64_t>(pair.ready);
}

void TreeStateReconciliation::finish_ledger()
{
    TreeLogicalStats pending_stats;
    pending_->copy_to(pending_stats);
    const auto pairs = merge_->snapshot();
    if (pending_pages_ != view_->stats.checkpoint_pending_pages ||
        pending_pages_ != pending_stats.checkpoint_pending_pages ||
        pending_stats.checkpoint_mapping_pending != view_->stats.checkpoint_mapping_pending ||
        pending_->inflight() != view_->checkpoint_inflight ||
        pending_->oldest_since_ns() != view_->checkpoint_pending_since_ns ||
        merge_pairs_ != view_->stats.merge_candidate_pairs || merge_pairs_ != pairs.pairs ||
        merge_ready_ != view_->stats.merge_ready_pairs || merge_ready_ != pairs.ready ||
        recomputed_.merge_candidate_pages != pairs.pages || view_->merge_candidate_since_ns != pairs.oldest_since_ns) {
        status_ = Status::corruption("published pending or canonical pair totals disagree with their membership");
    }
}
} // namespace crowdb::tree
