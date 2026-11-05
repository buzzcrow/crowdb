// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_state_reconcile.h"

#include "logical_page.h"

namespace crowdb::tree
{
TreeStateReconciliation::TreeStateReconciliation(std::shared_ptr<const PublishedTreeState> view)
    : view_(std::move(view))
{
}

void TreeStateReconciliation::visit(void *context, uint64_t pid, const std::shared_ptr<const PublishedPage> &page)
{
    auto &self = *static_cast<TreeStateReconciliation *>(context);
    ++self.visited_;
    if (!self.status_.ok()) {
        return;
    }
    if (page->metrics.leaf != (page->level == 0) || (page->level == 0 && (!page->leaf || page->inner)) ||
        (page->level != 0 && (page->leaf || (!page->inner && !page->inner_backing))) ||
        (page->metrics.split_ready && !page->metrics.split_candidate) ||
        (page->metrics.writeback_candidate && !page->metrics.dirty)) {
        self.status_ = Status::corruption("page shape or candidate metrics disagree");
        return;
    }
    if (self.pending_ != nullptr) {
        self.inspect_membership(pid, *page);
    }
    self.page_            = page;
    self.next_run_        = 0;
    self.overlay_entries_ = 0;
    if (pid == self.view_->root_page_id) {
        self.recomputed_.height = static_cast<uint64_t>(page->level) + 1;
    }
}

void TreeStateReconciliation::inspect_page()
{
    const auto &page = *page_;
    if (page.leaf) {
        const auto &leaf = *page.leaf;
        if (page.metrics.overlay_runs != leaf.overlays.size()) {
            status_ = Status::corruption("overlay run count disagrees with page metrics");
            return;
        }
        if (next_run_ < leaf.overlays.size()) {
            const auto &run = leaf.overlays[next_run_++];
            if (!run) {
                status_ = Status::corruption("overlay owner missing during reconciliation");
                return;
            }
            const size_t first = leaf.range.lower ? run->lower_bound(*leaf.range.lower) : 0;
            const size_t last  = leaf.range.upper ? run->lower_bound(*leaf.range.upper) : run->entries().size();
            if (first > last || last - first > UINT64_MAX - overlay_entries_) {
                status_ = Status::corruption("overlay contribution overflow");
                return;
            }
            overlay_entries_ += last - first;
            return;
        }
        if (overlay_entries_ != page.metrics.overlay_entries) {
            status_ = Status::corruption("overlay entry count disagrees with page metrics");
            return;
        }
    }
    if (!apply_page_metrics(recomputed_, nullptr, &page.metrics)) {
        status_ = Status::corruption("page metrics overflow during reconciliation");
    }
    page_.reset();
}

Status TreeStateReconciliation::step(size_t max_slots)
{
    if (!view_ || max_slots == 0) {
        return Status::invalid_argument("reconciliation needs a fixed view and a positive work bound");
    }
    if (ledger_mode_ && pending_ == nullptr) {
        return Status::invalid_argument("ledger reconciliation must use the publication owner");
    }
    if (complete_) {
        return status_;
    }
    started_ = true;
    for (size_t unit = 0; unit < max_slots && status_.ok() && !complete_; ++unit) {
        if (page_) {
            inspect_page();
        }
        else {
            complete_ = view_->mapping.visit_from(next_pid_, 1, this, &TreeStateReconciliation::visit) && !page_;
        }
    }
    if (!status_.ok()) {
        complete_ = true;
    }
    if (complete_ && status_.ok() &&
        (!equal_page_metrics(recomputed_, view_->stats) || recomputed_.height != view_->stats.height ||
         recomputed_.page_size_max_observed_bytes > view_->stats.page_size_max_observed_bytes ||
         view_->stats.checkpoint_pending_pages > recomputed_.page_count())) {
        status_ = Status::corruption("published aggregate differs from its fixed mapping");
    }
    if (complete_ && status_.ok() && pending_ != nullptr) {
        finish_ledger();
    }
    return status_;
}
} // namespace crowdb::tree
