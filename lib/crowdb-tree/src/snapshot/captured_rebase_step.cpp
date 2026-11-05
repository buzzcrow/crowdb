// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/writeback_rebase.h"
#include "snapshot/captured_rebase_internal.h"
#include "snapshot/primary_backing.h"

namespace crowdb::tree
{
void CapturedRebaseOperation::State::step()
{
    if (waiting) {
        if (!ready.load(std::memory_order_acquire)) {
            return;
        }
        waiting = false;
        if (summary_pending) {
            finish(Status::Ok());
            return;
        }
        auto outcome = TreeOutcome::Success;
        if (!publication_status.ok()) {
            outcome = publication_status.code() == Code::kUnavailable ? TreeOutcome::Stale : TreeOutcome::Error;
        }
        finish_batch(outcome);
        request.reset();
    }
    if (stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable());
        return;
    }
    if (summarizing) {
        summary_step();
        return;
    }
    if (next == pages->frames.end()) {
        summary_view = publisher.capture();
        if (report.deferred != 0 || summary_view->generation != captured->generation ||
            summary_view->coverage != pages->coverage || summary_view->root_page_id != pages->root_page_id ||
            summary_view->pending_handoff) {
            finish(Status::Ok());
            return;
        }
        summarizing          = true;
        next                 = pages->frames.begin();
        summary.covered_slot = pages->coverage;
        wake();
        return;
    }
    prepare_batch();
    if (!request) {
        wake();
        return;
    }
    waiting = true;
    ready.store(false, std::memory_order_release);
    request->on_done = [self = shared_from_this()](PublicationResult result) {
        self->publication_status = result.status.without_message();
        self->ready.store(true, std::memory_order_release);
        self->wake();
    };
    const auto submitted = publication ? publisher.try_submit(request, publication) : publisher.try_submit(request);
    if (!submitted) {
        waiting = false;
        finish_batch(TreeOutcome::Error);
        record_work(publisher.work_metrics(), TreeWorkCounter::AdmissionRejected, 1, cause);
        request.reset();
        wake();
    }
}
} // namespace crowdb::tree
