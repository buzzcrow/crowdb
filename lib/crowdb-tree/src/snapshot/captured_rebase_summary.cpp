// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_rebase_internal.h"
#include "snapshot/graph_summary.h"
#include "snapshot/primary_backing.h"

namespace crowdb::tree
{
namespace
{
bool matches_frame(const PublishedTreeState &view, const ReadablePageFrame &frame)
{
    if (frame.type == page_type::kOverflowFrame) {
        return true;
    }
    const auto page = view.mapping.get(frame.page_id);
    if (!page || page->metrics.dirty || !page->primary || page->primary->frame.backing != frame.backing) {
        return false;
    }
    if (frame.type == page_type::kLeafBase) {
        return page->leaf && page->leaf->overlays.empty() && page->leaf->bases.size() == 1 &&
               page->leaf->bases.front().backing == frame.backing;
    }
    return frame.type == page_type::kInnerBase && page->level != 0 && page->inner_backing == frame.backing;
}
} // namespace

void CapturedRebaseOperation::State::summary_step()
{
    for (size_t work = 0; next != pages->frames.end() && work < 16; ++work, ++next) {
        const auto &frame = next->second;
        if (!matches_frame(*summary_view, frame) || !add_graph_summary(summary, frame)) {
            finish(Status::Ok());
            return;
        }
    }
    if (next != pages->frames.end()) {
        wake();
        return;
    }
    if (summary.reachable_leaf_pages != summary_view->stats.leaf_pages ||
        summary.reachable_inner_pages != summary_view->stats.inner_pages) {
        finish(Status::Ok());
        return;
    }
    request = detail::reserved_shared<PublicationRequest>(metadata, metadata);
    request->transaction.record =
        detail::reserved_shared<InstallRecord>(metadata, InstallToken{.generation = summary_view->generation});
    request->transaction.summary             = summary;
    request->transaction.expected_commit_seq = summary_view->commit_seq;
    request->on_done                         = [self = shared_from_this()](PublicationResult result) {
        self->publication_status = result.status.without_message();
        self->ready.store(true, std::memory_order_release);
        self->wake();
    };
    waiting         = true;
    summary_pending = true;
    ready.store(false, std::memory_order_release);
    const auto submitted = publication ? publisher.try_submit(request, publication) : publisher.try_submit(request);
    if (!submitted) {
        waiting = false;
        finish(Status::Ok());
    }
}
} // namespace crowdb::tree
