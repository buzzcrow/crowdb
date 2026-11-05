// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "publication_snapshot.h"

#include "logical_page.h"
#include "snapshot/captured_write.h"
#include "snapshot/snapshot_baseline.h"

namespace crowdb::tree
{
namespace
{
SnapshotPageIdentity page_identity(const std::shared_ptr<const PublishedPage> &page)
{
    if (!page) {
        return {};
    }
    SnapshotPageIdentity identity{.revision = page->revision, .level = page->level, .present = true};
    if (page->leaf) {
        identity.range         = page->leaf->range;
        identity.content       = page->leaf->content_id;
        identity.route         = page->leaf->route_generation;
        identity.right_sibling = page->leaf->right_sibling;
    }
    return identity;
}
} // namespace

Status validate_snapshot_publication(const PublicationTransaction &transaction, const PublishedTreeState &current)
{
    switch (transaction.snapshot_effect) {
    case SnapshotChangeEffect::FullContent:
        break;
    case SnapshotChangeEffect::AcceptedPrefix:
        if (!transaction.handoff) {
            return Status::invalid_argument("accepted snapshot delta requires a handoff frontier");
        }
        break;
    case SnapshotChangeEffect::SourceStructure:
        if (transaction.handoff || transaction.coverage || transaction.sources) {
            return Status::invalid_argument("source structure cannot publish new accepted contents");
        }
        break;
    default:
        return Status::invalid_argument("snapshot publication requires one typed logical effect");
    }
    if (transaction.snapshot_metadata && transaction.snapshot_metadata->category() != detail::BudgetClass::kRouting) {
        return Status::invalid_argument("snapshot selection requires Routing metadata credit");
    }
    if (!transaction.checkpoint || !transaction.checkpoint->baseline) {
        return Status::Ok();
    }
    const auto &action = *transaction.checkpoint;
    const auto &base   = *action.baseline;
    if (action.action != CheckpointAction::Commit || base.capture_token != action.token ||
        base.generation != current.generation || !base.pages || base.pages->generation != base.generation ||
        base.pages->coverage != base.coverage || base.coverage != current.checkpoint_capture_coverage ||
        base.gc_floor != current.checkpoint_capture_floor || base.gc_floor > base.coverage ||
        base.snapshot_sequence == 0 || base.backend_id == 0 ||
        (!base.pages->frames.empty() && base.pages->backend_id != base.backend_id) ||
        (current.snapshot_baseline && (base.coverage < current.snapshot_baseline->coverage ||
                                       base.snapshot_sequence <= current.snapshot_baseline->snapshot_sequence ||
                                       base.backend_id != current.snapshot_baseline->backend_id))) {
        return Status::invalid_argument("snapshot baseline differs from its committed capture");
    }
    return Status::Ok();
}

void initialize_snapshot_changes(PublishedTreeState                                   &state,
                                 const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    state.snapshot_changes               = {};
    state.snapshot_changes_since_capture = {};
    state.snapshot_baseline.reset();
    state.checkpoint_capture_coverage = 0;
    state.checkpoint_capture_floor    = 0;

    detail::ReservedVector<SnapshotPageChange> changes{detail::ReservationAllocator<SnapshotPageChange>(metadata)};
    size_t                                     count = 0;
    state.mapping.visit(&count, [](void *raw, uint64_t, const auto &) { ++*static_cast<size_t *>(raw); });
    changes.reserve(count);
    state.mapping.visit(&changes, [](void *raw, uint64_t pid, const auto &page) {
        static_cast<detail::ReservedVector<SnapshotPageChange> *>(raw)->push_back(
            {.page_id = pid, .before = {}, .after = page_identity(page)});
    });
    state.snapshot_changes = SnapshotChanges::from_sorted(changes, metadata);
}

bool logical_page_change(const PageReplacement &page)
{
    if (page.expected == page.replacement) {
        return false;
    }
    if (!page.expected || !page.replacement || page.expected->level != page.replacement->level) {
        return true;
    }
    if (page.replacement->level == 0) {
        const auto &before = *page.expected->leaf;
        const auto &after  = *page.replacement->leaf;
        return before.content_id != after.content_id || before.route_generation != after.route_generation ||
               before.range.lower != after.range.lower || before.range.upper != after.range.upper ||
               before.right_sibling != after.right_sibling;
    }
    if (!page.expected->inner || !page.replacement->inner) {
        return !page.expected->inner_backing || page.expected->inner_backing != page.replacement->inner_backing;
    }
    return page.expected->inner->children != page.replacement->inner->children ||
           page.expected->inner->separators != page.replacement->inner->separators;
}

void prepare_snapshot_changes(const PublicationTransaction &transaction, const PublishedTreeState &current,
                              PublishedTreeState &next)
{
    static_assert(SnapshotChanges::kPageLimit == MappingView::kPageLimit);
    const auto &metadata = transaction.snapshot_metadata ? transaction.snapshot_metadata : transaction.metadata;
    for (const auto &page : transaction.pages) {
        if (!logical_page_change(page)) {
            continue;
        }
        SnapshotPageChange change{.page_id         = page.page_id,
                                  .publication_seq = next.commit_seq,
                                  .before          = page_identity(current.mapping.get(page.page_id)),
                                  .after           = page_identity(next.mapping.get(page.page_id)),
                                  .effect          = transaction.snapshot_effect};
        next.snapshot_changes = next.snapshot_changes.replace(change, metadata);
        if (current.checkpoint_inflight) {
            next.snapshot_changes_since_capture = next.snapshot_changes_since_capture.replace(change, metadata);
        }
    }
    if (!transaction.checkpoint) {
        return;
    }
    switch (transaction.checkpoint->action) {
    case CheckpointAction::Capture:
        next.snapshot_changes_since_capture = {};
        next.checkpoint_capture_coverage    = next.coverage;
        next.checkpoint_capture_floor       = next.gc_floor;
        break;
    case CheckpointAction::Commit:
        next.snapshot_changes               = next.snapshot_changes_since_capture;
        next.snapshot_changes_since_capture = {};
        if (transaction.checkpoint->baseline) {
            next.snapshot_baseline = transaction.checkpoint->baseline;
        }
        break;
    case CheckpointAction::Abort:
        next.snapshot_changes_since_capture = {};
        break;
    }
}

size_t PublicationCoordinator::snapshot_change_reserve_bytes() noexcept
{
    // Both the complete pending set and its post-capture successor can change.
    return 2 * SnapshotChanges::change_allocation_bound();
}
} // namespace crowdb::tree
