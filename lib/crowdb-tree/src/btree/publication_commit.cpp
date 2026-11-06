// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "btree/backing_ref.h"
#include "btree/logical_page.h"
#include "btree/publication_generation_internal.h"
#include "page_cache.h"
#include "publication_admission.h"
#include "publication_checkpoint.h"
#include "publication_snapshot.h"

#include <algorithm>
#include <cassert>

namespace crowdb::tree
{

namespace
{
bool invalidates_merge(const PageReplacement &page)
{
    return logical_page_change(page) || (page.replacement && page.replacement->metrics.split_candidate);
}

bool matches_expected_page(const std::shared_ptr<const PublishedPage> &current,
                           const std::shared_ptr<const PublishedPage> &expected)
{
    if (current == expected) {
        return true;
    }
    if (!current || !expected || current->revision != expected->revision || current->level != expected->level ||
        current->leaf != expected->leaf || current->inner != expected->inner ||
        current->inner_backing != expected->inner_backing || current->primary != expected->primary ||
        current->allocation_owner != expected->allocation_owner) {
        return false;
    }
    // Classification can copy a sibling without changing its representation.
    // Its derived membership is recomputed from the current ledger below.
    auto current_metrics             = current->metrics;
    auto expected_metrics            = expected->metrics;
    current_metrics.merge_candidate  = false;
    expected_metrics.merge_candidate = false;
    return current_metrics == expected_metrics;
}

void prepare_pending(const PublicationTransaction &transaction, const PublishedTreeState &current,
                     CheckpointPendingMetrics::PreparedDelta &delta)
{
    for (const auto &page : transaction.pages) {
        if (!logical_page_change(page)) {
            continue;
        }
        if (page.replacement) {
            delta.change_page(page.page_id);
        }
        else {
            delta.remove_page(page.page_id);
        }
        delta.change_mapping();
    }
    if (transaction.root_page_id.value_or(current.root_page_id) != current.root_page_id ||
        transaction.coverage.value_or(current.coverage) != current.coverage) {
        delta.change_mapping();
    }
}

std::shared_ptr<PublishedTreeState> checkpoint_artifact(const std::shared_ptr<PublishedTreeState> &next,
                                                        const PublicationTransaction              &transaction)
{
    if (!transaction.checkpoint || transaction.checkpoint->action != CheckpointAction::Capture) {
        return {};
    }
    auto snapshot     = detail::reserved_shared<PublishedTreeState>(transaction.metadata, *next);
    auto sources      = detail::reserved_shared<PublishedSources>(transaction.metadata, transaction.metadata);
    sources->floor    = next->coverage;
    snapshot->sources = std::move(sources);
    return snapshot;
}

Status validate_merge_pair(const PublishedTreeState &current, const PublicationTransaction &transaction)
{
    if (!transaction.merge_pair) {
        return Status::Ok();
    }
    if (!transaction.pages.empty() || transaction.root_page_id || transaction.sources || transaction.coverage ||
        transaction.checkpoint || transaction.handoff || transaction.next_page_id || transaction.flush_admission) {
        return Status::invalid_argument("merge classification must be a standalone transaction");
    }
    const auto &update = *transaction.merge_pair;
    if (!update.expected_left || !update.expected_left->leaf ||
        current.mapping.get(update.left) != update.expected_left) {
        return Status::unavailable("merge classification left page changed");
    }
    if (!update.candidate) {
        return Status::Ok();
    }
    const auto &pair   = *update.candidate;
    const auto &left   = update.expected_left;
    const auto &right  = update.expected_right;
    const auto &parent = update.expected_parent;
    if (pair.left != update.left || pair.left == pair.right || !right || !right->leaf || !parent || !parent->inner ||
        current.mapping.get(pair.right) != right || current.mapping.get(pair.parent) != parent ||
        left->leaf->right_sibling != pair.right || left->leaf->content_id != pair.left_content ||
        right->leaf->content_id != pair.right_content || left->leaf->route_generation != pair.left_route ||
        right->leaf->route_generation != pair.right_route || left->metrics.split_candidate ||
        right->metrics.split_candidate) {
        return Status::unavailable("merge classification route or content changed");
    }
    const auto child = std::find(parent->inner->children.begin(), parent->inner->children.end(), pair.left);
    if (child == parent->inner->children.end() || child + 1 == parent->inner->children.end() ||
        *(child + 1) != pair.right) {
        return Status::unavailable("merge pair no longer shares one parent");
    }
    if (pair.ready && (!left->metrics.page_size_known || !left->metrics.kv_known || !right->metrics.page_size_known ||
                       !right->metrics.kv_known)) {
        return Status::invalid_argument("ready merge pair requires exact page summaries");
    }
    return Status::Ok();
}

Status validate_transaction(const PublishedTreeState &current, const PublicationTransaction &transaction,
                            uint64_t publisher_id)
{
    if (!transaction.record || transaction.record->token.generation != current.generation) {
        return Status::unavailable("publication belongs to a different tree generation");
    }
    const bool checkpoint_summary =
        transaction.checkpoint && transaction.checkpoint->action == CheckpointAction::Commit &&
        current.checkpoint_inflight && transaction.expected_l1_seq && *transaction.expected_l1_seq == current.l1_seq;
    if (transaction.summary &&
        ((!transaction.expected_commit_seq && !checkpoint_summary) || !transaction.pages.empty() ||
         transaction.root_page_id || transaction.sources || transaction.coverage ||
         (transaction.checkpoint && !checkpoint_summary) || transaction.handoff || transaction.merge_pair ||
         transaction.flush_admission || transaction.next_page_id ||
         transaction.summary->covered_slot != current.coverage || !transaction.summary->exact)) {
        return Status::invalid_argument("summary installation requires a fixed unmodified graph");
    }
    if (transaction.expected_commit_seq && *transaction.expected_commit_seq != current.commit_seq) {
        return Status::unavailable("publication state changed before admission");
    }
    if (transaction.expected_l1_seq && *transaction.expected_l1_seq != current.l1_seq) {
        return Status::unavailable("publication L1 view changed");
    }
    if (transaction.expected_next_page_id && *transaction.expected_next_page_id != current.next_page_id) {
        return Status::unavailable("page identifier authority changed before publication");
    }
    if (transaction.next_page_id &&
        (!transaction.expected_next_page_id || *transaction.next_page_id < current.next_page_id ||
         *transaction.next_page_id > MappingView::kPageLimit)) {
        return Status::invalid_argument("page identifier reservation must monotonically advance the authority");
    }
    if (auto admission = validate_flush_admission(current, transaction); !admission.ok()) {
        return admission;
    }
    if (auto merge = validate_merge_pair(current, transaction); !merge.ok()) {
        return merge;
    }
    if (transaction.handoff) {
        const auto &handoff = *transaction.handoff;
        if (handoff.batch != transaction.record->token.batch || handoff.frontier <= current.coverage ||
            (transaction.coverage && *transaction.coverage != handoff.frontier) ||
            (transaction.root_page_id && !transaction.coverage)) {
            return Status::invalid_argument("invalid handoff identity or frontier");
        }
        if (current.pending_handoff && current.pending_handoff != transaction.handoff) {
            return Status::unavailable("another handoff needs repair before publication");
        }
    }
    else if (current.pending_handoff &&
             (!transaction.pages.empty() || transaction.root_page_id || transaction.sources || transaction.coverage)) {
        return Status::unavailable("logical mutation waits for complete handoff coverage");
    }
    if (transaction.expected_coverage && *transaction.expected_coverage != current.coverage) {
        return Status::unavailable("publication coverage changed");
    }
    if (transaction.expected_root_page_id && *transaction.expected_root_page_id != current.root_page_id) {
        return Status::unavailable("publication root changed");
    }
    if (transaction.sources && transaction.expected_sources != current.sources) {
        return Status::unavailable("publication source catalog changed");
    }
    const auto coverage = transaction.coverage.value_or(current.coverage);
    if (transaction.sources && current.sources->inheritance && !transaction.sources->inheritance &&
        coverage < current.sources->inheritance->frontier) {
        return Status::unavailable("split prefix must be covered before inherited sources detach");
    }
    if (transaction.sources && transaction.sources->inheritance) {
        const auto &inherited = *transaction.sources->inheritance;
        if (inherited.owner_identity == 0 || inherited.session == 0 || inherited.frontier < coverage ||
            inherited.local_count > transaction.sources->tables.size()) {
            return Status::invalid_argument("invalid split inheritance identity or frontier");
        }
        for (size_t i = inherited.local_count; i < transaction.sources->tables.size(); ++i) {
            const auto &table = transaction.sources->tables[i];
            if (!table || !table->closed() || table->writers() != 0) {
                return Status::unavailable("inherited sources must be closed and drained before publication");
            }
        }
    }
    if (coverage < current.coverage || (transaction.coverage && !transaction.sources) ||
        (transaction.sources && transaction.sources->floor != coverage)) {
        return Status::invalid_argument("coverage and source floor must publish together");
    }
    for (const auto &record : transaction.required_installs) {
        if (!record || record->publisher_id() != publisher_id || record->token.generation != current.generation ||
            record->token.batch != transaction.record->token.batch ||
            !record->committed.load(std::memory_order_acquire)) {
            return Status::unavailable("handoff has unfinished range installations");
        }
    }
    uint64_t previous = 0;
    bool     first    = true;
    for (const auto &page : transaction.pages) {
        if (page.page_id >= MappingView::kPageLimit || (!first && page.page_id <= previous)) {
            return Status::invalid_argument("replacement PIDs must be unique and ordered");
        }
        if (!matches_expected_page(current.mapping.get(page.page_id), page.expected)) {
            return Status::unavailable("page changed; publication needs replanning");
        }
        if (!page.expected && !page.replacement) {
            return Status::invalid_argument("an absent page cannot be removed");
        }
        if (!page.expected && page.replacement && page.page_id < current.next_page_id) {
            return Status::invalid_argument("a retired PID cannot be reused");
        }
        if (page.replacement && ((page.replacement->level == 0) != page.replacement->metrics.leaf)) {
            return Status::invalid_argument("page metrics kind disagrees with routing level");
        }
        if (page.replacement &&
            (page.replacement->level == 0
                 ? (!page.replacement->leaf || page.replacement->inner || page.replacement->inner_backing)
                 : (page.replacement->leaf ||
                    (!page.replacement->inner &&
                     (!page.replacement->inner_backing || !page.replacement->inner_backing->readable()))))) {
            return Status::invalid_argument("published page must have the representation for its routing level");
        }
        previous = page.page_id;
        first    = false;
    }
    return Status::Ok();
}
} // namespace

size_t PublicationCoordinator::empty_commit_reserve_bytes() noexcept
{
    // Includes a replacement request after a rejected attempt and allocator
    // control blocks. No mapping or checkpoint collection grows here.
    return detail::shared_allocation_bound<PublishedTreeState>() + sizeof(Root) + sizeof(PublicationRequest) +
           sizeof(InstallRecord) + 1024;
}

size_t PublicationCoordinator::merge_membership_reserve_bytes(size_t pages) noexcept
{
    return detail::AllocationBound()
        .add_product(pages, MappingView::path_copy_bound() + detail::shared_allocation_bound<PublishedPage>() +
                                (3 * sizeof(PageReplacement)) + 64)
        .value()
        .value_or(SIZE_MAX);
}

PublicationResult PublicationCoordinator::commit(const PublicationTransaction &transaction)
{
    const auto       *current = root_.load(std::memory_order_relaxed);
    PublicationResult result;
    if (!transaction.record || transaction.record->token.generation != current->state->generation) {
        result.status = Status::unavailable("publication belongs to a different tree generation");
        return result;
    }
    uint64_t expected_owner = 0;
    if (!transaction.record->publisher_id_.compare_exchange_strong(expected_owner, identity_,
                                                                   std::memory_order_acq_rel) &&
        expected_owner != identity_) {
        result.status = Status::unavailable("install record belongs to a different publisher");
        return result;
    }
    if (transaction.record->committed.load(std::memory_order_acquire)) {
        result.commit_seq        = transaction.record->commit_seq;
        result.l1_seq            = transaction.record->l1_seq;
        result.already_committed = true;
        result.checkpoint_view   = transaction.record->checkpoint_view;
        return result;
    }
    result.status = validate_transaction(*current->state, transaction, identity_);
    if (!result.status.ok()) {
        return result;
    }
    result.status = validate_checkpoint(transaction, *current->state, checkpoint_->pending);
    if (!result.status.ok()) {
        return result;
    }
    result.status = validate_snapshot_publication(transaction, *current->state);
    if (!result.status.ok()) {
        return result;
    }
    detail::BudgetMemoryResource::Scope checkpoint_credit(checkpoint_->resource, transaction.metadata);
    auto next          = detail::reserved_shared<PublishedTreeState>(transaction.metadata, *current->state);
    next->next_page_id = transaction.next_page_id.value_or(next->next_page_id);
    next->mapping      = current->state->mapping.replace(transaction.pages, result.copied, transaction.metadata);
    for (const auto &page : transaction.pages) {
        const auto previous = current->state->mapping.get(page.page_id);
        if (!apply_page_metrics(next->stats, previous ? &previous->metrics : nullptr,
                                page.replacement ? &page.replacement->metrics : nullptr)) {
            result.status = Status::invalid_argument("invalid logical metrics delta");
            return result;
        }
        next->next_page_id = std::max(next->next_page_id, page.page_id + 1);
    }
    next->root_page_id   = transaction.root_page_id.value_or(next->root_page_id);
    const auto root_page = next->mapping.get(next->root_page_id);
    if (next->root_page_id != UINT64_MAX && !root_page) {
        result.status = Status::invalid_argument("published root must exist in the same mapping");
        return result;
    }
    next->stats.height = root_page ? uint64_t{root_page->level} + 1 : 0;
    next->coverage     = transaction.coverage.value_or(next->coverage);
    next->gc_floor =
        std::max(next->gc_floor, std::min(next->coverage, requested_gc_floor_.load(std::memory_order_acquire)));
    if (transaction.flush_admission) {
        next->flush_admission_owner = transaction.flush_admission->action == FlushAdmissionAction::Acquire
                                        ? transaction.flush_admission->owner
                                        : 0;
    }
    else if ((transaction.coverage && !transaction.checkpoint) ||
             (transaction.checkpoint && transaction.checkpoint->action != CheckpointAction::Capture)) {
        next->flush_admission_owner = 0;
    }
    if (transaction.handoff) {
        next->pending_handoff = transaction.coverage ? std::nullopt : transaction.handoff;
    }
    if (transaction.sources) {
        next->sources = transaction.sources;
    }
    next->commit_seq++;
    const auto pending_changes =
        static_cast<size_t>(std::count_if(transaction.pages.begin(), transaction.pages.end(), logical_page_change));
    if (pending_changes != 0 || next->root_page_id != current->state->root_page_id ||
        next->coverage != current->state->coverage) {
        next->summary.reset();
    }
    if (transaction.summary) {
        next->summary = transaction.summary;
    }
    CheckpointPendingMetrics::PreparedDelta pending(checkpoint_->pending, pending_changes);
    prepare_pending(transaction, *current->state, pending);
    pending.copy_to(next->stats);
    std::optional<MergePairMetrics::Prepared> merge;
    if (transaction.merge_pair ||
        (checkpoint_->merge.snapshot().pairs != 0 && std::ranges::any_of(transaction.pages, invalidates_merge))) {
        merge.emplace(checkpoint_->merge, transaction.metadata, checkpoint_);
        for (const auto &page : transaction.pages) {
            if (invalidates_merge(page)) {
                merge->invalidate_page(page.page_id);
                merge->invalidate_parent(page.page_id);
            }
        }
        if (transaction.merge_pair) {
            const auto &update = *transaction.merge_pair;
            if (update.candidate) {
                merge->set_pair(*update.candidate, tree_metrics_now_ns());
            }
            else {
                merge->clear_pair(update.left);
            }
        }
        merge->finish();
        const auto totals = merge->snapshot();
        // Unique endpoint page flags are published below with the same root.
        // The canonical pair ledger owns pair counts and candidate age.
        next->stats.merge_candidate_pairs = totals.pairs;
        next->stats.merge_ready_pairs     = totals.ready;
        next->merge_candidate_since_ns    = totals.oldest_since_ns;
    }
    detail::ReservedVector<uint64_t> membership_pids{detail::ReservationAllocator<uint64_t>(transaction.metadata)};
    if (merge) {
        for (const auto pid : merge->affected_pages()) {
            membership_pids.push_back(pid);
        }
    }
    for (const auto &page : transaction.pages) {
        if (page.replacement && page.replacement->leaf) {
            const bool present =
                merge ? merge->contains_page(page.page_id) : checkpoint_->merge.contains_page(page.page_id);
            const auto proof = merge ? merge->pair(page.page_id) : checkpoint_->merge.pair(page.page_id);
            if (page.replacement->metrics.merge_candidate != present || page.replacement->merge_outgoing != proof) {
                membership_pids.push_back(page.page_id);
            }
        }
    }
    std::sort(membership_pids.begin(), membership_pids.end());
    membership_pids.erase(std::unique(membership_pids.begin(), membership_pids.end()), membership_pids.end());
    detail::ReservedVector<PageReplacement> memberships{
        detail::ReservationAllocator<PageReplacement>(transaction.metadata)};
    memberships.reserve(membership_pids.size());
    for (const auto pid : membership_pids) {
        const auto before  = next->mapping.get(pid);
        const bool present = merge ? merge->contains_page(pid) : checkpoint_->merge.contains_page(pid);
        const auto proof   = merge ? merge->pair(pid) : checkpoint_->merge.pair(pid);
        if (!before || (before->metrics.merge_candidate == present && before->merge_outgoing == proof)) {
            continue;
        }
        auto after                     = detail::reserved_shared<PublishedPage>(transaction.metadata, *before);
        after->metrics.merge_candidate = present;
        after->merge_outgoing          = proof;
        if (!apply_page_metrics(next->stats, &before->metrics, &after->metrics)) {
            result.status = Status::invalid_argument("invalid merge membership delta");
            return result;
        }
        memberships.push_back({.page_id = pid, .expected = before, .replacement = std::move(after)});
    }
    next->mapping        = next->mapping.replace(memberships, result.copied, transaction.metadata);
    const auto &previous = *current->state;
    if (!next->mapping.same_root(previous.mapping) || next->root_page_id != previous.root_page_id ||
        next->next_page_id != previous.next_page_id || next->coverage != previous.coverage ||
        next->gc_floor != previous.gc_floor || next->pending_handoff != previous.pending_handoff ||
        next->flush_admission_owner != previous.flush_admission_owner) {
        if (previous.l1_seq == UINT64_MAX) {
            result.status = Status::resource_exhausted("publication L1 sequence exhausted");
            return result;
        }
        ++next->l1_seq;
    }
    prepare_snapshot_changes(transaction, *current->state, *next);
    auto                                                     snapshot = checkpoint_artifact(next, transaction);
    std::optional<CheckpointPendingMetrics::PreparedCapture> prepared_capture;
    if (snapshot) {
        prepared_capture.emplace(checkpoint_->pending, transaction.metadata, checkpoint_);
    }
    std::shared_ptr<const void> checkpoint_retired;
    result.commit_seq = next->commit_seq;
    result.l1_seq     = next->l1_seq;
    auto residency    = page_cache_ && !transaction.pages.empty()
                          ? page_cache_->residency()->prepare(transaction.pages, transaction.metadata)
                          : std::shared_ptr<PageCacheResidency::Update>{};
#ifdef CROWDB_TREE_TEST_UTIL
    if (before_root_for_tests_) {
        before_root_for_tests_();
    }
#endif
    auto  complete_l1                 = prepare_complete_view(*current, *next, transaction.metadata);
    auto *replacement                 = Root::create(next, transaction.metadata);
    replacement->complete_l1          = std::move(complete_l1);
    replacement->checkpoint_allocator = checkpoint_;
    // Every allocation has succeeded. Ledger mutation and the root exchange
    // below are no-throw and contain no user callbacks or suspended work.
    pending.commit();
    auto merge_retired = merge ? merge->commit() : std::shared_ptr<const void>{};
    if (transaction.checkpoint) {
        const auto action = transaction.checkpoint->action;
        const auto token  = transaction.checkpoint->token;
        if (prepared_capture) {
            checkpoint_retired = prepared_capture->commit(token);
        }
        else {
            const bool applied = checkpoint_->pending.finish(token, action == CheckpointAction::Commit);
            assert(applied);
            (void)applied;
        }
        next->checkpoint_token    = token;
        next->checkpoint_inflight = action == CheckpointAction::Capture;
        checkpoint_->pending.copy_to(next->stats);
        if (snapshot) {
            snapshot->checkpoint_token            = token;
            snapshot->checkpoint_inflight         = true;
            snapshot->stats                       = next->stats;
            snapshot->checkpoint_pending_since_ns = checkpoint_->pending.oldest_since_ns();
            result.checkpoint_view                = snapshot;
            transaction.record->checkpoint_view   = std::move(snapshot);
        }
    }
    next->checkpoint_pending_since_ns = checkpoint_->pending.oldest_since_ns();
    auto *old                         = root_.exchange(replacement, std::memory_order_acq_rel);
    record_work(work_metrics_, TreeWorkCounter::CowMetadataCopyBytes, result.copied.bytes);
    if (residency) {
        PageCacheResidency::publish(std::move(residency));
    }
    old->checkpoint_retired        = std::move(checkpoint_retired);
    old->merge_retired             = std::move(merge_retired);
    transaction.record->commit_seq = result.commit_seq;
    transaction.record->l1_seq     = result.l1_seq;
    transaction.record->committed.store(true, std::memory_order_release);
    old->state->retirement.mark();
    epoch_.defer(old);
    return result;
}
} // namespace crowdb::tree
