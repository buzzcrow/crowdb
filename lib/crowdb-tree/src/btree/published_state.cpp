// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/log.h"
#include "page_cache.h"
#include "publication_generation_internal.h"
#include "publication_lane.h"

#include <algorithm>
#include <cassert>
#include <stdexcept>

namespace crowdb::tree
{
void PublishedTreeState::clear_merge_candidates(const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    detail::ReservedVector<PageReplacement> changes{detail::ReservationAllocator<PageReplacement>(metadata)};
    changes.reserve(mapping.copy_bounds().merge_candidate_pages);

    struct Context
    {
        detail::ReservedVector<PageReplacement>              &changes;
        const std::shared_ptr<detail::AllocationReservation> &metadata;
    } context{.changes = changes, .metadata = metadata};

    mapping.visit(&context, [](void *argument, uint64_t pid, const auto &page) {
        if (!page->metrics.merge_candidate) {
            return;
        }
        auto &self                           = *static_cast<Context *>(argument);
        auto  replacement                    = detail::reserved_shared<PublishedPage>(self.metadata, *page);
        replacement->metrics.merge_candidate = false;
        replacement->merge_outgoing.reset();
        self.changes.push_back({.page_id = pid, .expected = page, .replacement = std::move(replacement)});
    });
    MappingView::CopyStats copied;
    mapping                     = mapping.replace(changes, copied, metadata);
    stats.merge_candidate_pages = 0;
    stats.merge_candidate_pairs = 0;
    stats.merge_ready_pairs     = 0;
    merge_candidate_since_ns    = 0;
}

namespace
{
TreeOutcome publication_outcome(const PublicationResult &result) noexcept
{
    if (result.already_committed) {
        return TreeOutcome::Retry;
    }
    if (result.status.ok()) {
        return TreeOutcome::Success;
    }
    if (result.status.code() == Code::kUnavailable) {
        return TreeOutcome::Stale;
    }
    return TreeOutcome::Error;
}
} // namespace

PublicationCoordinator::Root::Root(std::shared_ptr<const PublishedTreeState>      value,
                                   std::shared_ptr<detail::AllocationReservation> reservation)
    : state(std::move(value)),
      metadata(std::move(reservation))
{
    destroy = [](EpochManager::Deferred *entry) noexcept { dispose(static_cast<Root *>(entry)); };
}

PublicationCoordinator::Root *
PublicationCoordinator::Root::create(std::shared_ptr<const PublishedTreeState>             value,
                                     const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    detail::ReservationAllocator<Root> allocator(metadata);
    auto                              *root = allocator.allocate(1);
    try {
        std::construct_at(root, std::move(value), metadata);
    }
    catch (...) {
        allocator.deallocate(root, 1);
        throw;
    }
    return root;
}

void PublicationCoordinator::Root::dispose(Root *root) noexcept
{
    detail::ReservationAllocator<Root> allocator(root->metadata);
    std::destroy_at(root);
    allocator.deallocate(root, 1);
}

PublicationCoordinator::~PublicationCoordinator()
{
    close();
    assert(idle());
    if (page_cache_) {
        page_cache_->residency()->clear();
    }
    auto *root = root_.load();
    root->state->retirement.mark();
    Root::dispose(root);
}

std::shared_ptr<const PublishedTreeState> PublicationCoordinator::capture() const
{
    auto guard = epoch_.enter();
    return root_.load(std::memory_order_acquire)->state;
}

TreeStatsSnapshot PublicationCoordinator::stats(const TreeResourceMetrics &resources) const
{
    const auto state           = capture();
    const auto now             = tree_metrics_now_ns();
    const auto age             = [now](uint64_t since) { return since != 0 && now >= since ? now - since : 0; };
    auto       candidate_since = state->mapping.copy_bounds().candidate_since_ns;
    if (state->merge_candidate_since_ns != 0 &&
        (candidate_since == 0 || state->merge_candidate_since_ns < candidate_since)) {
        candidate_since = state->merge_candidate_since_ns;
    }
    return {
        .instance_generation = state->generation,
        .commit_seq          = state->commit_seq,
        .sampled_at_ns       = now,
        .logical             = state->stats,
        .resources           = resources.snapshot(),
        .progress            = {.maintenance_oldest_age_ns        = age(oldest_maintenance_since_ns()),
                                .candidate_oldest_age_ns          = age(candidate_since),
                                .checkpoint_oldest_pending_age_ns = age(state->checkpoint_pending_since_ns)}
    };
}

void PublicationCoordinator::request_gc_floor(uint64_t floor) noexcept
{
    auto current = requested_gc_floor_.load(std::memory_order_relaxed);
    while (floor > current && !requested_gc_floor_.compare_exchange_weak(current, floor, std::memory_order_release,
                                                                         std::memory_order_relaxed)) {
    }
}

uint64_t PublicationCoordinator::requested_gc_floor() const noexcept
{
    return requested_gc_floor_.load(std::memory_order_acquire);
}

void PublicationCoordinator::close() noexcept
{
    closed_.store(true, std::memory_order_release);
}

bool PublicationCoordinator::idle() const noexcept
{
    return queued_.load(std::memory_order_acquire) == 0 && !running_.load(std::memory_order_acquire);
}

size_t PublicationCoordinator::reclaim()
{
    return epoch_.try_reclaim();
}

bool PublicationCoordinator::try_submit(const std::shared_ptr<PublicationRequest> &request)
{
    if (!request || closed_.load(std::memory_order_acquire) || request->submitted_.exchange(true)) {
        return false;
    }
    auto queued = ordinary_queued_.load(std::memory_order_relaxed);
    do {
        if (queued >= capacity_) {
            request->submitted_.store(false, std::memory_order_release);
            return false;
        }
    } while (!ordinary_queued_.compare_exchange_weak(queued, queued + 1, std::memory_order_acq_rel));
    enqueue(request);
    return true;
}

void PublicationCoordinator::enqueue(const std::shared_ptr<PublicationRequest> &request)
{
    queued_.fetch_add(1, std::memory_order_acq_rel);
    request->queued_owner_ = request;
    request->queue_metrics_.emplace(work_metrics_, TreeStage::PublishQueueWait, tree_metrics_now_ns());
    auto *head = incoming_.load(std::memory_order_relaxed);
    do {
        request->next_ = head;
    } while (
        !incoming_.compare_exchange_weak(head, request.get(), std::memory_order_release, std::memory_order_relaxed));
    if (!running_.exchange(true, std::memory_order_acq_rel)) {
        drain();
    }
}

void PublicationCoordinator::complete(PublicationRequest *request) noexcept
{
    auto       owner      = std::move(request->queued_owner_);
    const auto started_at = tree_metrics_now_ns();
    if (request->queue_metrics_) {
        (void)request->queue_metrics_->finish(TreeOutcome::Success, started_at);
    }
    TreeMetricsOperation commit_metrics(work_metrics_, TreeStage::PublishCommit, started_at);
    PublicationResult    result;
    try {
        result = request->reconciliation ? reconcile(*request) : commit(request->transaction);
    }
    catch (const std::bad_alloc &) {
        result.status = Status::resource_exhausted();
    }
    catch (const std::exception &) {
        result.status = Status::internal_error();
    }
    (void)commit_metrics.finish(publication_outcome(result), tree_metrics_now_ns());
    queued_.fetch_sub(1, std::memory_order_release);
    if (request->completion_slot_) {
        checkpoint_completion_.store(false, std::memory_order_release);
    }
    else if (request->lane_) {
        request->lane_->queued_.fetch_sub(1, std::memory_order_release);
    }
    else {
        ordinary_queued_.fetch_sub(1, std::memory_order_release);
    }
    // One request is submitted once. A retry uses a new request, the same
    // preallocated install record, and the same immutable replacements.
    if (request->on_done) {
        try {
            request->on_done(std::move(result));
        }
        catch (...) {
            CRB_LOG_ERROR("publication receiver threw; remaining accepted requests "
                          "will continue draining");
        }
    }
}

void PublicationCoordinator::drain() noexcept
{
    for (;;) {
        auto               *list    = incoming_.exchange(nullptr, std::memory_order_acquire);
        PublicationRequest *ordered = nullptr;
        while (list != nullptr) {
            auto *next  = list->next_;
            list->next_ = ordered;
            ordered     = list;
            list        = next;
        }
        while (ordered != nullptr) {
            auto *next = ordered->next_;
            complete(ordered);
            ordered = next;
        }
        // The acquire half observes producers that saw the busy gate before
        // release. A plain store permits a StoreLoad reorder that can miss an
        // already queued request while its producer still sees running=true.
        (void)running_.exchange(false, std::memory_order_acq_rel);
        if (incoming_.load(std::memory_order_acquire) == nullptr ||
            running_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
    }
}

} // namespace crowdb::tree
