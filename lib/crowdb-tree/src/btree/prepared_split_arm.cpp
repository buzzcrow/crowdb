// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "prepared_split_internal.h"

namespace crowdb::tree
{
PreparedSplitOverlay::PreparedSplitOverlay(std::shared_ptr<State> state) : state_(std::move(state))
{
}

PreparedSplitOverlay::~PreparedSplitOverlay()
{
    auto &self = *state_;
    if (self.tail) {
        self.tail->expire();
    }
    if (self.domain) {
        self.domain->detach(self.subscription);
    }
    if (self.metadata) {
        self.metadata->seal();
    }
    self.lease.reset();
    if (self.destination_gate) {
        self.destination->state_->split_busy.clear(std::memory_order_release);
    }
    if (self.source_gate) {
        self.source->state_->split_busy.clear(std::memory_order_release);
    }
}

Status PreparedSplitOverlay::arm(TreeCore &source, TreeCore &destination, SplitOverlayLimits limits,
                                 std::shared_ptr<PreparedSplitOverlay> *out, uint64_t *start_frontier)
try {
    if (&source == &destination || source.config().runtime != destination.config().runtime || limits.max_records == 0 ||
        limits.max_bytes == 0 || out == nullptr || *out || start_frontier == nullptr ||
        detail::FlushRuntime::on_worker_thread()) {
        return Status::invalid_argument("split arm requires distinct same-host trees and a finite tail");
    }
    const auto bound = detail::AllocationBound()
                           .add(SourceTail::allocation_bound(limits.max_records, limits.max_bytes))
                           .add(SourceTailDomain::registration_bound())
                           .add(detail::shared_allocation_bound<State>())
                           .add(detail::shared_allocation_bound<PreparedSplitOverlay>())
                           .add(detail::shared_allocation_bound<SplitSourceSession>())
                           .add(PublishedSources::allocation_bound())
                           .add(PublicationCoordinator::complete_view_reserve_bytes())
                           .add(4 * sizeof(std::shared_ptr<MemTable>))
                           .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                           .value();
    if (!bound) {
        return Status::resource_exhausted("split tail bound overflow");
    }
    auto reserved =
        detail::ResourceBudget::try_reserve(destination.binding()->account(), detail::BudgetClass::kRouting, *bound);
    if (!reserved) {
        return Status::resource_exhausted("split tail credit unavailable");
    }
    auto metadata      = detail::AllocationReservation::create(std::move(*reserved));
    auto state         = detail::reserved_shared<State>(metadata);
    state->source      = source.shared_from_this();
    state->destination = destination.shared_from_this();
    state->metadata    = metadata;
    state->tail        = detail::reserved_shared<SourceTail>(metadata, metadata, limits.max_records, limits.max_bytes);
    auto token         = detail::reserved_construct<PreparedSplitOverlay>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) PreparedSplitOverlay(state); });
    {
        TreeCore::State::Ingress parent(*source.state_);
        TreeCore::State::Ingress child(*destination.state_);
        if (parent.owner == nullptr || child.owner == nullptr) {
            return Status::unavailable("split ingress closed");
        }
        state->domain          = source.state_->sources->tail_domain();
        state->source_identity = source.state_->sources->identity();
        state->source_gate     = !source.state_->split_busy.test_and_set(std::memory_order_acquire);
        if (!state->source_gate) {
            return Status::unavailable("source split administration busy");
        }
        state->destination_gate = !destination.state_->split_busy.test_and_set(std::memory_order_acquire);
        if (!state->destination_gate) {
            return Status::unavailable("destination split administration busy");
        }
        if ((source.state_->split_sources && source.state_->split_sources->active) ||
            source.state_->split_sequence == UINT64_MAX) {
            return Status::unavailable("source already has a fixed split prefix");
        }
        state->session                           = detail::reserved_shared<SplitSourceSession>(metadata);
        state->session->generation               = source.capture()->generation;
        state->session->session                  = ++source.state_->split_sequence;
        state->session->limits                   = source.state_->sources->catalog()->active_budget->options();
        state->session->limits.max_entries       = limits.max_records;
        state->session->limits.max_gap_entries   = 0;
        state->session->limits.max_key_bytes     = limits.max_bytes;
        state->session->limits.max_gap_key_bytes = 0;
        auto status                              = state->domain->arm(state->tail, metadata, &state->subscription);
        if (!status.ok()) {
            return status;
        }
    }
    // No admission pause: only pre-registration writers belong to this epoch
    // barrier. They release their observation guard after frontier.complete.
    state->subscription->wait();
    TreeCore::State::Ingress parent(*source.state_);
    if (parent.owner == nullptr || source.state_->sources->tail_domain() != state->domain || state->tail->expired()) {
        return Status::unavailable("split tail expired before its starting frontier");
    }
    // Pre-registration out-of-order writes may lie above the contiguous
    // frontier. The private child must replay this complete high-watermark.
    state->start    = source.max_seen_slot();
    *start_frontier = state->start;
    *out            = std::move(token);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("split arm allocation exhausted its reservation");
}
} // namespace crowdb::tree
