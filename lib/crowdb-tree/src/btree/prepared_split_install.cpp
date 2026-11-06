// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "prepared_split_internal.h"

namespace crowdb::tree
{
Status PreparedSplitOverlay::prepare(uint64_t replayed_prefix)
try {
    auto   &self     = *state_;
    uint8_t expected = State::Armed;
    if (detail::FlushRuntime::on_worker_thread() ||
        !self.phase.compare_exchange_strong(expected, State::Preparing, std::memory_order_acq_rel)) {
        return Status::unavailable("split capability is not awaiting background preparation");
    }
    const auto fail = [&](Status status) {
        self.tail->expire();
        self.domain->detach(self.subscription);
        if (self.lease) {
            self.lease->cancel();
        }
        self.phase.store(State::Failed, std::memory_order_release);
        return status;
    };
    if (replayed_prefix < self.start || replayed_prefix == UINT64_MAX || self.tail->expired() ||
        self.destination->contiguous_slot() != replayed_prefix) {
        return fail(Status::invalid_argument("child replay must cover the armed observer start"));
    }
    auto status = self.destination->flush();
    if (!status.ok()) {
        return fail(status);
    }
    const auto expected_child = self.destination->capture();
    const auto base           = self.destination->publisher().capture_complete();
    if (!base || base->coverage != replayed_prefix || expected_child->coverage != replayed_prefix ||
        self.destination->max_seen_slot() != replayed_prefix || base->generation != expected_child->generation ||
        base->pending_handoff || base->flush_admission_owner != 0 || self.tail->expired()) {
        return fail(Status::unavailable("child prefix changed before split preparation"));
    }
    // Complete L1 may be the current root with an empty active source. Only
    // after flush and the exact P checks may its immutable mapping be copied
    // without L0. Lease acquisition rechecks the expected root and drains any
    // admitted writer before publication, rejecting a child that advanced.
    auto candidate       = detail::reserved_shared<PublishedTreeState>(self.metadata, *base);
    auto empty_sources   = detail::reserved_shared<PublishedSources>(self.metadata, self.metadata);
    empty_sources->floor = replayed_prefix;
    candidate->sources   = std::move(empty_sources);
    auto inherited       = detail::reserved_shared<PublishedSources>(self.metadata, self.metadata);
    inherited->floor     = replayed_prefix;
    inherited->tables.push_back(self.tail->table());
    // Private placeholder only. The preallocated replacement is unreachable
    // by readers until try_install fills its exact F after freezing the delta.
    inherited->inheritance = PublishedSources::Inheritance{.owner_identity = self.source_identity,
                                                           .session        = self.session->session,
                                                           .frontier       = replayed_prefix + 1,
                                                           .local_count    = 0};
    SplitGeneration split{.expected = expected_child, .inherited = std::move(inherited)};
    status = TreeGenerationLease::prepare(*self.destination, std::move(candidate), &split, &self.lease);
    if (!status.ok()) {
        return fail(status);
    }
    status = self.lease->acquire();
    if (!status.ok()) {
        return fail(status);
    }
    self.replayed = replayed_prefix;
    self.phase.store(State::Ready, std::memory_order_release);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    state_->tail->expire();
    state_->domain->detach(state_->subscription);
    if (state_->lease) {
        state_->lease->cancel();
    }
    state_->phase.store(State::Failed, std::memory_order_release);
    return Status::resource_exhausted("split preparation exhausted prepaid storage");
}
catch (...) {
    state_->tail->expire();
    state_->domain->detach(state_->subscription);
    if (state_->lease) {
        state_->lease->cancel();
    }
    state_->phase.store(State::Failed, std::memory_order_release);
    return Status::internal_error("split preparation failed before installation");
}

Status PreparedSplitOverlay::try_install(uint64_t frontier, InstalledSplitOverlay *out) noexcept
{
    auto &self = *state_;
    if (out == nullptr) {
        return Status::invalid_argument();
    }
    uint8_t expected = State::Ready;
    if (!self.phase.compare_exchange_strong(expected, State::Installing, std::memory_order_acq_rel)) {
        if (expected == State::Installed && frontier == self.installed) {
            *out = {.generation = self.session->session, .frontier = self.installed};
            return Status::Ok();
        }
        return Status::unavailable();
    }
    TreeCore::State::Ingress parent(*self.source->state_);
    if (parent.owner == nullptr || self.source->state_->sources->tail_domain() != self.domain ||
        frontier < self.replayed || frontier > self.source->contiguous_slot() || self.tail->expired()) {
        self.phase.store(State::Failed, std::memory_order_release);
        return Status::unavailable();
    }
    if (!self.tail->freeze()) {
        self.phase.store(self.tail->expired() ? State::Failed : State::Ready, std::memory_order_release);
        return Status::unavailable();
    }
    self.domain->detach(self.subscription);
    auto status = self.lease->set_split_frontier(frontier, self.tail->table()->source_stats());
    if (status.ok()) {
        status = self.lease->commit();
    }
    if (!status.ok()) {
        self.phase.store(State::Failed, std::memory_order_release);
        return status;
    }
    self.session->tables[0]            = self.tail->table();
    self.session->table_count          = 1;
    self.session->intervals[0]         = {.floor = self.replayed, .frontier = frontier};
    self.session->interval_count       = 1;
    self.session->frontier             = frontier;
    self.session->active               = true;
    self.previous_session              = std::move(self.source->state_->split_sources);
    self.source->state_->split_sources = self.session;
    self.installed                     = frontier;
    self.destination->state_->split_busy.clear(std::memory_order_release);
    self.source->state_->split_busy.clear(std::memory_order_release);
    self.destination_gate = false;
    self.source_gate      = false;
    self.phase.store(State::Installed, std::memory_order_release);
    *out = {.generation = self.session->session, .frontier = frontier};
    return Status::Ok();
}

std::optional<uint64_t> PreparedSplitOverlay::committed_frontier() const noexcept
{
    if (state_->phase.load(std::memory_order_acquire) == State::Installed) {
        return state_->installed;
    }
    return {};
}
} // namespace crowdb::tree
