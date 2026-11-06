// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_generation_lease_internal.h"

#include <cassert>

namespace crowdb::tree
{
TreeGenerationLease::TreeGenerationLease(detail::ReservedUnique<TreeGenerationPreparation> state)
    : state_(std::move(state))
{
}

TreeGenerationLease::~TreeGenerationLease()
{
    cancel();
}

Status TreeGenerationLease::acquire()
try {
    auto &prepared = *state_;
    auto &core     = *prepared.owner;
    auto &state    = *core.state_;
    if (prepared.held || prepared.committed || !state.begin_replacement()) {
        return Status::unavailable("generation lease is already held or Tree is closed");
    }
    prepared.held = true;
    if (core.closed() || state.publisher->capture()->generation != prepared.previous_generation) {
        cancel();
        return Status::unavailable("Tree generation changed during preparation");
    }
    // An armed proposal can still choose a distant slot, and accepted replay
    // can already be waiting for a missing slot. With ingress fenced neither
    // can obtain new gap repair. Leave the old generation usable instead of
    // waiting for work whose prerequisite this lease would prevent.
    if (state.apply_lifetime->outstanding() != 0 || state.replay_lifetime->outstanding() != 0) {
        cancel();
        return Status::unavailable("generation replacement requires completed accepted apply and replay");
    }
    state.source_rotation->drain();
    if (prepared.expected &&
        (state.publisher->capture() != prepared.expected || state.frontier->max_seen() > prepared.expected->coverage)) {
        cancel();
        return Status::unavailable("split destination changed before prepared installation");
    }
    // A completed controller can retain its backend ticket. Releasing it is
    // reversible: no source or controller has been permanently closed yet.
    state.flush->release_pending();
    state.lifetime->drain_accepted();
    state.binding->drain();
    state.cache->evict();
#ifdef CROWDB_TREE_TEST_UTIL
    if (state.before_generation_install_for_tests) {
        state.before_generation_install_for_tests();
    }
#endif
    if (core.closed()) {
        cancel();
        return Status::unavailable("Tree closed while preparing generation installation");
    }
    assert(state.publisher->idle());
    uint64_t expected = TreeCore::State::kReplacing;
    if (!state.ingress.compare_exchange_strong(expected, TreeCore::State::kPrepared, std::memory_order_acq_rel)) {
        cancel();
        return Status::unavailable("Tree closed before its prepared installation became ready");
    }
    return Status::Ok();
}

catch (const std::bad_alloc &) {
    cancel();
    return Status::resource_exhausted("generation acquisition exhausted its control credit");
}

catch (...) {
    cancel();
    return Status::internal_error("generation acquisition failed before installation");
}

Status TreeGenerationLease::commit() noexcept
{
    auto &prepared = *state_;
    auto &core     = *prepared.owner;
    auto &state    = *core.state_;
    if (prepared.committed) {
        return Status::Ok();
    }
    if (!prepared.held) {
        return Status::unavailable();
    }
    uint64_t expected = TreeCore::State::kPrepared;
    if (!state.ingress.compare_exchange_strong(expected, TreeCore::State::kReplacing, std::memory_order_acq_rel)) {
        return Status::unavailable();
    }
    assert(state.publisher->idle());
    state.sources->close();
    state.flush->close();
    state.split_handoff.swap(prepared.previous_split_handoff);
    state.split_sources.swap(prepared.previous_split_sources);
    state.sources.swap(prepared.sources);
    state.flush.swap(prepared.flush);
    state.source_rotation->rebind(*state.sources, *state.flush);
    state.source_lifecycle.swap(prepared.source_lifecycle);
    state.frontier->reset_quiescent(prepared.frontier);
    state.automatic_slot.store(prepared.frontier, std::memory_order_release);
    state.publisher->install_replacement(prepared.publication);
    prepared.committed = true;
    // Old controls stay owned by this lease until its host-side release. Do
    // not reclaim epochs or destroy old controllers on the commit boundary.
    state.finish_replacement();
    prepared.held = false;
    return Status::Ok();
}

Status TreeGenerationLease::set_split_frontier(uint64_t frontier, SourceMemoryStats memory) noexcept
{
    auto &prepared = *state_;
    if (!prepared.held || prepared.committed || !prepared.inherited || frontier < prepared.coverage) {
        return Status::invalid_argument();
    }
    auto &range = prepared.inherited->inheritance;
    if (!range.has_value() || prepared.inherited->tables.empty()) {
        return Status::invalid_argument();
    }
    if (frontier == prepared.coverage) {
        prepared.inherited->inheritance.reset();
        prepared.inherited->tables.erase(prepared.inherited->tables.begin() + 1, prepared.inherited->tables.end());
    }
    else {
        range->frontier = frontier;
        range->memory   = memory;
    }
    prepared.frontier = frontier;
    return Status::Ok();
}

void TreeGenerationLease::cancel() noexcept
{
    if (!state_->held) {
        return;
    }
    auto    &state    = *state_->owner->state_;
    uint64_t expected = TreeCore::State::kPrepared;
    // The second case is acquire failing before its host-side guard handoff.
    if (state.ingress.compare_exchange_strong(expected, TreeCore::State::kReplacing, std::memory_order_acq_rel) ||
        (expected & TreeCore::State::kReplacing) != 0) {
        state.finish_replacement();
    }
    state_->held = false;
}
} // namespace crowdb::tree
