// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_internal.h"
#include "btree/tree_core_split_wait.h"

namespace crowdb::tree
{
Status TreeCore::begin_split_view(uint64_t *generation, uint64_t *journal_frontier)
try {
    if (generation == nullptr || journal_frontier == nullptr) {
        return Status::invalid_argument("split view requires output parameters");
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("split ingress closed");
    }
    detail::SplitGate gate(state_->split_busy);
    if (!gate.held()) {
        return Status::unavailable("split administration is busy");
    }
    if (state_->split_sources && state_->split_sources->active) {
        return Status::invalid_argument("a split source view is already active");
    }
    auto status = state_->capture_split_sources(*this, contiguous_slot());
    if (!status.ok()) {
        return status;
    }
    state_->split_sources->active = true;
    *generation                   = state_->split_sources->session;
    *journal_frontier             = state_->split_sources->frontier;
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("split source capture allocation failed");
}

Status TreeCore::release_split_view(uint64_t generation)
{
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("split ingress closed");
    }
    detail::SplitGate gate(state_->split_busy);
    if (!gate.held()) {
        return Status::unavailable("split administration is busy");
    }
    if (!state_->split_sources || !state_->split_sources->active || generation == 0 ||
        generation != state_->split_sources->session) {
        return Status::invalid_argument("split source generation is not active");
    }
    state_->split_sources.reset();
    return Status::Ok();
}
} // namespace crowdb::tree
