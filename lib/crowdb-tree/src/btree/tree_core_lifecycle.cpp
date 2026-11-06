// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/checkpoint_backend.h"
#include "backend/local/local_backend_state.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
bool TreeCore::State::enter() noexcept
{
    auto value = ingress.load(std::memory_order_acquire);
    do {
        if ((value & (kClosed | kReplacing | kPrepared)) != 0) {
            return false;
        }
    } while (!ingress.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));
    return true;
}

void TreeCore::State::leave() noexcept
{
    ingress.fetch_sub(1, std::memory_order_acq_rel);
    ingress.notify_all();
}

void TreeCore::State::join_ingress() noexcept
{
    ingress.fetch_or(kClosed, std::memory_order_acq_rel);
    // A prepared capability has no running publisher. Closing revokes it
    // without waiting for an external owner to destroy its handle.
    ingress.fetch_and(~kPrepared, std::memory_order_acq_rel);
    auto value = ingress.load(std::memory_order_acquire);
    while (value != kClosed) {
        ingress.wait(value, std::memory_order_acquire);
        value = ingress.load(std::memory_order_acquire);
    }
}

bool TreeCore::State::begin_replacement() noexcept
{
    auto value = ingress.load(std::memory_order_acquire);
    do {
        if ((value & (kClosed | kReplacing | kPrepared)) != 0) {
            return false;
        }
    } while (!ingress.compare_exchange_weak(value, value | kReplacing, std::memory_order_acq_rel));
    source_rotation->stop();
    drain_ingress();
    source_rotation->drain();
    return true;
}

void TreeCore::State::drain_ingress() noexcept
{
    auto value = ingress.load(std::memory_order_acquire);
    while ((value & kParticipants) != 0) {
        ingress.wait(value, std::memory_order_acquire);
        value = ingress.load(std::memory_order_acquire);
    }
}

void TreeCore::State::finish_replacement() noexcept
{
    if ((ingress.load(std::memory_order_acquire) & kClosed) == 0) {
        source_rotation->restart();
    }
    ingress.fetch_and(~kReplacing, std::memory_order_acq_rel);
    ingress.notify_all();
}

void TreeCore::close_and_drain() noexcept
{
    uint8_t expected = 0;
    if (!state_->close_state.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
        while (expected != 2) {
            state_->close_state.wait(expected, std::memory_order_acquire);
            expected = state_->close_state.load(std::memory_order_acquire);
        }
        return;
    }
    // Closing is visible before waiting for an in-progress replacement. Its
    // release clears only kReplacing, so no install can reopen this boundary.
    state_->ingress.fetch_or(State::kClosed, std::memory_order_acq_rel);
    // A host ingress participant may await this unaccepted soft pause. Release
    // it before joining ingress; accepted apply owns its separate flush path.
    state_->source_rotation->stop();
    state_->lifetime->close();
    state_->apply_lifetime->close();
    state_->replay_lifetime->close();
    state_->join_ingress();
    // Already armed proposals can still become chosen. Their prepaid apply
    // must finish while source rotation and handoff remain available.
    state_->apply_lifetime->close_and_drain();
    state_->replay_lifetime->close_and_drain();
    state_->source_rotation->drain();
    state_->apply_progress->close();
    state_->split_handoff.reset();
    state_->split_sources.reset();
    state_->sources->close();
    state_->flush->close();
    state_->flush->release_pending();
    state_->lifetime->close_and_drain();
    state_->apply_dispatcher->close_and_drain();
    state_->publisher->close();
    state_->cache->close_and_drain();
    state_->binding->drain();
    if (state_->chunk) {
        state_->chunk->close_writer();
    }
    state_->publisher->reclaim();
    state_->sources->reclaim();
    state_->binding->account()->drain_retirements();
    state_->close_state.store(2, std::memory_order_release);
    state_->close_state.notify_all();
}

bool TreeCore::closed() const noexcept
{
    return (state_->ingress.load(std::memory_order_acquire) & State::kClosed) != 0;
}

const Config &TreeCore::config() const noexcept
{
    return state_->config;
}

const std::shared_ptr<TreeRuntimeBinding> &TreeCore::binding() const noexcept
{
    return state_->binding;
}

const std::shared_ptr<ApplyDispatcher> &TreeCore::cpu_progress() const noexcept
{
    return state_->apply_dispatcher;
}

const std::shared_ptr<SharedPageCache> &TreeCore::cache() const noexcept
{
    return state_->cache;
}

const std::shared_ptr<LocalBackendState> &TreeCore::local_backend() const noexcept
{
    return state_->local;
}

const std::shared_ptr<CheckpointBackend> &TreeCore::chunk_backend() const noexcept
{
    return state_->chunk;
}

uint64_t TreeCore::durable_coverage() const noexcept
{
    if (state_->chunk) {
        return state_->chunk->durable_coverage.load(std::memory_order_acquire);
    }
    return state_->local ? state_->local->checkpoints->durable_coverage() : 0;
}

uint64_t TreeCore::snapshot_sequence() const noexcept
{
    if (state_->chunk) {
        return state_->chunk->snapshot_sequence.load(std::memory_order_acquire);
    }
    return state_->local ? state_->local->checkpoints->snapshot_sequence() : 0;
}

AsyncPageStore *TreeCore::async_backend() const noexcept
{
    return state_->backend.get();
}

PageLoadLimits TreeCore::load_limits() const noexcept
{
    return state_->loads;
}

PublishedReadLimits TreeCore::read_limits() const noexcept
{
    return state_->reads;
}

std::shared_ptr<const PublishedTreeState> TreeCore::capture() const
{
    return state_->publisher->capture();
}

PublicationCoordinator &TreeCore::publisher() const noexcept
{
    return *state_->publisher;
}

TreeOperationLifetime &TreeCore::operation_lifetime() const noexcept
{
    return *state_->lifetime;
}

void TreeCore::request_gc_floor(uint64_t floor) noexcept
{
    State::Ingress admission(*state_);
    if (admission.owner != nullptr) {
        state_->publisher->request_gc_floor(floor);
    }
}

uint64_t TreeCore::requested_gc_floor() const noexcept
{
    return state_->publisher->requested_gc_floor();
}

uint64_t TreeCore::contiguous_slot() const noexcept
{
    return state_->frontier->contiguous().load(std::memory_order_acquire);
}

uint64_t TreeCore::max_seen_slot() const noexcept
{
    return state_->frontier->max_seen();
}

size_t TreeCore::memtable_count() const
{
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return 0;
    }
    size_t result = 0;
    for (const auto &table : state_->sources->catalog()->tables) {
        result += table->count();
    }
    return result;
}

size_t TreeCore::frozen_table_count() const
{
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return 0;
    }
    const auto catalog = state_->sources->catalog();
    return catalog->tables.size() - 1;
}
} // namespace crowdb::tree
