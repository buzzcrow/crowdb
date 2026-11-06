// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/retired_shared.h"
#include "epoch_internal.h"

#include <array>
#include <utility>
#include <vector>

namespace crowdb::tree
{

namespace
{
// Stable thread identities survive eviction from the fixed participant cache.
std::atomic<uint64_t> g_epoch_thread_id{1};
} // namespace

// ── Reader hot path (lock-free) ───────────────────────────────────

EpochManager::Participant *EpochManager::participant_for_this_thread(const std::shared_ptr<AllocationBudget> &budget)
{
    // New reads of an immutable source need only its retained table owner.
    // Guards accepted before the seal still exit their original participant.
    if (source_sealed()) {
        return nullptr;
    }
    // Fixed TLS storage cannot grow with the number of retired MemTables. A
    // cache miss searches the manager's immutable list before allocating, so
    // evicting an active nested guard's cache entry cannot create a second slot.
    static thread_local std::array<std::pair<uint64_t, Participant *>, 32> cache{};
    static thread_local size_t                                             replacement = 0;
    static thread_local const uint64_t thread_id = g_epoch_thread_id.fetch_add(1, std::memory_order_relaxed);
    for (auto &e : cache) {
        if (e.first == id_) {
            return e.second;
        }
    }
    auto &participants = lifetime_->queue ? lifetime_->live_participants : participants_;
    for (auto *p = participants.load(std::memory_order_acquire); p != nullptr; p = p->next) {
        if (p->thread_id == thread_id) {
            cache[replacement++ % cache.size()] = {id_, p};
            return p;
        }
    }
    if (participants.sealed()) {
        return nullptr;
    }
    MetadataAllocator<Participant> allocator(budget ? budget : budget_);
    auto                          *p = allocator.allocate(1);
    std::construct_at(p);
    p->thread_id    = thread_id;
    p->budget       = allocator.owner();
    const auto head = participants.registration_head();
#ifdef CROWDB_TREE_TEST_UTIL
    if (append_hook_ != nullptr) {
        append_hook_(append_context_);
    }
#endif
    if (!participants.push(p, head)) {
        destroy_participant(p);
        return nullptr;
    }
    cache[replacement++ % cache.size()] = {id_, p};
    return p;
}

EpochManager::Guard EpochManager::enter(std::shared_ptr<AllocationBudget> budget)
{
    Participant *p = participant_for_this_thread(budget);
    if (p == nullptr) {
        return {};
    }
    // Reentrant: only the outermost enter publishes the entry epoch.
    if (p->nest++ == 0) {
        // seq_cst pair: publish our entry epoch before any subsequent shared load,
        // so a concurrent reclaimer either sees this epoch (keeps the object) or we
        // see its swapped-in mapping (never touch the retired object).
        auto    &epoch = lifetime_->queue ? lifetime_->live_epoch : global_epoch_;
        uint64_t e     = epoch.load(std::memory_order_seq_cst);
        p->local_epoch.store(e, std::memory_order_seq_cst);
    }
    return Guard(p, lifetime_);
}

void EpochManager::destroy_participant(Participant *participant) noexcept
{
    MetadataAllocator<Participant> allocator(participant->budget);
    std::destroy_at(participant);
    allocator.deallocate(participant, 1);
}

void EpochManager::Guard::release()
{
    if (p_ != nullptr) {
        // Only the outermost guard clears the slot. Owner-thread only, so `nest` is
        // a plain field. No reclamation here — that is writer-driven.
        if (--p_->nest == 0) {
            p_->local_epoch.store(0, std::memory_order_release);
            if (lifetime_) {
                lifetime_->request_collect();
            }
        }
        p_ = nullptr;
    }
}

// ── Writer side (reclaim_mu_) ─────────────────────────────────────

EpochManager::~EpochManager()
{
    if (!lifetime_) {
        return;
    }
    if (lifetime_->queue) {
        return;
    }
    if (active_guards() != 0) {
        lifetime_->participants = participants_.exchange(nullptr);
        lifetime_->pending      = deferred_.exchange(nullptr);
        lifetime_->collected    = std::exchange(collected_, nullptr);
        lifetime_->retired      = std::move(retired_);
        return;
    }

    // By destruction time no guards must remain. Free anything still pending,
    // then free the participant nodes. Drain in a loop (not once): a deleter can
    // retire something else on this same manager (see reclaim_locked()'s doc
    // comment), which re-populates retired_ -- keep detaching+running until
    // nothing's left, so a nested retirement's deleter still runs instead of
    // being silently dropped.
    {
        std::scoped_lock lk(reclaim_mu_);
        while (!retired_.empty() || deferred_.load() != nullptr || collected_ != nullptr) {
            reclaim_deferred_locked();
            std::vector<Retired> pending;
            pending.swap(retired_);
            for (auto &r : pending) {
                try {
                    r.deleter(r.ptr);
                }
                catch (...) { // NOLINT(bugprone-empty-catch)
                              // Destructors must not throw; swallow deleter exceptions.
                }
            }
        }
    }
    Participant *p = participants_.load(std::memory_order_acquire);
    while (p != nullptr) {
        Participant *next = p->next;
        destroy_participant(p);
        p = next;
    }
}

uint64_t EpochManager::min_active_epoch()
{
    // Oldest epoch any active reader might still be inside. seq_cst loads pair
    // with enter()'s publish for the total-order argument above.
    uint64_t min_active = global_epoch_.load(std::memory_order_seq_cst);
    for (Participant *p = participants_.load(std::memory_order_acquire); p != nullptr; p = p->next) {
        uint64_t e = p->local_epoch.load(std::memory_order_seq_cst);
        if (e != 0 && e < min_active) {
            min_active = e;
        }
    }
    return min_active;
}

void EpochManager::retire(void *ptr, Deleter deleter)
{
    if (lifetime_ && lifetime_->queue) {
        struct Callback : Deferred
        {
            void                             *value;
            Deleter                           deleter;
            std::shared_ptr<AllocationBudget> budget;
        };

        MetadataAllocator<Callback> allocator(budget_);
        auto                       *callback = allocator.allocate(1);
        std::construct_at(callback);
        callback->value   = ptr;
        callback->deleter = std::move(deleter);
        callback->budget  = budget_;
        callback->destroy = [](Deferred *entry) noexcept {
            auto                       *callback = static_cast<Callback *>(entry);
            MetadataAllocator<Callback> allocator(callback->budget);
            callback->deleter(callback->value);
            std::destroy_at(callback);
            allocator.deallocate(callback, 1);
        };
        lifetime_->defer(callback);
        return;
    }
    std::scoped_lock lk(reclaim_mu_);
    // New retirements belong to the current epoch; bump so a fresh guard entering
    // after this cannot claim to predate the retirement.
    retired_.push_back(
        {.epoch = global_epoch_.load(std::memory_order_relaxed), .ptr = ptr, .deleter = std::move(deleter)});
    global_epoch_.fetch_add(1, std::memory_order_seq_cst);
    reclaim_locked();
}

size_t EpochManager::reclaim_locked()
{
    uint64_t min_active = min_active_epoch();
    // Detach the current retired list *before* running any deleter: a
    // deleter can legitimately call retire() again on this same manager
    // -- reclaim_mu_ being recursive permits same-thread re-entry, but
    // it would still corrupt an in-progress `for (auto &r : retired_)` (the
    // nested call's own push_back/swap reallocates or replaces the very
    // vector this loop is iterating). Iterating a local, already-swapped-out
    // copy instead means a nested call only ever touches `retired_` itself
    // (empty at that point, plus whatever this loop has re-added so far),
    // never this loop's iteration state.
    std::vector<Retired> pending;
    pending.swap(retired_);
    size_t freed = 0;
    for (auto &r : pending) {
        if (r.epoch < min_active) {
            r.deleter(r.ptr);
            ++freed;
        }
        else {
            retired_.push_back(std::move(r));
        }
    }
    return freed;
}

size_t EpochManager::try_reclaim()
{
    if (lifetime_ && lifetime_->queue) {
        const auto before = lifetime_->freed_count.load(std::memory_order_relaxed);
        lifetime_->request_collect();
        return lifetime_->freed_count.load(std::memory_order_relaxed) - before;
    }
    std::scoped_lock lk(reclaim_mu_);
    return reclaim_deferred_locked() + reclaim_locked();
}

size_t EpochManager::pending_retired()
{
    if (lifetime_ && lifetime_->queue) {
        return lifetime_->pending_count.load(std::memory_order_relaxed);
    }
    std::scoped_lock lk(reclaim_mu_);
    size_t           count = retired_.size();
    // Diagnostic snapshot; tickets can only be reclaimed while this lock is held.
    for (auto *p = deferred_.load(std::memory_order_acquire); p != nullptr; p = p->next) {
        ++count;
    }
    for (auto *p = collected_; p != nullptr; p = p->next) {
        ++count;
    }
    return count;
}

void EpochManager::defer(Deferred *entry) noexcept
{
    if (lifetime_ && lifetime_->queue) {
        lifetime_->defer(entry);
        return;
    }
    auto *head = deferred_.load(std::memory_order_relaxed);
    do {
        entry->next = head;
    } while (!deferred_.compare_exchange_weak(head, entry, std::memory_order_release, std::memory_order_relaxed));
}

size_t EpochManager::reclaim_deferred_locked()
{
    auto *pending = deferred_.exchange(nullptr, std::memory_order_acquire);
    if (pending != nullptr) {
        const auto epoch = global_epoch_.fetch_add(1, std::memory_order_seq_cst);
        while (pending != nullptr) {
            auto *next     = pending->next;
            pending->epoch = epoch;
            pending->next  = collected_;
            collected_     = pending;
            pending        = next;
        }
    }
    const auto oldest = min_active_epoch();
    pending           = std::exchange(collected_, nullptr);
    size_t freed      = 0;
    while (pending != nullptr) {
        auto *next = pending->next;
        if (pending->epoch < oldest) {
            pending->destroy(pending);
            ++freed;
        }
        else {
            pending->next = collected_;
            collected_    = pending;
        }
        pending = next;
    }
    return freed;
}

size_t EpochManager::active_guards()
{
    // Threads currently inside a guard (per-thread, not per-nested-guard).
    size_t n            = 0;
    auto  &participants = lifetime_ && lifetime_->queue ? lifetime_->live_participants : participants_;
    for (Participant *p = participants.load(std::memory_order_acquire); p != nullptr; p = p->next) {
        if (p->local_epoch.load(std::memory_order_seq_cst) != 0) {
            ++n;
        }
    }
    return n;
}

} // namespace crowdb::tree
