// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "epoch_internal.h"

#include <cassert>
#include <utility>

namespace crowdb::tree
{
EpochManager::Lifetime::Lifetime(std::shared_ptr<AllocationBudget>        budget,
                                 std::shared_ptr<detail::RetirementQueue> retirement)
    : queue(std::move(retirement))
{
    collector.owner             = this;
    collector.retirement_budget = std::move(budget);
    collector.dispose           = &collect;
}

void EpochManager::Lifetime::defer(Deferred *entry) noexcept
{
    entry->epoch = live_epoch.fetch_add(1, std::memory_order_seq_cst);
    pending_count.fetch_add(1, std::memory_order_relaxed);
    auto *head = live_pending.load(std::memory_order_relaxed);
    do {
        entry->next = head;
    } while (!live_pending.compare_exchange_weak(head, entry, std::memory_order_release, std::memory_order_relaxed));
    request_collect();
}

void EpochManager::Lifetime::request_collect() noexcept
{
    if (!queue || pending_count.load(std::memory_order_acquire) == 0) {
        return;
    }
    requested.store(true, std::memory_order_release);
    if (!collecting.exchange(true, std::memory_order_acq_rel)) {
        collector_owner = weak_from_this().lock();
        assert(collector_owner);
        queue->retire(&collector);
    }
}

bool EpochManager::Lifetime::collect(detail::RetirementNode *node) noexcept
{
    auto &self = *static_cast<Collector *>(node)->owner;
    auto  keep = std::move(self.collector_owner);
    if (!self.collect_step()) {
        self.collector_owner = std::move(keep);
        return false;
    }
    self.finish_cycle();
    return true;
}

bool EpochManager::Lifetime::collect_step() noexcept
{
    if (phase == Phase::kStart) {
        requested.exchange(false, std::memory_order_acq_rel);
        batch = live_pending.exchange(nullptr, std::memory_order_acquire);
        if (batch == nullptr) {
            return true;
        }
        oldest = live_epoch.load(std::memory_order_seq_cst);
        scan   = live_participants.load(std::memory_order_acquire);
        phase  = Phase::kParticipants;
    }
    size_t work = 0;
    while (scan != nullptr && work < 64) {
        const auto epoch = scan->local_epoch.load(std::memory_order_seq_cst);
        if (epoch != 0 && epoch < oldest) {
            oldest = epoch;
        }
        scan = scan->next;
        ++work;
    }
    if (scan != nullptr) {
        return false;
    }
    phase = Phase::kDeferred;
    while (batch != nullptr && work < 64) {
        auto *entry = batch;
        batch       = entry->next;
        if (entry->epoch < oldest) {
            entry->destroy(entry);
            pending_count.fetch_sub(1, std::memory_order_release);
            freed_count.fetch_add(1, std::memory_order_relaxed);
        }
        else {
            entry->next = blocked;
            blocked     = entry;
            if (blocked_tail == nullptr) {
                blocked_tail = entry;
            }
        }
        ++work;
    }
    if (batch != nullptr) {
        return false;
    }
    return_blocked();
    phase = Phase::kStart;
    return true;
}

void EpochManager::Lifetime::return_blocked() noexcept
{
    if (blocked == nullptr) {
        return;
    }
    auto *head = live_pending.load(std::memory_order_relaxed);
    do {
        blocked_tail->next = head;
    } while (!live_pending.compare_exchange_weak(head, blocked, std::memory_order_release, std::memory_order_relaxed));
    blocked      = nullptr;
    blocked_tail = nullptr;
}

void EpochManager::Lifetime::finish_cycle() noexcept
{
    // Acquire the producer's request when its exchange observed us running.
    // Otherwise the producer observes idle and schedules the next sweep.
    collecting.exchange(false, std::memory_order_acq_rel);
    // A live reader ends the cycle instead of keeping a worker busy. Its final
    // guard exit, or a newly retired object, starts the next bounded sweep.
    if (requested.load(std::memory_order_acquire)) {
        request_collect();
    }
}

bool EpochManager::Lifetime::retire_step() noexcept
{
    assert(!collecting.load(std::memory_order_relaxed));
    if (participants == nullptr) {
        participants = live_participants.exchange(nullptr);
    }
    if (pending == nullptr) {
        pending = live_pending.exchange(nullptr);
    }
    size_t steps = 0;
    for (auto **head : {&pending, &collected}) {
        while (*head != nullptr && steps < 64) {
            auto *node = *head;
            *head      = node->next;
            node->destroy(node);
            ++steps;
        }
    }
    while (!retired.empty() && steps < 64) {
        auto entry = std::move(retired.back());
        retired.pop_back();
        entry.deleter(entry.ptr);
        ++steps;
    }
    while (participants != nullptr && steps < 64) {
        auto *next = participants->next;
        destroy_participant(participants);
        participants = next;
        ++steps;
    }
    return pending == nullptr && collected == nullptr && retired.empty() && participants == nullptr;
}

EpochManager::Lifetime::~Lifetime()
{
    while (!retire_step()) {
    }
}
} // namespace crowdb::tree
