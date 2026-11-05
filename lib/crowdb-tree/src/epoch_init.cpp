// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/retired_shared.h"
#include "epoch_internal.h"

namespace crowdb::tree
{
namespace
{
std::atomic<uint64_t> next_epoch_identity{1};
}

EpochManager::EpochManager(std::shared_ptr<AllocationBudget> budget, bool enabled)
    : budget_(std::move(budget)),
      id_(next_epoch_identity.fetch_add(1, std::memory_order_relaxed))
{
    if (!enabled) {
        return;
    }
    memtable_allocation_ = std::allocate_shared<AllocationCounter>(MetadataAllocator<AllocationCounter>(budget_), 0);
    if (auto queue = budget_ ? budget_->retirement_queue() : nullptr) {
        lifetime_ = detail::retired_shared<Lifetime>(budget_, queue, budget_, queue);
    }
    else {
        lifetime_ = std::allocate_shared<Lifetime>(MetadataAllocator<Lifetime>(budget_));
    }
}

size_t EpochManager::infrastructure_allocation_bound() noexcept
{
    return sizeof(EpochManager) + detail::shared_allocation_bound<Lifetime>() +
           detail::shared_allocation_bound<AllocationCounter>();
}

bool EpochManager::ParticipantList::push(Participant *participant, uintptr_t head) noexcept
{
    while ((head & 1) == 0) {
        participant->next = pointer(head);
        if (head_.compare_exchange_weak(head, reinterpret_cast<uintptr_t>(participant), std::memory_order_release,
                                        std::memory_order_acquire)) {
            return true;
        }
    }
    return false;
}

bool EpochManager::source_sealed() const noexcept
{
    return lifetime_ && lifetime_->queue && lifetime_->live_participants.sealed();
}

void EpochManager::seal_source() noexcept
{
    if (lifetime_ && lifetime_->queue) {
        (void)lifetime_->live_participants.seal();
    }
}
} // namespace crowdb::tree
