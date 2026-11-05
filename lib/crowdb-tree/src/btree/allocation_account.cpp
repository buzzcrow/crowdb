// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account.h"

#include <algorithm>
#include <cassert>
#include <limits>

namespace crowdb::tree::detail
{
AllocationAccount::AllocationAccount(std::shared_ptr<ResourceBudget::Account> account, BudgetClass category)
    : retirement_(account ? account->retirement_.lock() : nullptr),
      account_(std::move(account)),
      category_(category),
      domain_(ResourceBudget::allocation_domain(account_))
{
}

void AllocationAccount::leave() noexcept
{
    const auto old = ingress_.fetch_sub(1, std::memory_order_acq_rel);
    if ((old & kClosed) != 0) {
        ingress_.notify_one();
    }
}

bool AllocationAccount::claim(size_t bytes) noexcept
{
    const auto entered = ingress_.fetch_add(1, std::memory_order_acq_rel);
    if ((entered & kClosed) != 0) {
        leave();
        return false;
    }
    auto current = allocated_.load(std::memory_order_relaxed);
    while (bytes <= std::numeric_limits<size_t>::max() - current) {
        if (!allocated_.compare_exchange_weak(current, current + bytes, std::memory_order_acq_rel)) {
            continue;
        }
        const bool accepted = !account_ || ResourceBudget::claim_allocation(account_, category_, bytes);
        if (!accepted) {
            allocated_.fetch_sub(bytes, std::memory_order_relaxed);
        }
        leave();
        return accepted;
    }
    leave();
    return false;
}

void AllocationAccount::apply_release(size_t bytes) noexcept
{
    if (bytes == 0) {
        return;
    }
    [[maybe_unused]] const auto old = allocated_.fetch_sub(bytes, std::memory_order_relaxed);
    assert(old >= bytes);
    ResourceBudget::release_allocation(account_, category_, bytes);
}

void AllocationAccount::release(size_t bytes) noexcept
{
    release(bytes, nullptr, nullptr);
}

void AllocationAccount::release(size_t bytes, Hook hook, void *context) noexcept
{
    const auto entered = ingress_.fetch_add(1, std::memory_order_acq_rel);
    if (hook != nullptr) {
        hook(context, Step::kReleaseEntered, 0);
    }
    if ((entered & kClosed) != 0) {
        pending_release_.fetch_add(bytes, std::memory_order_relaxed);
        ingress_.fetch_or(kDirty, std::memory_order_release);
    }
    else {
        apply_release(bytes);
    }
    leave();
}

void AllocationAccount::apply_record(size_t kind, size_t bytes, bool acquired) noexcept
{
    if (bytes == 0) {
        return;
    }
    if (acquired) {
        physical_[kind].fetch_add(bytes, std::memory_order_relaxed);
    }
    else {
        [[maybe_unused]] const auto old = physical_[kind].fetch_sub(bytes, std::memory_order_relaxed);
        assert(old >= bytes);
    }
    ResourceBudget::record_allocation(account_, static_cast<AllocationKind>(kind), bytes, acquired);
}

void AllocationAccount::record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept
{
    const auto index = static_cast<size_t>(kind);
    if (kind == AllocationKind::kUnknown || index >= kKinds) {
        return;
    }
    const auto entered = ingress_.fetch_add(1, std::memory_order_acq_rel);
    if ((entered & kClosed) != 0) {
        (acquired ? pending_acquired_[index] : pending_freed_[index]).fetch_add(bytes, std::memory_order_relaxed);
        ingress_.fetch_or(kDirty, std::memory_order_release);
    }
    else {
        apply_record(index, bytes, acquired);
    }
    leave();
}

void AllocationAccount::reopen(Hook hook, void *context) noexcept
{
    for (;;) {
        ingress_.fetch_and(~kDirty, std::memory_order_acq_rel);
        for (size_t kind = 1; kind < kKinds; ++kind) {
            apply_record(kind, pending_acquired_[kind].exchange(0, std::memory_order_acq_rel), true);
            if (hook != nullptr) {
                hook(context, Step::kAcquiredDrained, kind);
            }
            const auto freed = pending_freed_[kind].exchange(0, std::memory_order_acq_rel);
            const auto known = physical_[kind].load(std::memory_order_relaxed);
            apply_record(kind, std::min(freed, known), false);
            if (freed > known) {
                // An acquire record can arrive after its exchange but before
                // the matching free exchange. Keep the unmatched free until
                // the next pass instead of exposing an unsigned underflow.
                pending_freed_[kind].fetch_add(freed - known, std::memory_order_relaxed);
                ingress_.fetch_or(kDirty, std::memory_order_release);
            }
        }
        apply_release(pending_release_.exchange(0, std::memory_order_acq_rel));
        if (hook != nullptr) {
            hook(context, Step::kBeforeReopen, 0);
        }
        uint64_t expected = kClosed;
        if (ingress_.compare_exchange_strong(expected, 0, std::memory_order_release, std::memory_order_acquire)) {
            return;
        }
        if ((expected & kActive) != 0) {
            if (hook != nullptr) {
                hook(context, Step::kWaitForWriter, 0);
            }
            ingress_.wait(expected, std::memory_order_acquire);
        }
    }
}

Status AllocationAccount::rebind(std::shared_ptr<ResourceBudget::Account> account)
{
    return rebind(std::move(account), nullptr, nullptr);
}

Status AllocationAccount::rebind(std::shared_ptr<ResourceBudget::Account> account, Hook hook, void *context)
{
    const auto *current_domain = domain();
    if (!account || (current_domain != nullptr && ResourceBudget::allocation_domain(account) != current_domain)) {
        return Status::invalid_argument("backend metadata belongs to another host; reopen the backend object");
    }
    uint64_t idle = 0;
    if (!ingress_.compare_exchange_strong(idle, kClosed, std::memory_order_acq_rel)) {
        return Status::unavailable("backend metadata allocation is active");
    }
    const auto *closed_domain = domain();
    if (closed_domain != nullptr && ResourceBudget::allocation_domain(account) != closed_domain) {
        reopen(nullptr, nullptr);
        return Status::invalid_argument("backend metadata belongs to another host; reopen the backend object");
    }
    if (hook != nullptr) {
        hook(context, Step::kClosed, 0);
    }
    std::array<size_t, kKinds> physical{};
    for (size_t kind = 0; kind < kKinds; ++kind) {
        physical[kind] = physical_[kind].load(std::memory_order_relaxed);
    }
    const bool moved = ResourceBudget::transfer_allocation(account_, account, category_, allocated_.load(), physical);
    if (moved) {
        account_ = std::move(account);
        domain_.store(ResourceBudget::allocation_domain(account_), std::memory_order_release);
    }
    if (hook != nullptr) {
        hook(context, Step::kTransferred, 0);
    }
    reopen(hook, context);
    return moved ? Status::Ok() : Status::resource_exhausted("new Tree cannot adopt backend metadata capacity");
}
} // namespace crowdb::tree::detail
