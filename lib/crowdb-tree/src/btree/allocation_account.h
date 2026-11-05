// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "resource_budget.h"

namespace crowdb::tree::detail
{
template <class T> class AccountControlAllocator;

// Stable allocation owner for backend metadata that can outlive a Tree. New
// capacity is admitted against both its current Tree account and host limit.
class AllocationAccount final : public AllocationBudget
{
  public:
    static Status create(std::shared_ptr<ResourceBudget::Account> account, BudgetClass category,
                         std::shared_ptr<AllocationAccount> *out);
    // Legacy backend construction records physical capacity before a Tree
    // exists. First rebind admits every retained byte before runtime use.
    static Status      create_unbound(std::shared_ptr<AllocationAccount> *out);
    [[nodiscard]] bool claim(size_t bytes) noexcept override;
    void               release(size_t bytes) noexcept override;
    void               record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept override;

    [[nodiscard]] const void *domain() const noexcept override
    {
        return domain_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::shared_ptr<RetirementQueue> retirement_queue() const noexcept override
    {
        return retirement_;
    }

    [[nodiscard]] size_t owned_bytes() const noexcept
    {
        return allocated_.load(std::memory_order_acquire);
    }

    // Transfer already-admitted operation credit into stable backend ownership.
    // Both forms preserve host admission; a different Tree must have quota.
    bool adopt_allocated(ResourceBudget::Credit *credit) noexcept;
    bool claim_from(AllocationReservation &source, size_t bytes) noexcept;
    void release_to(AllocationReservation &source, size_t bytes) noexcept;

    // Caller excludes backend growth while reopening a Tree. Existing owners
    // may free concurrently. Refusal leaves the original account usable.
    Status rebind(std::shared_ptr<ResourceBudget::Account> account);

  private:
    template <class T> friend class AccountControlAllocator;
#ifdef CROWDB_TREE_TEST_UTIL
    friend struct AllocationAccountTestAccess;
#endif
    static constexpr uint64_t kClosed = uint64_t{1} << 63;
    static constexpr uint64_t kDirty  = uint64_t{1} << 62;
    static constexpr uint64_t kActive = kDirty - 1;
    static constexpr size_t   kKinds  = static_cast<size_t>(AllocationKind::kCount);
    enum class Step : uint8_t {
        kClosed,
        kTransferred,
        kAcquiredDrained,
        kBeforeReopen,
        kReleaseEntered,
        kWaitForWriter
    };
    using Hook = void (*)(void *, Step, size_t) noexcept;
    AllocationAccount(std::shared_ptr<ResourceBudget::Account> account, BudgetClass category);
    static Status build(std::shared_ptr<ResourceBudget::Account> account, BudgetClass category,
                        std::shared_ptr<AllocationAccount> *out);
    Status        rebind(std::shared_ptr<ResourceBudget::Account> account, Hook hook, void *context);
    void          release(size_t bytes, Hook hook, void *context) noexcept;
    void          leave() noexcept;
    bool          adopt_existing(const std::shared_ptr<ResourceBudget::Account> &source, BudgetClass category,
                                 size_t bytes) noexcept;
    void          apply_release(size_t bytes) noexcept;
    void          apply_record(size_t kind, size_t bytes, bool acquired) noexcept;
    void          reopen(Hook hook, void *context) noexcept;
    void          destroy_control(void *storage, size_t bytes, size_t alignment) noexcept;

    // Bound payload accounts use the immutable host service across same-host
    // ownership transfers. Unbound backend accounts do not observe payloads.
    const std::shared_ptr<RetirementQueue>   retirement_;
    std::shared_ptr<ResourceBudget::Account> account_;
    BudgetClass                              category_;
    std::atomic<const void *>                domain_;
    // Closing succeeds only between active account accesses. Closed releases
    // publish bounded deltas without waiting. Reopen requires no active writer
    // and no undrained delta in the same compare-and-exchange operation.
    std::atomic<uint64_t>                   ingress_{0};
    std::atomic<size_t>                     allocated_{0};
    std::array<std::atomic<size_t>, kKinds> physical_{};
    std::atomic<size_t>                     pending_release_{0};
    std::array<std::atomic<size_t>, kKinds> pending_acquired_{};
    std::array<std::atomic<size_t>, kKinds> pending_freed_{};
};
} // namespace crowdb::tree::detail
