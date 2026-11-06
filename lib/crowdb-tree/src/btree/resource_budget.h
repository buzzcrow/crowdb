// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/resource_usage.h"
#include "stdexec_adapter.h"
#include "tree_wait_metrics.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace crowdb::tree::detail
{
class AllocationReservation;
class AllocationAccount;
class RetirementQueue;

enum class BudgetClass : uint8_t { kHandoff, kRouting, kReclaim, kAdmittedWriter, kGapRepair, kIo, kCount };

// Partitions do not borrow from one another: ordinary handoff cannot consume
// the reserve needed to route, reclaim, or apply already admitted writes.
// Values are bytes except kIo, whose unit is simultaneous I/O operations.
class ResourceBudget
{
    struct State;

  public:
    static constexpr size_t kClasses = static_cast<size_t>(BudgetClass::kCount);
    using Limits                     = std::array<size_t, kClasses>;

    struct Snapshot
    {
        Limits   reserved{};
        Limits   allocated{};
        uint64_t waiters   = 0;
        uint64_t grants    = 0;
        uint64_t cancelled = 0;
        uint64_t wait_ns   = 0;
    };

    struct AccountSnapshot
    {
        Limits                                                          reserved{};
        Limits                                                          allocated{};
        std::array<size_t, static_cast<size_t>(AllocationKind::kCount)> physical{};
    };

    class Account
    {
      public:
        // Independent relaxed samples; these counters never govern admission.
        [[nodiscard]] AccountSnapshot       snapshot() const noexcept;
        [[nodiscard]] ResourceUsageSnapshot resource_usage() const noexcept;

        [[nodiscard]] uint64_t scope_id() const noexcept
        {
            return scope_id_;
        }

        void drain_retirements() const noexcept;
        // Bootstrap only, before this account or its binding is published.
        void set_work_metrics(const std::shared_ptr<TreeWorkMetrics> &metrics) noexcept;

        [[nodiscard]] std::shared_ptr<TreeWorkMetrics> work_metrics() const noexcept
        {
            return retirement_metrics_.lock();
        }

        void set_resource_metrics(const std::shared_ptr<TreeResourceMetrics> &metrics) noexcept;
        [[nodiscard]] std::shared_ptr<TreeResourceMetrics> resource_metrics() const noexcept;
#ifdef CROWDB_TREE_TEST_UTIL
        [[nodiscard]] size_t retirements_pending_for_tests() const noexcept
        {
            return retirement_pending_.load(std::memory_order_acquire);
        }
#endif

      private:
        friend class AllocationReservation;
        friend class AllocationAccount;
        friend class ResourceBudget;
        friend struct State;
        explicit Account(std::shared_ptr<State> state, Limits limits);
        std::shared_ptr<State>                    state_;
        std::weak_ptr<RetirementQueue>            retirement_;
        std::weak_ptr<TreeWorkMetrics>            retirement_metrics_;
        std::weak_ptr<TreeResourceMetrics>        resource_metrics_;
        mutable std::atomic<size_t>               retirement_pending_{0};
        Limits                                    limits_;
        size_t                                    memory_limit_ = 0;
        std::atomic<size_t>                       memory_allocated_{0};
        uint64_t                                  scope_id_;
        std::array<std::atomic<size_t>, kClasses> used_{};
        std::array<std::atomic<size_t>, kClasses> reserved_{};
        std::array<std::atomic<size_t>, kClasses> allocated_{};

        struct Observation
        {
            std::atomic<uint64_t> denied_tree{0}, denied_host{0};
            std::atomic<uint64_t> waits{0}, wait_ns{0}, waiters{0};
        };

        std::array<Observation, kClasses>                                            observation_{};
        std::array<std::atomic<size_t>, static_cast<size_t>(AllocationKind::kCount)> physical_{};
    };

    class Credit
    {
      public:
        Credit() = default;
        ~Credit();
        Credit(Credit &&other) noexcept;
        Credit &operator=(Credit &&other) noexcept;
        Credit(const Credit &)            = delete;
        Credit &operator=(const Credit &) = delete;

        [[nodiscard]] size_t amount() const noexcept
        {
            return amount_;
        }

        [[nodiscard]] bool allocated() const noexcept
        {
            return allocated_;
        }

        [[nodiscard]] bool belongs_to(const std::shared_ptr<Account> &account, BudgetClass category) const noexcept
        {
            return account_ == account && category_ == static_cast<size_t>(category);
        }

        // Transfer part of a reservation into allocation ownership, without
        // changing admission usage or charging the same bytes a second time.
        [[nodiscard]] std::optional<Credit> allocate(size_t amount) noexcept;
        [[nodiscard]] std::optional<Credit> split(size_t amount) noexcept;
        void                                release() noexcept;
        // Call only after a raw allocation succeeds. The classified token
        // follows moves/splits and subtracts its bytes on final release.
        [[nodiscard]] bool mark_allocation(AllocationKind kind) noexcept;

      private:
        friend class AllocationReservation;
        friend class AllocationAccount;
        friend class ResourceBudget;
        friend struct State;
        Credit(std::shared_ptr<Account> account, size_t category, size_t amount, bool allocated);
        std::shared_ptr<Account> account_;
        size_t                   category_  = 0;
        size_t                   amount_    = 0;
        bool                     allocated_ = false;
        AllocationKind           kind_      = AllocationKind::kUnknown;
    };

    class Request;

    // A fixed host waiter slot acquired before an operation can be chosen.
    // One request at a time may borrow it; ownership outlives every borrower.
    class Registration
    {
      public:
        Registration() = default;
        ~Registration();
        Registration(Registration &&other) noexcept;
        Registration &operator=(Registration &&other) noexcept;
        Registration(const Registration &)            = delete;
        Registration &operator=(const Registration &) = delete;

      private:
        friend class ResourceBudget;
        friend class Request;

        Registration(std::shared_ptr<State> state, size_t index) : state_(std::move(state)), index_(index)
        {
        }

        void                   reset() noexcept;
        std::shared_ptr<State> state_;
        size_t                 index_ = 0;
    };

    [[nodiscard]] static std::optional<Registration> try_register(const std::shared_ptr<Account> &account,
                                                                  BudgetClass                     category) noexcept;

    class Request
    {
      public:
        // Unaccepted soft work may wait for reservations to finish, but cannot
        // require live allocations that only this same work could retire.
        enum class WaitPolicy : uint8_t { AnyRelease, ReservationsOnly };

        Request(std::shared_ptr<Account> account, BudgetClass category, size_t amount,
                const Registration *registration = nullptr, WaitPolicy policy = WaitPolicy::AnyRelease);
        ~Request();
        Request(const Request &)            = delete;
        Request &operator=(const Request &) = delete;

        // Request and connected operation must stay alive until terminal.
        [[nodiscard]] CallbackSender        sender() noexcept;
        void                                request_stop() noexcept;
        [[nodiscard]] std::optional<Credit> take_credit() noexcept;

      private:
        friend struct State;
        Request() = default;
        static void              submit(void *source, CallbackComplete complete, void *context);
        std::shared_ptr<Account> account_;
        size_t                   category_     = 0;
        size_t                   amount_       = 0;
        const Registration      *registration_ = nullptr;
        WaitPolicy               policy_       = WaitPolicy::AnyRelease;
        std::optional<Credit>    credit_;
        std::atomic<bool>        stopped_{false};
        std::atomic<bool>        pending_{false};
        std::atomic_flag         submitted_ = ATOMIC_FLAG_INIT;
        CallbackComplete         complete_  = nullptr;
        void                    *context_   = nullptr;
        uint64_t                 queued_at_ = 0;
        TreeWaitSpan             wait_span_;
    };

    // Waiter slots are partitioned too, so handoff waiters cannot exhaust the
    // registrations required by routing/reclaim progress.
    ResourceBudget(Limits limits, size_t max_waiters_per_class,
                   const std::shared_ptr<AllocationBudget> &bootstrap = {});
    ~ResourceBudget();
    ResourceBudget(const ResourceBudget &)            = delete;
    ResourceBudget &operator=(const ResourceBudget &) = delete;

    [[nodiscard]] std::shared_ptr<Account> make_account(Limits limits);
    // Charge both the account and its shared control block to host metadata.
    [[nodiscard]] std::shared_ptr<Account> make_account(Limits limits, const std::shared_ptr<AllocationBudget> &budget);
    [[nodiscard]] static std::optional<Credit> try_reserve(const std::shared_ptr<Account> &account,
                                                           BudgetClass category, size_t amount) noexcept;
    [[nodiscard]] static size_t capacity(const std::shared_ptr<Account> &account, BudgetClass category) noexcept;
    // Planning hint from admission usage, including both host and Tree limits.
    // A subsequent reservation must still claim the capacity atomically.
    [[nodiscard]] static size_t available(const std::shared_ptr<Account> &account, BudgetClass category) noexcept;
    [[nodiscard]] static bool   memory_exhausted(const std::shared_ptr<Account> &account) noexcept;
    [[nodiscard]] static bool   accepting(const std::shared_ptr<Account> &account) noexcept;
    // Stop new admission and cancel waiting requests. Outstanding credit
    // remains charged until its actual owner releases it.
    // Bootstrap-only before publishing the host runtime. New accounts copy
    // this weak association; the queue never retains its executor.
    void                                set_retirement_queue(const std::shared_ptr<RetirementQueue> &queue);
    void                                close() noexcept;
    [[nodiscard]] Snapshot              snapshot() const noexcept;
    [[nodiscard]] ResourceUsageSnapshot resource_usage() const noexcept;

  private:
    friend class AllocationAccount;
    static bool claim_allocation(const std::shared_ptr<Account> &account, BudgetClass category, size_t bytes) noexcept;
    static void release_allocation(const std::shared_ptr<Account> &account, BudgetClass category,
                                   size_t bytes) noexcept;
    static const void *allocation_domain(const std::shared_ptr<Account> &account) noexcept;
    static bool
    transfer_allocation(const std::shared_ptr<Account> &from, const std::shared_ptr<Account> &to, BudgetClass category,
                        size_t                                                                 bytes,
                        const std::array<size_t, static_cast<size_t>(AllocationKind::kCount)> &physical) noexcept;
    static void            record_allocation(const std::shared_ptr<Account> &account, AllocationKind kind, size_t bytes,
                                             bool acquired) noexcept;
    std::shared_ptr<State> state_;
};

} // namespace crowdb::tree::detail
