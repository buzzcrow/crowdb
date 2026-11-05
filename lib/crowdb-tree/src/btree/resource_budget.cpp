// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/resource_budget.h"

#include "btree/allocation_reservation.h"
#include "tree_work_counters.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace crowdb::tree::detail
{
namespace
{
std::atomic<uint64_t> next_scope_id{1};

uint64_t budget_now_ns() noexcept
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

bool reserve_amount(std::atomic<size_t> &used, size_t limit, size_t amount) noexcept
{
    auto current = used.load(std::memory_order_relaxed);
    while (current <= limit && amount <= limit - current) {
        if (used.compare_exchange_weak(current, current + amount, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}
} // namespace

struct ResourceBudget::State
{
    State(Limits capacity, size_t max_waiters, const std::shared_ptr<AllocationBudget> &bootstrap)
        : limits(capacity),
          waiter_capacity(max_waiters),
          waiters(max_waiters * kClasses, MetadataAllocator<std::atomic<Request *>>(bootstrap))
    {
    }

    std::optional<Credit> reserve(const std::shared_ptr<Account> &account, size_t category, size_t amount) noexcept
    {
        if (category >= kClasses || closed.load(std::memory_order_acquire)) {
            return {};
        }
        if (!reserve_amount(account->used_[category], account->limits_[category], amount)) {
            denied(*account, category, false);
            return {};
        }
        if (!reserve_amount(used[category], limits[category], amount)) {
            account->used_[category].fetch_sub(amount, std::memory_order_acq_rel);
            denied(*account, category, true);
            return {};
        }
        reserved[category].fetch_add(amount, std::memory_order_relaxed);
        account->reserved_[category].fetch_add(amount, std::memory_order_relaxed);
        grants.fetch_add(1, std::memory_order_relaxed);
        return Credit(account, category, amount, false);
    }

    void denied(Account &account, size_t category, bool host) noexcept
    {
        auto &tree_counter =
            host ? account.observation_[category].denied_host : account.observation_[category].denied_tree;
        auto &host_counter = host ? observation[category].denied_host : observation[category].denied_tree;
        tree_counter.fetch_add(1, std::memory_order_relaxed);
        host_counter.fetch_add(1, std::memory_order_relaxed);
    }

    void wait_started(const Request &request) noexcept
    {
        auto &tree = request.account_->observation_[request.category_];
        auto &host = observation[request.category_];
        tree.waits.fetch_add(1, std::memory_order_relaxed);
        host.waits.fetch_add(1, std::memory_order_relaxed);
        tree.waiters.fetch_add(1, std::memory_order_relaxed);
        host.waiters.fetch_add(1, std::memory_order_relaxed);
    }

    void wait_finished(const Request &request) noexcept
    {
        const auto elapsed = budget_now_ns() - request.queued_at_;
        auto      &tree    = request.account_->observation_[request.category_];
        auto      &host    = observation[request.category_];
        tree.wait_ns.fetch_add(elapsed, std::memory_order_relaxed);
        host.wait_ns.fetch_add(elapsed, std::memory_order_relaxed);
        tree.waiters.fetch_sub(1, std::memory_order_relaxed);
        host.waiters.fetch_sub(1, std::memory_order_relaxed);
    }

    ResourceUsageSnapshot usage(const Account *account) const noexcept
    {
        ResourceUsageSnapshot result;
        for (size_t index = 0; index < kClasses; ++index) {
            const auto &counters = account != nullptr ? account->observation_[index] : observation[index];
            auto       &out      = result[index];
            out.limit            = account != nullptr ? account->limits_[index] : limits[index];
            out.used     = (account != nullptr ? account->used_[index] : used[index]).load(std::memory_order_relaxed);
            out.reserved = std::min(
                out.used, static_cast<uint64_t>((account != nullptr ? account->reserved_[index] : reserved[index])
                                                    .load(std::memory_order_relaxed)));
            out.allocated = std::min(
                out.used, static_cast<uint64_t>((account != nullptr ? account->allocated_[index] : allocated[index])
                                                    .load(std::memory_order_relaxed)));
            out.denied_tree = counters.denied_tree.load(std::memory_order_relaxed);
            out.denied_host = counters.denied_host.load(std::memory_order_relaxed);
            out.waits       = counters.waits.load(std::memory_order_relaxed);
            out.wait_ns     = counters.wait_ns.load(std::memory_order_relaxed);
            out.waiters     = counters.waiters.load(std::memory_order_relaxed);
        }
        return result;
    }

    enum class LimitFailure : uint8_t { None, Tree, Host };

    [[nodiscard]] LimitFailure allocation_failure(const Request &request) const noexcept
    {
        const auto category = request.category_;
        const auto tree     = request.account_->allocated_[category].load(std::memory_order_acquire);
        const auto host     = allocated[category].load(std::memory_order_acquire);
        if (request.amount_ >
            request.account_->limits_[category] - std::min(tree, request.account_->limits_[category])) {
            return LimitFailure::Tree;
        }
        return request.amount_ > limits[category] - std::min(host, limits[category]) ? LimitFailure::Host
                                                                                     : LimitFailure::None;
    }

    void allocation_changed() noexcept
    {
        if (reservation_waiters.load(std::memory_order_acquire) != 0) {
            notify();
        }
    }

    void notify() noexcept
    {
        if (notifications.fetch_add(1, std::memory_order_acq_rel) != 0) {
            return;
        }
        size_t observed = 1;
        do {
            const auto start = next_waiter;
            // Keep every release in the notification handshake, even when
            // empty: a concurrent registration must force another scan. Most
            // releases have no waiters and need not inspect every host slot.
            const auto count = waiting.load(std::memory_order_acquire) == 0 ? 0 : waiters.size();
            for (size_t offset = 0; offset < count; ++offset) {
                auto  index   = (start + offset) % waiters.size();
                auto &slot    = waiters[index];
                auto *request = slot.load(std::memory_order_acquire);
                if (request == nullptr || request == &idle_request) {
                    continue;
                }
                bool stopped =
                    closed.load(std::memory_order_acquire) || request->stopped_.load(std::memory_order_acquire);
                const auto failure   = !stopped && request->policy_ == Request::WaitPolicy::ReservationsOnly
                                         ? allocation_failure(*request)
                                         : LimitFailure::None;
                const bool exhausted = failure != LimitFailure::None;
                if (exhausted) {
                    denied(*request->account_, request->category_, failure == LimitFailure::Host);
                }
                if (!stopped && !exhausted) {
                    request->credit_ = reserve(request->account_, request->category_, request->amount_);
                    if (!request->credit_) {
                        continue;
                    }
                }
                slot.store(request->registration_ != nullptr ? &idle_request : nullptr, std::memory_order_release);
                next_waiter = (index + 1) % waiters.size();
                waiting.fetch_sub(1, std::memory_order_relaxed);
                if (request->policy_ == Request::WaitPolicy::ReservationsOnly) {
                    reservation_waiters.fetch_sub(1, std::memory_order_acq_rel);
                }
                wait_ns.fetch_add(budget_now_ns() - request->queued_at_, std::memory_order_relaxed);
                wait_finished(*request);
                if (stopped) {
                    cancelled.fetch_add(1, std::memory_order_relaxed);
                }
                auto  complete = request->complete_;
                auto *context  = request->context_;
                auto  outcome  = TreeWaitOutcome::Granted;
                auto  signal   = CallbackSignal::kValue;
                if (stopped) {
                    outcome = TreeWaitOutcome::Cancelled;
                    signal  = CallbackSignal::kStopped;
                }
                else if (exhausted) {
                    outcome = TreeWaitOutcome::Error;
                    signal  = CallbackSignal::kError;
                }
                request->wait_span_.finish(outcome, budget_now_ns());
                request->pending_.store(false, std::memory_order_release);
                complete(context, signal, exhausted ? Status::resource_exhausted() : Status::Ok());
            }
            // One scan observes all releases that preceded it. Coalesce that
            // batch so sustained allocation/free traffic cannot leave one
            // notifier rescanning every waiter once per historical release.
            observed = notifications.fetch_sub(observed, std::memory_order_acq_rel) - observed;
        } while (observed != 0);
    }

    std::weak_ptr<RetirementQueue>                                                 retirement;
    Limits                                                                         limits;
    size_t                                                                         waiter_capacity;
    Request                                                                        idle_request;
    std::array<std::atomic<size_t>, kClasses>                                      used{};
    std::array<std::atomic<size_t>, kClasses>                                      reserved{};
    std::array<std::atomic<size_t>, kClasses>                                      allocated{};
    std::array<Account::Observation, kClasses>                                     observation{};
    std::vector<std::atomic<Request *>, MetadataAllocator<std::atomic<Request *>>> waiters;
    size_t                                                                         next_waiter = 0;
    std::atomic<size_t>                                                            notifications{0};
    std::atomic<bool>                                                              closed{false};
    std::atomic<uint64_t>                                                          waiting{0};
    std::atomic<uint64_t>                                                          reservation_waiters{0};
    std::atomic<uint64_t>                                                          grants{0};
    std::atomic<uint64_t>                                                          cancelled{0};
    std::atomic<uint64_t>                                                          wait_ns{0};
};

size_t ResourceBudget::capacity(const std::shared_ptr<Account> &account, BudgetClass category) noexcept
{
    const auto index = static_cast<size_t>(category);
    return account && index < kClasses ? std::min(account->limits_[index], account->state_->limits[index]) : 0;
}

size_t ResourceBudget::available(const std::shared_ptr<Account> &account, BudgetClass category) noexcept
{
    const auto index = static_cast<size_t>(category);
    if (!account || index >= kClasses || account->state_->closed.load(std::memory_order_acquire)) {
        return 0;
    }
    const auto tree = account->used_[index].load(std::memory_order_acquire);
    const auto host = account->state_->used[index].load(std::memory_order_acquire);
    return std::min(account->limits_[index] - std::min(tree, account->limits_[index]),
                    account->state_->limits[index] - std::min(host, account->state_->limits[index]));
}

ResourceBudget::Account::Account(std::shared_ptr<State> state, Limits limits)
    : state_(std::move(state)),
      retirement_(state_->retirement),
      limits_(limits),
      scope_id_(next_scope_id.fetch_add(1, std::memory_order_relaxed))
{
}

void ResourceBudget::Account::set_resource_metrics(const std::shared_ptr<TreeResourceMetrics> &metrics) noexcept
{
    assert(resource_metrics_.expired());
    resource_metrics_ = metrics;
}

std::shared_ptr<TreeResourceMetrics> ResourceBudget::Account::resource_metrics() const noexcept
{
    return resource_metrics_.lock();
}

void ResourceBudget::Account::set_work_metrics(const std::shared_ptr<TreeWorkMetrics> &metrics) noexcept
{
    assert(retirement_metrics_.expired());
    retirement_metrics_ = metrics;
}

void ResourceBudget::Account::drain_retirements() const noexcept
{
    auto pending = retirement_pending_.load(std::memory_order_acquire);
    while (pending != 0) {
        retirement_pending_.wait(pending, std::memory_order_acquire);
        pending = retirement_pending_.load(std::memory_order_acquire);
    }
}

ResourceUsageSnapshot ResourceBudget::Account::resource_usage() const noexcept
{
    return state_->usage(this);
}

ResourceUsageSnapshot ResourceBudget::resource_usage() const noexcept
{
    return state_->usage(nullptr);
}

ResourceBudget::AccountSnapshot ResourceBudget::Account::snapshot() const noexcept
{
    AccountSnapshot result;
    for (size_t index = 0; index < kClasses; ++index) {
        // A release can publish reusable capacity before its mirror updates.
        // Clamp each relaxed gauge to an independently sampled admission total.
        const auto used         = used_[index].load(std::memory_order_relaxed);
        result.reserved[index]  = std::min(used, reserved_[index].load(std::memory_order_relaxed));
        result.allocated[index] = std::min(used, allocated_[index].load(std::memory_order_relaxed));
    }
    for (size_t index = 1; index < result.physical.size(); ++index) {
        result.physical[index] = physical_[index].load(std::memory_order_relaxed);
    }
    return result;
}

AllocationReservation::AllocationReservation(ResourceBudget::Credit                 credit,
                                             std::shared_ptr<AllocationReservation> parent)
    : category_(0),
      available_(0),
      parent_(std::move(parent))
{
    if (!credit.account_ || credit.allocated_ || credit.amount_ >= kSealed) {
        throw std::invalid_argument("allocation reservation requires unconsumed metadata credit");
    }
    assert(!parent_ || (parent_->account_ == credit.account_ && parent_->category_ == credit.category_));
    account_  = std::move(credit.account_);
    category_ = credit.category_;
    available_.store(std::exchange(credit.amount_, 0), std::memory_order_relaxed);
}

AllocationReservation::~AllocationReservation()
{
    seal();
}

bool AllocationReservation::claim(size_t bytes) noexcept
{
    auto remaining = available_.load(std::memory_order_relaxed);
    while ((remaining & kSealed) == 0 && bytes <= remaining) {
        if (available_.compare_exchange_weak(remaining, remaining - bytes, std::memory_order_acq_rel)) {
            auto &state = *account_->state_;
            account_->allocated_[category_].fetch_add(bytes, std::memory_order_relaxed);
            account_->reserved_[category_].fetch_sub(bytes, std::memory_order_relaxed);
            state.allocated[category_].fetch_add(bytes, std::memory_order_relaxed);
            state.reserved[category_].fetch_sub(bytes, std::memory_order_relaxed);
            state.allocation_changed();
            return true;
        }
    }
    return false;
}

void AllocationReservation::release(size_t bytes) noexcept
{
    auto state     = account_->state_;
    auto remaining = available_.load(std::memory_order_acquire);
    while ((remaining & kSealed) == 0) {
        if (available_.compare_exchange_weak(remaining, remaining + bytes, std::memory_order_acq_rel)) {
            account_->allocated_[category_].fetch_sub(bytes, std::memory_order_relaxed);
            account_->reserved_[category_].fetch_add(bytes, std::memory_order_relaxed);
            state->allocated[category_].fetch_sub(bytes, std::memory_order_relaxed);
            state->reserved[category_].fetch_add(bytes, std::memory_order_relaxed);
            return;
        }
    }
    if (parent_) {
        parent_->release(bytes);
        return;
    }
    account_->allocated_[category_].fetch_sub(bytes, std::memory_order_relaxed);
    state->allocated[category_].fetch_sub(bytes, std::memory_order_relaxed);
    account_->used_[category_].fetch_sub(bytes, std::memory_order_acq_rel);
    state->used[category_].fetch_sub(bytes, std::memory_order_acq_rel);
    state->notify();
}

void AllocationReservation::seal() noexcept
{
    const auto remaining = available_.exchange(kSealed, std::memory_order_acq_rel);
    if ((remaining & kSealed) != 0) {
        return;
    }
    if (parent_ && parent_->restore_reserved(remaining)) {
        return;
    }
    auto state = account_->state_;
    account_->reserved_[category_].fetch_sub(remaining, std::memory_order_relaxed);
    state->reserved[category_].fetch_sub(remaining, std::memory_order_relaxed);
    account_->used_[category_].fetch_sub(remaining, std::memory_order_acq_rel);
    state->used[category_].fetch_sub(remaining, std::memory_order_acq_rel);
    state->notify();
}

bool AllocationReservation::restore_reserved(size_t bytes) noexcept
{
    auto remaining = available_.load(std::memory_order_acquire);
    while ((remaining & kSealed) == 0) {
        assert(bytes < kSealed - remaining);
        if (available_.compare_exchange_weak(remaining, remaining + bytes, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

size_t AllocationReservation::available() const noexcept
{
    return available_.load(std::memory_order_relaxed) & ~kSealed;
}

std::optional<ResourceBudget::Credit> AllocationReservation::take_reserved(size_t bytes) noexcept
{
    auto remaining = available_.load(std::memory_order_acquire);
    while ((remaining & kSealed) == 0 && bytes <= remaining) {
        if (available_.compare_exchange_weak(remaining, remaining - bytes, std::memory_order_acq_rel)) {
            return ResourceBudget::Credit(account_, category_, bytes, false);
        }
    }
    return {};
}

const void *AllocationReservation::domain() const noexcept
{
    return account_->state_.get();
}

bool AllocationReservation::return_reserved(ResourceBudget::Credit credit) noexcept
{
    if (!credit.belongs_to(account_, category()) || credit.allocated() || !restore_reserved(credit.amount_)) {
        return false;
    }
    credit.amount_ = 0;
    return true;
}

ResourceBudget::Credit::Credit(std::shared_ptr<Account> account, size_t category, size_t amount, bool allocated)
    : account_(std::move(account)),
      category_(category),
      amount_(amount),
      allocated_(allocated)
{
}

ResourceBudget::Credit::~Credit()
{
    release();
}

ResourceBudget::Credit::Credit(Credit &&other) noexcept
    : account_(std::move(other.account_)),
      category_(other.category_),
      amount_(std::exchange(other.amount_, 0)),
      allocated_(other.allocated_),
      kind_(other.kind_)
{
}

ResourceBudget::Credit &ResourceBudget::Credit::operator=(Credit &&other) noexcept
{
    if (this != &other) {
        release();
        account_   = std::move(other.account_);
        category_  = other.category_;
        amount_    = std::exchange(other.amount_, 0);
        allocated_ = other.allocated_;
        kind_      = other.kind_;
    }
    return *this;
}

std::optional<ResourceBudget::Credit> ResourceBudget::Credit::allocate(size_t amount) noexcept
{
    if (!account_ || allocated_ || amount > amount_) {
        return {};
    }
    auto &state = *account_->state_;
    account_->allocated_[category_].fetch_add(amount, std::memory_order_relaxed);
    account_->reserved_[category_].fetch_sub(amount, std::memory_order_relaxed);
    state.allocated[category_].fetch_add(amount, std::memory_order_relaxed);
    state.reserved[category_].fetch_sub(amount, std::memory_order_relaxed);
    amount_ -= amount;
    state.allocation_changed();
    return Credit(account_, category_, amount, true);
}

std::optional<ResourceBudget::Credit> ResourceBudget::Credit::split(size_t amount) noexcept
{
    if (!account_ || amount > amount_) {
        return {};
    }
    amount_ -= amount;
    Credit part(account_, category_, amount, allocated_);
    part.kind_ = kind_;
    return part;
}

void ResourceBudget::Credit::release() noexcept
{
    if (!account_) {
        return;
    }
    auto account = std::move(account_);
    if (kind_ != AllocationKind::kUnknown) {
        account->physical_[static_cast<size_t>(kind_)].fetch_sub(amount_, std::memory_order_relaxed);
        record_work(account->work_metrics(), TreeWorkCounter::MemoryReclaimedBytes, amount_, TreeWorkCause::Allocation);
    }
    auto  state           = account->state_;
    auto  amount          = std::exchange(amount_, 0);
    auto &counter         = allocated_ ? state->allocated[category_] : state->reserved[category_];
    auto &account_counter = allocated_ ? account->allocated_[category_] : account->reserved_[category_];
    account_counter.fetch_sub(amount, std::memory_order_relaxed);
    counter.fetch_sub(amount, std::memory_order_relaxed);
    account->used_[category_].fetch_sub(amount, std::memory_order_acq_rel);
    state->used[category_].fetch_sub(amount, std::memory_order_acq_rel);
    state->notify();
}

ResourceBudget::Registration::~Registration()
{
    reset();
}

ResourceBudget::Registration::Registration(Registration &&other) noexcept
    : state_(std::move(other.state_)),
      index_(other.index_)
{
}

ResourceBudget::Registration &ResourceBudget::Registration::operator=(Registration &&other) noexcept
{
    if (this != &other) {
        reset();
        state_ = std::move(other.state_);
        index_ = other.index_;
    }
    return *this;
}

void ResourceBudget::Registration::reset() noexcept
{
    if (auto state = std::move(state_)) {
        auto                       *expected = &state->idle_request;
        [[maybe_unused]] const bool released =
            state->waiters[index_].compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel);
        assert(released);
    }
}

std::optional<ResourceBudget::Registration> ResourceBudget::try_register(const std::shared_ptr<Account> &account,
                                                                         BudgetClass category) noexcept
{
    const auto value = static_cast<size_t>(category);
    if (!account || value >= kClasses) {
        return std::nullopt;
    }
    auto state = account->state_;
    if (state->closed.load(std::memory_order_acquire)) {
        return std::nullopt;
    }
    const auto begin = value * state->waiter_capacity;
    for (size_t index = begin; index < begin + state->waiter_capacity; ++index) {
        Request *expected = nullptr;
        if (state->waiters[index].compare_exchange_strong(expected, &state->idle_request, std::memory_order_acq_rel)) {
            return Registration(std::move(state), index);
        }
    }
    return std::nullopt;
}

ResourceBudget::Request::Request(std::shared_ptr<Account> account, BudgetClass category, size_t amount,
                                 const Registration *registration, WaitPolicy policy)
    : account_(std::move(account)),
      category_(static_cast<size_t>(category)),
      amount_(amount),
      registration_(registration),
      policy_(policy)
{
    if (!account_ || category_ >= kClasses ||
        (registration_ != nullptr && (registration_->state_ != account_->state_ ||
                                      registration_->index_ / account_->state_->waiter_capacity != category_))) {
        throw std::invalid_argument("budget request requires account and valid class");
    }
}

ResourceBudget::Request::~Request()
{
    assert(!pending_.load(std::memory_order_acquire));
}

CallbackSender ResourceBudget::Request::sender() noexcept
{
    return {this, &Request::submit};
}

void ResourceBudget::Request::submit(void *source, CallbackComplete complete, void *context)
{
    auto &request = *static_cast<Request *>(source);
    if (request.submitted_.test_and_set(std::memory_order_acq_rel)) {
        complete(context, CallbackSignal::kError, Status::invalid_argument("budget request already started"));
        return;
    }
    auto state = request.account_->state_;
    static_assert(kClasses == static_cast<size_t>(TreeWaitClass::SharedCache));
    request.queued_at_ = budget_now_ns();
    request.wait_span_.begin(request.account_->work_metrics(), static_cast<TreeWaitClass>(request.category_),
                             request.queued_at_);
    if (request.amount_ > request.account_->limits_[request.category_] ||
        request.amount_ > state->limits[request.category_]) {
        state->denied(*request.account_, request.category_,
                      request.amount_ <= request.account_->limits_[request.category_]);
        request.wait_span_.finish(TreeWaitOutcome::Error, budget_now_ns());
        complete(context, CallbackSignal::kError, Status::resource_exhausted("request exceeds budget partition"));
        return;
    }
    request.complete_ = complete;
    request.context_  = context;
    request.pending_.store(true, std::memory_order_release);
    state->wait_started(request);
    state->waiting.fetch_add(1, std::memory_order_relaxed);
    if (request.policy_ == WaitPolicy::ReservationsOnly) {
        state->reservation_waiters.fetch_add(1, std::memory_order_acq_rel);
    }
    if (request.registration_ != nullptr) {
        auto *expected = &state->idle_request;
        if (state->waiters[request.registration_->index_].compare_exchange_strong(expected, &request,
                                                                                  std::memory_order_release)) {
            record_work(request.account_->work_metrics(), TreeWorkCounter::AdmissionWaits, 1,
                        TreeWorkCause::Allocation);
            state->notify();
            return;
        }
        state->waiting.fetch_sub(1, std::memory_order_relaxed);
        state->wait_finished(request);
        if (request.policy_ == WaitPolicy::ReservationsOnly) {
            state->reservation_waiters.fetch_sub(1, std::memory_order_acq_rel);
        }
        request.pending_.store(false, std::memory_order_release);
        request.wait_span_.finish(TreeWaitOutcome::Error, budget_now_ns());
        complete(context, CallbackSignal::kError, Status::internal_error("prepaid budget waiter already in use"));
        return;
    }
    auto start = request.category_ * state->waiter_capacity;
    for (size_t index = start; index < start + state->waiter_capacity; ++index) {
        auto    &slot     = state->waiters[index];
        Request *expected = nullptr;
        if (slot.compare_exchange_strong(expected, &request, std::memory_order_release)) {
            record_work(request.account_->work_metrics(), TreeWorkCounter::AdmissionWaits, 1,
                        TreeWorkCause::Allocation);
            // Registration precedes the retry; a concurrent release cannot
            // slip between a failed check and going to sleep.
            state->notify();
            return;
        }
    }
    state->waiting.fetch_sub(1, std::memory_order_relaxed);
    state->wait_finished(request);
    if (request.policy_ == WaitPolicy::ReservationsOnly) {
        state->reservation_waiters.fetch_sub(1, std::memory_order_acq_rel);
    }
    request.pending_.store(false, std::memory_order_release);
    request.wait_span_.finish(TreeWaitOutcome::Error, budget_now_ns());
    complete(context, CallbackSignal::kError, Status::resource_exhausted("budget waiter slots full"));
}

void ResourceBudget::Request::request_stop() noexcept
{
    auto state = account_->state_;
    stopped_.store(true, std::memory_order_release);
    state->notify();
}

std::optional<ResourceBudget::Credit> ResourceBudget::Request::take_credit() noexcept
{
    return std::exchange(credit_, std::nullopt);
}

ResourceBudget::ResourceBudget(Limits limits, size_t max_waiters_per_class,
                               const std::shared_ptr<AllocationBudget> &bootstrap)
{
    if (max_waiters_per_class == 0 || max_waiters_per_class > std::numeric_limits<size_t>::max() / kClasses) {
        throw std::invalid_argument("budget requires bounded waiter slots");
    }
    state_ = std::allocate_shared<State>(MetadataAllocator<State>(bootstrap), limits, max_waiters_per_class, bootstrap);
}

bool ResourceBudget::claim_allocation(const std::shared_ptr<Account> &account, BudgetClass category,
                                      size_t bytes) noexcept
{
    if (category == BudgetClass::kIo) {
        return false;
    }
    auto credit = try_reserve(account, category, bytes);
    if (!credit) {
        return false;
    }
    auto allocation = credit->allocate(bytes);
    if (!allocation) {
        return false;
    }
    credit->account_.reset();
    allocation->amount_ = 0;
    allocation->account_.reset();
    return true;
}

void ResourceBudget::release_allocation(const std::shared_ptr<Account> &account, BudgetClass category,
                                        size_t bytes) noexcept
{
    if (account) {
        Credit allocation(account, static_cast<size_t>(category), bytes, true);
    }
}

const void *ResourceBudget::allocation_domain(const std::shared_ptr<Account> &account) noexcept
{
    return account != nullptr ? account->state_.get() : nullptr;
}

bool ResourceBudget::transfer_allocation(
    const std::shared_ptr<Account> &from, const std::shared_ptr<Account> &to, BudgetClass category, size_t bytes,
    const std::array<size_t, static_cast<size_t>(AllocationKind::kCount)> &physical) noexcept
{
    const auto index = static_cast<size_t>(category);
    if (!to || (from && from->state_ != to->state_) || index >= kClasses || category == BudgetClass::kIo ||
        to->state_->closed.load(std::memory_order_acquire)) {
        return false;
    }
    if (!from) {
        if (!claim_allocation(to, category, bytes)) {
            return false;
        }
        for (size_t kind = 1; kind < physical.size(); ++kind) {
            record_allocation(to, static_cast<AllocationKind>(kind), physical[kind], true);
        }
        return true;
    }
    if (from == to) {
        return true;
    }
    if (!reserve_amount(to->used_[index], to->limits_[index], bytes)) {
        to->state_->denied(*to, index, false);
        return false;
    }
    to->allocated_[index].fetch_add(bytes, std::memory_order_relaxed);
    [[maybe_unused]] const auto previous = from->allocated_[index].fetch_sub(bytes, std::memory_order_relaxed);
    assert(previous >= bytes);
    from->used_[index].fetch_sub(bytes, std::memory_order_acq_rel);
    for (size_t kind = 0; kind < physical.size(); ++kind) {
        to->physical_[kind].fetch_add(physical[kind], std::memory_order_relaxed);
        [[maybe_unused]] const auto old = from->physical_[kind].fetch_sub(physical[kind], std::memory_order_relaxed);
        assert(old >= physical[kind]);
    }
    // Host admission is unchanged: these are the same live allocations.
    from->state_->notify();
    return true;
}

ResourceBudget::~ResourceBudget()
{
    close();
    // The facade is no longer callable. Existing accounts keep their own weak
    // queue association, while the host ledger must not retain a queue control
    // block whose allocator points back to that same ledger.
    state_->retirement.reset();
}

std::shared_ptr<ResourceBudget::Account> ResourceBudget::make_account(Limits limits)
{
    return std::shared_ptr<Account>(new Account(state_, limits));
}

std::shared_ptr<ResourceBudget::Account> ResourceBudget::make_account(Limits                                   limits,
                                                                      const std::shared_ptr<AllocationBudget> &budget)
{
    if (!budget) {
        throw std::invalid_argument("charged account requires host metadata credit");
    }
    MetadataAllocator<Account> allocator(budget);
    auto                      *account = allocator.allocate(1);
    try {
        ::new (static_cast<void *>(account)) Account(state_, limits);
    }
    catch (...) {
        allocator.deallocate(account, 1);
        throw;
    }
    return {account,
            [allocator](Account *value) mutable {
                std::destroy_at(value);
                allocator.deallocate(value, 1);
            },
            allocator};
}

std::optional<ResourceBudget::Credit> ResourceBudget::try_reserve(const std::shared_ptr<Account> &account,
                                                                  BudgetClass category, size_t amount) noexcept
{
    if (!account) {
        return {};
    }
    auto state  = account->state_;
    auto credit = state->reserve(account, static_cast<size_t>(category), amount);
    if (!credit) {
        record_work(account->work_metrics(), TreeWorkCounter::AdmissionRejected, 1, TreeWorkCause::Allocation);
        // A failed host acquisition rolls back a temporary Tree charge. A
        // concurrent waiter may have observed that charge; retry after rollback.
        state->notify();
    }
    return credit;
}

void ResourceBudget::set_retirement_queue(const std::shared_ptr<RetirementQueue> &queue)
{
    state_->retirement = queue;
}

void ResourceBudget::close() noexcept
{
    auto state = state_;
    state->closed.store(true, std::memory_order_release);
    state->notify();
}

ResourceBudget::Snapshot ResourceBudget::snapshot() const noexcept
{
    Snapshot result;
    for (size_t i = 0; i < kClasses; ++i) {
        result.reserved[i]  = state_->reserved[i].load(std::memory_order_relaxed);
        result.allocated[i] = state_->allocated[i].load(std::memory_order_relaxed);
    }
    result.waiters   = state_->waiting.load(std::memory_order_relaxed);
    result.grants    = state_->grants.load(std::memory_order_relaxed);
    result.cancelled = state_->cancelled.load(std::memory_order_relaxed);
    result.wait_ns   = state_->wait_ns.load(std::memory_order_relaxed);
    return result;
}

} // namespace crowdb::tree::detail
