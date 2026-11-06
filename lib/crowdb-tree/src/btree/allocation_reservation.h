// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"
#include "resource_budget.h"
#include "retired_shared.h"

#include <limits>
#include <new>
#include <utility>
#include <vector>

namespace crowdb::tree::detail
{
// A whole-operation reservation can be consumed by independent workers without
// another budget wait. Allocators retain this owner across publication and old
// views. seal() returns unused credit; subsequent frees return allocated credit
// to the host. Before sealing, freed bytes are reusable by a repair attempt.
class AllocationReservation : public AllocationBudget
{
  public:
    explicit AllocationReservation(ResourceBudget::Credit credit, std::shared_ptr<AllocationReservation> parent = {});
    // Bootstrap a byte reservation from its own credit. Its shared control
    // allocation remains charged even after destruction while weak refs exist.
    [[nodiscard]] static std::shared_ptr<AllocationReservation> create(ResourceBudget::Credit credit);
    // Retain independent progress after the parent seals; return unused credit
    // and later frees to that parent's window while it remains open.
    [[nodiscard]] static std::shared_ptr<AllocationReservation>
    create(const std::shared_ptr<AllocationReservation> &parent, size_t bytes);
    ~AllocationReservation() override;
    AllocationReservation(const AllocationReservation &)            = delete;
    AllocationReservation &operator=(const AllocationReservation &) = delete;

    [[nodiscard]] bool   claim(size_t bytes) noexcept override;
    void                 release(size_t bytes) noexcept override;
    void                 record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept override;
    void                 seal() noexcept;
    [[nodiscard]] size_t available() const noexcept;
    // Carve out independent progress credit without changing physical usage.
    [[nodiscard]] std::optional<ResourceBudget::Credit> take_reserved(size_t bytes) noexcept;
    [[nodiscard]] bool                                  return_reserved(ResourceBudget::Credit credit) noexcept;
    [[nodiscard]] const void                           *domain() const noexcept override;

    [[nodiscard]] const std::shared_ptr<ResourceBudget::Account> &account() const noexcept
    {
        return account_;
    }

    [[nodiscard]] std::shared_ptr<TreeResourceMetrics> resource_metrics() const noexcept override
    {
        return account_->resource_metrics();
    }

    [[nodiscard]] std::shared_ptr<TreeWorkMetrics> retirement_metrics() const noexcept override
    {
        return account_->retirement_metrics_.lock();
    }

    [[nodiscard]] RetirementCompletion retirement_started() noexcept override
    {
        account_->retirement_pending_.fetch_add(1, std::memory_order_acq_rel);
        return {.owner = account_, .finish = [](const void *raw) noexcept {
                    const auto &account = *static_cast<const ResourceBudget::Account *>(raw);
                    account.retirement_pending_.fetch_sub(1, std::memory_order_acq_rel);
                    account.retirement_pending_.notify_all();
                }};
    }

    [[nodiscard]] std::shared_ptr<RetirementQueue> retirement_queue() const noexcept override
    {
        return account_->retirement_.lock();
    }

    [[nodiscard]] BudgetClass category() const noexcept
    {
        return static_cast<BudgetClass>(category_);
    }

  private:
    bool                                     restore_reserved(size_t bytes) noexcept;
    static constexpr size_t                  kSealed = size_t{1} << (std::numeric_limits<size_t>::digits - 1);
    std::shared_ptr<ResourceBudget::Account> account_;
    size_t                                   category_;
    std::atomic<size_t>                      available_;
    std::shared_ptr<AllocationReservation>   parent_;
};

template <class T, AllocationKind Kind = AllocationKind::kMetadata>
using ReservationAllocator = BudgetAllocator<T, AllocationBudget, Kind>;
template <class T, AllocationKind Kind = AllocationKind::kMetadata>
using ReservedVector = std::vector<T, ReservationAllocator<T, Kind>>;

template <class T> struct ReservationDeleter
{
    std::shared_ptr<AllocationBudget> owner;

    void operator()(T *value) const noexcept
    {
        if (value != nullptr) {
            ReservationAllocator<T> allocator(owner);
            std::destroy_at(value);
            allocator.deallocate(value, 1);
        }
    }
};

template <class T> using ReservedUnique = std::unique_ptr<T, ReservationDeleter<T>>;

template <class T, class... Args>
ReservedUnique<T> reserved_unique(const std::shared_ptr<AllocationBudget> &owner, Args &&...args)
{
    ReservationAllocator<T> allocator(owner);
    auto                   *value = allocator.allocate(1);
    try {
        std::construct_at(value, std::forward<Args>(args)...);
    }
    catch (...) {
        allocator.deallocate(value, 1);
        throw;
    }
    return ReservedUnique<T>(value, ReservationDeleter<T>{owner});
}

template <class T, class... Args>
std::shared_ptr<T> reserved_shared(const std::shared_ptr<AllocationBudget> &owner, Args &&...args)
{
    if constexpr (RetirementTraits<T>::enabled) {
        if (owner) {
            if (auto queue = owner->retirement_queue()) {
                return retired_shared<T>(owner, queue, std::forward<Args>(args)...);
            }
        }
    }
    return std::allocate_shared<T>(ReservationAllocator<T>(owner), std::forward<Args>(args)...);
}

// Allows a class factory to construct a private operation handle while both
// its object and shared control block remain charged to the same reservation.
template <class T, class Construct>
std::shared_ptr<T> reserved_construct(const std::shared_ptr<AllocationBudget> &owner, Construct &&construct)
{
    ReservationAllocator<T> allocator(owner);
    auto                   *value = allocator.allocate(1);
    try {
        std::forward<Construct>(construct)(value);
    }
    catch (...) {
        allocator.deallocate(value, 1);
        throw;
    }
    // Keep raw storage guarded until shared_ptr owns its failure cleanup.
    // shared_ptr invokes the deleter if allocating its control block fails.
    return std::shared_ptr<T>(value, ReservationDeleter<T>{owner}, allocator);
}
} // namespace crowdb::tree::detail
