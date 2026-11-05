// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <utility>

namespace crowdb::tree
{
class TreeWorkMetrics;
class TreeResourceMetrics;
class AllocationBudget;

namespace detail
{
class RetirementQueue;
template <class T> struct RetirementTraits;

struct RetirementCompletion
{
    std::shared_ptr<const void> owner;
    void (*finish)(const void *) noexcept = nullptr;
};

struct RetirementObservation
{
    void                                 start() noexcept;
    std::shared_ptr<TreeWorkMetrics>     metrics;
    std::shared_ptr<TreeResourceMetrics> resources;
    size_t                               bytes = 0;
    std::atomic<uint64_t>                started_at{0};
    size_t                               age_slot = std::numeric_limits<size_t>::max();
};

// A copied immutable descriptor receives a fresh observer from its factory.
// The observer storage is embedded in the charged lifetime node, outside T.
class RetirementObserver
{
  public:
    RetirementObserver() = default;

    RetirementObserver(const RetirementObserver & /*other*/) noexcept
    {
    }

    RetirementObserver &operator=(const RetirementObserver &) = delete;

    void bind(RetirementObservation *observation) noexcept
    {
        observation_ = observation;
    }

    void mark() const noexcept
    {
        if (observation_ != nullptr) {
            observation_->start();
        }
    }

  private:
    RetirementObservation *observation_ = nullptr;
};

struct RetirementNode
{
    std::shared_ptr<AllocationBudget> retirement_budget;
    RetirementCompletion              retirement_completion;
    RetirementObservation             observation;
    RetirementNode                   *next     = nullptr;
    bool (*dispose)(RetirementNode *) noexcept = nullptr;
};
} // namespace detail

enum class AllocationKind : uint8_t { kUnknown, kPayload, kFrame, kMetadata, kIo, kScratch, kCount };

// Allocation owners can outlive the Tree and every source catalog. Claims
// fail without waiting. Reservations consume admitted credit; stable backend
// accounts admit new capacity against both their current Tree and host quota.
class AllocationBudget
{
  public:
    virtual ~AllocationBudget()                               = default;
    [[nodiscard]] virtual bool claim(size_t bytes) noexcept   = 0;
    virtual void               release(size_t bytes) noexcept = 0;

    // Physical categories come from the actual allocation owner, independently
    // of the admission partition. Observations never govern admission.
    virtual void record_allocation(AllocationKind /*kind*/, size_t /*bytes*/, bool /*acquired*/) noexcept
    {
    }

    [[nodiscard]] virtual std::shared_ptr<TreeWorkMetrics> retirement_metrics() const noexcept
    {
        return {};
    }

    [[nodiscard]] virtual std::shared_ptr<TreeResourceMetrics> resource_metrics() const noexcept
    {
        return {};
    }

    [[nodiscard]] virtual detail::RetirementCompletion retirement_started() noexcept
    {
        return {};
    }

    [[nodiscard]] virtual std::shared_ptr<detail::RetirementQueue> retirement_queue() const noexcept
    {
        return {};
    }

    [[nodiscard]] virtual const void *domain() const noexcept
    {
        return this;
    }
};

class AllocationLease
{
  public:
    AllocationLease() = default;

    AllocationLease(std::shared_ptr<AllocationBudget> budget, size_t bytes,
                    AllocationKind kind = AllocationKind::kUnknown)
        : budget_(std::move(budget)),
          bytes_(bytes),
          kind_(kind)
    {
        if (budget_ && !budget_->claim(bytes_)) {
            throw std::bad_alloc();
        }
        if (budget_ && kind_ != AllocationKind::kUnknown) {
            budget_->record_allocation(kind_, bytes_, true);
        }
    }

    ~AllocationLease()
    {
        reset();
    }

    AllocationLease(const AllocationLease &)            = delete;
    AllocationLease &operator=(const AllocationLease &) = delete;

    AllocationLease(AllocationLease &&other) noexcept
        : budget_(std::move(other.budget_)),
          bytes_(std::exchange(other.bytes_, 0)),
          kind_(other.kind_)
    {
    }

    AllocationLease &operator=(AllocationLease &&other) noexcept
    {
        if (this != &other) {
            reset();
            budget_ = std::move(other.budget_);
            bytes_  = std::exchange(other.bytes_, 0);
            kind_   = other.kind_;
        }
        return *this;
    }

    void reset() noexcept
    {
        if (auto budget = std::move(budget_)) {
            if (kind_ != AllocationKind::kUnknown) {
                budget->record_allocation(kind_, bytes_, false);
            }
            budget->release(std::exchange(bytes_, 0));
        }
    }

  private:
    std::shared_ptr<AllocationBudget> budget_;
    size_t                            bytes_ = 0;
    AllocationKind                    kind_  = AllocationKind::kUnknown;
};

// The allocator sees the actual control-block/container capacity requested by
// the standard library, including growth and simultaneous old/new buffers.
template <class T, class Budget = AllocationBudget, AllocationKind Kind = AllocationKind::kUnknown>
class BudgetAllocator
{
  public:
    using value_type      = T;
    using is_always_equal = std::false_type;

    template <class U> struct rebind
    {
        using other = BudgetAllocator<U, Budget, Kind>;
    };

    BudgetAllocator() = default;

    explicit BudgetAllocator(std::shared_ptr<Budget> owner) noexcept : owner_(std::move(owner))
    {
    }

    template <class U> BudgetAllocator(const BudgetAllocator<U, Budget, Kind> &other) noexcept : owner_(other.owner())
    {
    }

    [[nodiscard]] T *allocate(size_t count)
    {
        if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        const auto bytes = count * sizeof(T);
        if (owner_ && !owner_->claim(bytes)) {
            throw std::bad_alloc();
        }
        try {
            auto *result = static_cast<T *>(::operator new(bytes, std::align_val_t{alignof(T)}));
            if constexpr (Kind != AllocationKind::kUnknown) {
                if (owner_) {
                    owner_->record_allocation(Kind, bytes, true);
                }
            }
            return result;
        }
        catch (...) {
            if (owner_) {
                owner_->release(bytes);
            }
            throw;
        }
    }

    void deallocate(T *pointer, size_t count) noexcept
    {
        ::operator delete(pointer, std::align_val_t{alignof(T)});
        if (owner_) {
            if constexpr (Kind != AllocationKind::kUnknown) {
                owner_->record_allocation(Kind, count * sizeof(T), false);
            }
            owner_->release(count * sizeof(T));
        }
    }

    [[nodiscard]] const std::shared_ptr<Budget> &owner() const noexcept
    {
        return owner_;
    }

    template <class U> bool operator==(const BudgetAllocator<U, Budget, Kind> &other) const noexcept
    {
        return owner_ == other.owner();
    }

  private:
    std::shared_ptr<Budget> owner_;
};
template <class T, class Budget = AllocationBudget>
using MetadataAllocator = BudgetAllocator<T, Budget, AllocationKind::kMetadata>;
} // namespace crowdb::tree
