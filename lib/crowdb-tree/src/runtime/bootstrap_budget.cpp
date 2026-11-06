// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "bootstrap_budget.h"

#include <cassert>
#include <stdexcept>

namespace crowdb::tree::detail
{
// This allocator is used for the bootstrap budget's own control block only.
// The object lives until the last weak control release, so the control can
// return its actual bytes before deleting the root accounting object.
template <class T> class BootstrapControlAllocator
{
  public:
    using value_type = T;

    explicit BootstrapControlAllocator(BootstrapBudget *owner) noexcept : owner_(owner)
    {
    }

    template <class U>
    BootstrapControlAllocator(const BootstrapControlAllocator<U> &other) noexcept : owner_(other.owner_)
    {
    }

    T *allocate(size_t count)
    {
        if (count > SIZE_MAX / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        const auto bytes = count * sizeof(T);
        if (!owner_->claim(bytes)) {
            throw std::bad_alloc();
        }
        try {
            auto *pointer = static_cast<T *>(::operator new(bytes, std::align_val_t{alignof(T)}));
            owner_->record_allocation(AllocationKind::kMetadata, bytes, true);
            return pointer;
        }
        catch (...) {
            owner_->release(bytes);
            throw;
        }
    }

    void deallocate(T *pointer, size_t count) noexcept
    {
        owner_->release_control(pointer, count * sizeof(T), alignof(T));
    }

    template <class U> bool operator==(const BootstrapControlAllocator<U> &other) const noexcept
    {
        return owner_ == other.owner_;
    }

  private:
    template <class U> friend class BootstrapControlAllocator;
    BootstrapBudget *owner_;
};

BootstrapBudget::BootstrapBudget(size_t capacity) : capacity_(capacity)
{
    physical_[static_cast<size_t>(AllocationKind::kMetadata)].store(sizeof(BootstrapBudget), std::memory_order_relaxed);
}

BootstrapBudget::~BootstrapBudget()
{
    assert((used_.load(std::memory_order_relaxed) & ~kSealed) == sizeof(BootstrapBudget));
}

std::shared_ptr<BootstrapBudget> BootstrapBudget::create(size_t capacity)
{
    if (capacity >= kSealed) {
        throw std::invalid_argument("host lifecycle capacity overflows accounting");
    }
    if (capacity < sizeof(BootstrapBudget)) {
        throw std::bad_alloc();
    }
    auto *budget = new BootstrapBudget(capacity);
    try {
        // The control allocator performs final destruction after weak owners.
        return {budget, [](BootstrapBudget *) noexcept {}, BootstrapControlAllocator<BootstrapBudget>(budget)};
    }
    catch (...) {
        delete budget;
        throw;
    }
}

void BootstrapBudget::release_control(void *pointer, size_t bytes, size_t alignment) noexcept
{
    ::operator delete(pointer, std::align_val_t{alignment});
    record_allocation(AllocationKind::kMetadata, bytes, false);
    release(bytes);
    delete this;
}

bool BootstrapBudget::claim(size_t bytes) noexcept
{
    auto current = used_.load(std::memory_order_relaxed);
    while ((current & kSealed) == 0 && bytes <= capacity_ - current) {
        if (used_.compare_exchange_weak(current, current + bytes, std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void BootstrapBudget::release(size_t bytes) noexcept
{
    [[maybe_unused]] const auto before = used_.fetch_sub(bytes, std::memory_order_acq_rel);
    assert((before & ~kSealed) >= bytes + sizeof(BootstrapBudget));
}

void BootstrapBudget::record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept
{
    const auto index = static_cast<size_t>(kind);
    if (index >= physical_.size() || kind == AllocationKind::kUnknown) {
        return;
    }
    if (acquired) {
        physical_[index].fetch_add(bytes, std::memory_order_relaxed);
    }
    else {
        [[maybe_unused]] const auto before = physical_[index].fetch_sub(bytes, std::memory_order_relaxed);
        assert(before >= bytes);
    }
}

BootstrapBudget::Snapshot BootstrapBudget::snapshot() const noexcept
{
    const auto used = used_.load(std::memory_order_relaxed);
    Snapshot   result{.allocated = used & ~kSealed, .reserved = (used & kSealed) == 0 ? capacity_ - used : 0};
    for (size_t index = 0; index < physical_.size(); ++index) {
        result.physical[index] = physical_[index].load(std::memory_order_relaxed);
    }
    return result;
}

size_t BootstrapBudget::available() const noexcept
{
    const auto used = used_.load(std::memory_order_relaxed);
    return (used & kSealed) == 0 ? capacity_ - used : 0;
}

void BootstrapBudget::seal() noexcept
{
    used_.fetch_or(kSealed, std::memory_order_acq_rel);
}
} // namespace crowdb::tree::detail
