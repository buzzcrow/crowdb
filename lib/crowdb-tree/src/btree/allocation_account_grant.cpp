// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_grant.h"

#include <limits>
#include <new>
#include <stdexcept>

namespace crowdb::tree::detail
{
namespace
{
// Only allocate() borrows the factory's stack credit. The stored allocator
// frees through the independent stable owner after the grant is destroyed.
template <class T> class GrantControlAllocator
{
  public:
    using value_type = T;

    GrantControlAllocator(std::shared_ptr<AllocationAccount> owner, ResourceBudget::Credit *source) noexcept
        : owner_(std::move(owner)),
          source_(source)
    {
    }

    template <class U>
    GrantControlAllocator(const GrantControlAllocator<U> &other) noexcept
        : owner_(other.owner()),
          source_(other.source())
    {
    }

    [[nodiscard]] T *allocate(size_t count)
    {
        if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        const auto bytes  = count * sizeof(T);
        auto       credit = source_->allocate(bytes);
        if (!credit || !owner_->adopt_allocated(&*credit)) {
            throw std::bad_alloc();
        }
        try {
            auto *result = static_cast<T *>(::operator new(bytes, std::align_val_t{alignof(T)}));
            owner_->record_allocation(AllocationKind::kMetadata, bytes, true);
            return result;
        }
        catch (...) {
            owner_->release(bytes);
            throw;
        }
    }

    void deallocate(T *pointer, size_t count) noexcept
    {
        ::operator delete(pointer, std::align_val_t{alignof(T)});
        owner_->record_allocation(AllocationKind::kMetadata, count * sizeof(T), false);
        owner_->release(count * sizeof(T));
    }

    [[nodiscard]] const std::shared_ptr<AllocationAccount> &owner() const noexcept
    {
        return owner_;
    }

    [[nodiscard]] ResourceBudget::Credit *source() const noexcept
    {
        return source_;
    }

    template <class U> bool operator==(const GrantControlAllocator<U> &other) const noexcept
    {
        return owner_ == other.owner() && source_ == other.source();
    }

  private:
    std::shared_ptr<AllocationAccount> owner_;
    ResourceBudget::Credit            *source_;
};
} // namespace

BackendAllocationGrant::BackendAllocationGrant(std::shared_ptr<AllocationAccount> owner, ResourceBudget::Credit credit)
    : owner_(std::move(owner)),
      source_(std::move(credit))
{
}

Status BackendAllocationGrant::create(std::shared_ptr<AllocationAccount> owner, ResourceBudget::Credit credit,
                                      std::shared_ptr<BackendAllocationGrant> *out)
try {
    if (!owner || out == nullptr || *out || credit.allocated() || credit.amount() == 0) {
        return Status::invalid_argument("backend grant requires unused operation credit");
    }
    *out = std::allocate_shared<BackendAllocationGrant>(GrantControlAllocator<BackendAllocationGrant>(owner, &credit),
                                                        owner, std::move(credit));
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("backend operation credit unavailable");
}
catch (const std::invalid_argument &) {
    return Status::invalid_argument("backend grant requires byte credit");
}

bool BackendAllocationGrant::claim(size_t bytes) noexcept
{
    return owner_->claim_from(source_, bytes);
}

void BackendAllocationGrant::release(size_t bytes) noexcept
{
    owner_->release_to(source_, bytes);
}

void BackendAllocationGrant::record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept
{
    owner_->record_allocation(kind, bytes, acquired);
}

const void *BackendAllocationGrant::domain() const noexcept
{
    return owner_->domain();
}

void BackendAllocationGrant::seal() noexcept
{
    source_.seal();
}
} // namespace crowdb::tree::detail
