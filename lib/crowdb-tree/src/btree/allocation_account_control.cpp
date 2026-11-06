// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account.h"

#include <cassert>
#include <limits>
#include <new>

namespace crowdb::tree::detail
{
// The stable budget survives until its last weak control reference. Its control
// allocator borrows the budget, so neither object creates a strong owner cycle.
template <class T> class AccountControlAllocator
{
  public:
    using value_type = T;

    explicit AccountControlAllocator(AllocationAccount *owner) noexcept : owner_(owner)
    {
    }

    template <class U> AccountControlAllocator(const AccountControlAllocator<U> &other) noexcept : owner_(other.owner())
    {
    }

    [[nodiscard]] T *allocate(size_t count)
    {
        if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        const auto bytes = count * sizeof(T);
        if (!owner_->claim(bytes)) {
            throw std::bad_alloc();
        }
        try {
            auto *storage = static_cast<T *>(::operator new(bytes, std::align_val_t{alignof(T)}));
            owner_->record_allocation(AllocationKind::kMetadata, bytes, true);
            return storage;
        }
        catch (...) {
            owner_->release(bytes);
            throw;
        }
    }

    void deallocate(T *storage, size_t count) noexcept
    {
        owner_->destroy_control(storage, count * sizeof(T), alignof(T));
    }

    [[nodiscard]] AllocationAccount *owner() const noexcept
    {
        return owner_;
    }

    template <class U> bool operator==(const AccountControlAllocator<U> &other) const noexcept
    {
        return owner_ == other.owner();
    }

  private:
    AllocationAccount *owner_;
};

void AllocationAccount::destroy_control(void *storage, size_t bytes, size_t alignment) noexcept
{
    // Every allocation owner also holds a strong budget reference. Once the
    // last control is freed, only the budget object and that control remain.
    const auto total    = bytes + sizeof(AllocationAccount);
    auto       account  = std::move(account_);
    const auto category = category_;
    assert(ingress_.load() == 0);
    assert(allocated_.load() == total);
    assert(physical_[static_cast<size_t>(AllocationKind::kMetadata)].load() == total);
    ::operator delete(storage, std::align_val_t{alignment});
    delete this;
    ResourceBudget::record_allocation(account, AllocationKind::kMetadata, total, false);
    ResourceBudget::release_allocation(account, category, total);
}

Status AllocationAccount::create(std::shared_ptr<ResourceBudget::Account> account, BudgetClass category,
                                 std::shared_ptr<AllocationAccount> *out)
{
    if (!account || out == nullptr || category == BudgetClass::kIo || category >= BudgetClass::kCount) {
        return Status::invalid_argument("backend metadata requires a byte account and output");
    }
    if (!ResourceBudget::accepting(account)) {
        return Status::unavailable("allocation account admission is closed");
    }
    return build(std::move(account), category, out);
}

Status AllocationAccount::create_unbound(std::shared_ptr<AllocationAccount> *out)
{
    if (out == nullptr) {
        return Status::invalid_argument("backend allocation account requires output");
    }
    return build({}, BudgetClass::kRouting, out);
}

Status AllocationAccount::build(std::shared_ptr<ResourceBudget::Account> account, BudgetClass category,
                                std::shared_ptr<AllocationAccount> *out)
{
    constexpr auto bytes = sizeof(AllocationAccount);
    if (account && !ResourceBudget::claim_allocation(account, category, bytes)) {
        return Status::resource_exhausted("backend allocation account reserve unavailable");
    }
    AllocationAccount *owner = nullptr;
    try {
        owner = new AllocationAccount(account, category);
        owner->allocated_.store(bytes, std::memory_order_relaxed);
        owner->apply_record(static_cast<size_t>(AllocationKind::kMetadata), bytes, true);
        std::shared_ptr<AllocationAccount> result(
            owner, [](AllocationAccount *) noexcept {}, AccountControlAllocator<AllocationAccount>(owner));
        *out = std::move(result);
        return Status::Ok();
    }
    catch (const std::bad_alloc &) {
        const bool allocated = owner != nullptr;
        delete owner;
        if (allocated) {
            ResourceBudget::record_allocation(account, AllocationKind::kMetadata, bytes, false);
        }
        ResourceBudget::release_allocation(account, category, bytes);
        return Status::resource_exhausted("backend allocation account allocation failed");
    }
}
} // namespace crowdb::tree::detail
