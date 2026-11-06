// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_reservation.h"

#include <algorithm>
#include <cassert>
#include <stdexcept>

namespace crowdb::tree::detail
{
namespace
{
// allocate_shared first allocates its control/object storage, then moves the
// remaining credit into AllocationReservation. Only allocate() borrows that
// stack credit; deallocate() uses the independent token embedded in storage.
template <class T> class CreditAllocator
{
    struct Header
    {
        ResourceBudget::Credit allocation;
    };

    static constexpr size_t alignment = std::max(alignof(T), alignof(Header));
    static constexpr size_t offset    = (sizeof(Header) + alignment - 1) & ~(alignment - 1);

  public:
    using value_type = T;

    explicit CreditAllocator(ResourceBudget::Credit *source) : source_(source)
    {
    }

    template <class U> CreditAllocator(const CreditAllocator<U> &other) : source_(other.source())
    {
    }

    [[nodiscard]] ResourceBudget::Credit *source() const noexcept
    {
        return source_;
    }

    [[nodiscard]] T *allocate(size_t count)
    {
        if (count > (SIZE_MAX - offset) / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        const auto bytes  = (count * sizeof(T)) + offset;
        auto       credit = source_->allocate(bytes);
        if (!credit) {
            throw std::bad_alloc();
        }
        auto                       *base = static_cast<std::byte *>(::operator new(bytes, std::align_val_t{alignment}));
        [[maybe_unused]] const bool marked = credit->mark_allocation(AllocationKind::kMetadata);
        assert(marked);
        std::construct_at(reinterpret_cast<Header *>(base), Header{std::move(*credit)});
        return reinterpret_cast<T *>(base + offset);
    }

    void deallocate(T *pointer, size_t /*count*/) noexcept
    {
        auto *base   = reinterpret_cast<std::byte *>(pointer) - offset;
        auto *header = std::launder(reinterpret_cast<Header *>(base));
        auto  credit = std::move(header->allocation);
        std::destroy_at(header);
        ::operator delete(base, std::align_val_t{alignment});
    }

    template <class U> bool operator==(const CreditAllocator<U> &other) const noexcept
    {
        return source_ == other.source();
    }

  private:
    ResourceBudget::Credit *source_;
};
} // namespace

std::shared_ptr<AllocationReservation> AllocationReservation::create(ResourceBudget::Credit credit)
{
    if (!credit.account_ || credit.allocated_ || credit.category_ == static_cast<size_t>(BudgetClass::kIo)) {
        throw std::invalid_argument("reservation bootstrap requires unconsumed byte credit");
    }
    return std::allocate_shared<AllocationReservation>(CreditAllocator<AllocationReservation>(&credit),
                                                       std::move(credit));
}

std::shared_ptr<AllocationReservation>
AllocationReservation::create(const std::shared_ptr<AllocationReservation> &parent, size_t bytes)
{
    auto credit = parent ? parent->take_reserved(bytes) : std::nullopt;
    if (!credit) {
        throw std::bad_alloc();
    }
    try {
        return std::allocate_shared<AllocationReservation>(MetadataAllocator<AllocationReservation>(parent),
                                                           std::move(*credit), parent);
    }
    catch (...) {
        if (credit->amount_ != 0 && parent->restore_reserved(credit->amount_)) {
            credit->amount_ = 0;
        }
        throw;
    }
}
} // namespace crowdb::tree::detail
