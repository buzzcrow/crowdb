// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "budget_memory_resource.h"

#include <algorithm>
#include <stdexcept>

namespace crowdb::tree::detail
{
BudgetMemoryResource::BudgetMemoryResource(std::pmr::memory_resource *upstream) : upstream_(upstream)
{
    if (upstream_ == nullptr) {
        throw std::invalid_argument("budget memory resource requires an upstream allocator");
    }
}

BudgetMemoryResource::Scope::Scope(BudgetMemoryResource &resource, std::shared_ptr<AllocationBudget> budget)
    : resource_(resource),
      previous_(std::exchange(resource.budget_, std::move(budget)))
{
}

BudgetMemoryResource::Scope::~Scope()
{
    resource_.budget_ = std::move(previous_);
}

void *BudgetMemoryResource::do_allocate(size_t bytes, size_t alignment)
{
    alignment = std::max(alignment, alignof(Header));
    if (alignment - 1 > SIZE_MAX - sizeof(Header)) {
        throw std::bad_alloc();
    }
    const auto offset = (sizeof(Header) + alignment - 1) & ~(alignment - 1);
    if (bytes > SIZE_MAX - offset) {
        throw std::bad_alloc();
    }
    const auto      total = bytes + offset;
    AllocationLease credit(budget_, total, AllocationKind::kMetadata);
    auto           *base = static_cast<std::byte *>(upstream_->allocate(total, alignment));
    std::construct_at(reinterpret_cast<Header *>(base), Header{std::move(credit)});
    retained_bytes_.fetch_add(total, std::memory_order_relaxed);
    return base + offset;
}

void BudgetMemoryResource::do_deallocate(void *pointer, size_t bytes, size_t alignment)
{
    alignment         = std::max(alignment, alignof(Header));
    const auto offset = (sizeof(Header) + alignment - 1) & ~(alignment - 1);
    auto      *base   = static_cast<std::byte *>(pointer) - offset;
    auto      *header = reinterpret_cast<Header *>(base);
    auto       credit = std::move(header->credit);
    std::destroy_at(header);
    upstream_->deallocate(base, bytes + offset, alignment);
    retained_bytes_.fetch_sub(bytes + offset, std::memory_order_relaxed);
}

bool BudgetMemoryResource::do_is_equal(const std::pmr::memory_resource &other) const noexcept
{
    return this == &other;
}
} // namespace crowdb::tree::detail
