// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_memory_resource.h"

#include <new>

namespace crowdb::tree::detail
{
void *AllocationMemoryResource::do_allocate(size_t bytes, size_t alignment)
{
    if (!budget_ || !budget_->claim(bytes)) {
        throw std::bad_alloc();
    }
    try {
        auto *pointer = ::operator new(bytes, std::align_val_t{alignment});
        budget_->record_allocation(kind_, bytes, true);
        return pointer;
    }
    catch (...) {
        budget_->release(bytes);
        throw;
    }
}

void AllocationMemoryResource::do_deallocate(void *pointer, size_t bytes, size_t alignment)
{
    ::operator delete(pointer, std::align_val_t{alignment});
    budget_->record_allocation(kind_, bytes, false);
    budget_->release(bytes);
}

bool AllocationMemoryResource::do_is_equal(const std::pmr::memory_resource &other) const noexcept
{
    return this == &other;
}
} // namespace crowdb::tree::detail
