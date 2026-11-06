// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend_metadata.h"

#include <algorithm>
#include <limits>

namespace crowdb::tree::detail
{
namespace
{
struct Header
{
    std::shared_ptr<AllocationBudget> budget;
    size_t                            bytes;
    size_t                            alignment;
};
} // namespace

BackendObjectStorage::BackendObjectStorage(size_t bytes, size_t alignment, std::shared_ptr<AllocationBudget> budget)
{
    alignment         = std::max(alignment, alignof(Header));
    const auto prefix = (sizeof(Header) + sizeof(Header *) + alignment - 1) & ~(alignment - 1);
    if (bytes > std::numeric_limits<size_t>::max() - prefix) {
        throw std::bad_array_new_length();
    }
    bytes += prefix;
    if (budget && !budget->claim(bytes)) {
        throw std::bad_alloc();
    }
    try {
        auto *base = static_cast<std::byte *>(::operator new(bytes, std::align_val_t{alignment}));
        auto *header =
            ::new (static_cast<void *>(base)) Header{.budget = budget, .bytes = bytes, .alignment = alignment};
        pointer_ = base + prefix;
        ::new (static_cast<void *>(base + prefix - sizeof(Header *))) Header *(header);
        if (budget) {
            budget->record_allocation(AllocationKind::kMetadata, bytes, true);
        }
    }
    catch (...) {
        if (budget) {
            budget->release(bytes);
        }
        throw;
    }
}

BackendObjectStorage::~BackendObjectStorage()
{
    destroy(pointer_);
}

void BackendObjectStorage::destroy(void *pointer) noexcept
{
    if (pointer == nullptr) {
        return;
    }
    auto      *header    = *(reinterpret_cast<Header **>(pointer) - 1);
    auto       budget    = std::move(header->budget);
    const auto bytes     = header->bytes;
    const auto alignment = header->alignment;
    std::destroy_at(header);
    ::operator delete(header, std::align_val_t{alignment});
    if (budget) {
        budget->record_allocation(AllocationKind::kMetadata, bytes, false);
        budget->release(bytes);
    }
}
} // namespace crowdb::tree::detail
