// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "external_owner_bridge.h"

namespace crowdb::tree
{
Status ExternalOwnerBridge::create(const uint8_t *data, size_t initialized_bytes, size_t capacity,
                                   ExternalAllocation::Owner owner, const std::shared_ptr<AllocationBudget> &budget,
                                   std::shared_ptr<const ExternalOwnerBridge> *out) noexcept
try {
    if (out == nullptr || !budget || initialized_bytes > capacity || (capacity != 0 && data == nullptr) || !owner ||
        owner.get_deleter() == nullptr) {
        return Status::invalid_argument();
    }
    auto allocation = ExternalAllocation::create(data, capacity, std::move(owner), budget);
    *out            = std::allocate_shared<ExternalOwnerBridge>(MetadataAllocator<ExternalOwnerBridge>(budget),
                                                                std::move(allocation), initialized_bytes);
    return {};
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (...) {
    return Status::internal_error();
}

Status ExternalOwnerBridge::slice(size_t offset, size_t length, buffer *out) const noexcept
try {
    if (out == nullptr || offset > initialized_bytes_ || length > initialized_bytes_ - offset) {
        return Status::invalid_argument();
    }
    *out = buffer::share_external(allocation_, offset, length);
    return {};
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (...) {
    return Status::internal_error();
}
} // namespace crowdb::tree
