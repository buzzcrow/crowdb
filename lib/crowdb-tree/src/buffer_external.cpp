// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/buffer.h"
#include "crowdb-tree/external_allocation.h"

namespace crowdb::tree
{
namespace
{
struct ExternalSliceHolder
{

    std::shared_ptr<const ExternalAllocation> allocation;
};
} // namespace

buffer buffer::share_external(std::shared_ptr<const ExternalAllocation> allocation, size_t offset, size_t len)
{
    if (!allocation || offset > allocation->capacity() || len > allocation->capacity() - offset) {
        throw std::out_of_range("external slice exceeds allocation capacity");
    }
    using Holder = ExternalSliceHolder;
    MetadataAllocator<Holder> allocator(allocation->budget());
    auto                     *holder = allocator.allocate(1);
    try {
        std::construct_at(holder, std::move(allocation));
    }
    catch (...) {
        allocator.deallocate(holder, 1);
        throw;
    }
    const auto *identity    = holder->allocation.get();
    const auto *data        = offset == 0 ? identity->data() : identity->data() + offset;
    auto        result      = wrap_external(data, len, holder, [](void *context) {
        auto                     *owner = static_cast<Holder *>(context);
        MetadataAllocator<Holder> allocator(owner->allocation->budget());
        std::destroy_at(owner);
        allocator.deallocate(owner, 1);
    });
    result.ext_.allocation_ = identity;
    return result;
}
} // namespace crowdb::tree
