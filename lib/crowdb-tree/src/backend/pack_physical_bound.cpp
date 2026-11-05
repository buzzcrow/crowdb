// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/retired_shared.h"
#include "chunk/chunk_physical_registry_internal.h"

namespace crowdb::tree
{
namespace
{
constexpr size_t kNodeOverhead = (8 * sizeof(void *)) + sizeof(AllocationLease);
}

size_t ChunkPhysicalRegistry::pack_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<ChunkPhysicalPack>() + sizeof(State::Key) +
           sizeof(std::shared_ptr<ChunkPhysicalPack>) + kNodeOverhead +
           (3 * detail::shared_allocation_bound<State::Request>());
}

} // namespace crowdb::tree
