// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "chunk/chunk_fixed_pack.h"
#include "chunk/chunk_frame.h"
#include "chunk/chunk_physical_registry.h"
#include "chunk/chunk_pin_release_internal.h"

namespace crowdb::tree::detail
{
std::optional<size_t> ChunkFixedPackStore::write_allocation_bound(size_t bytes) noexcept
{
    if (bytes == 0 || bytes > kMaxChunkPackBytes) {
        return {};
    }
    return AllocationBound(chunk_framed_length(bytes))
        .add(shared_allocation_bound<ChunkPackWindow>() + ChunkPhysicalRegistry::pack_allocation_bound())
        .add(12 * sizeof(ChunkStagePack))
        .add(2 * (100 + 16))
        .value();
}

std::optional<size_t> ChunkFixedPackStore::read_allocation_bound(size_t bytes) noexcept
{
    if (bytes == 0 || bytes > kMaxChunkPackBytes) {
        return {};
    }
    return AllocationBound(chunk_framed_length(bytes))
        .add(bytes)
        .add(2 * shared_allocation_bound<ChunkPackWindow>())
        .value();
}

std::optional<size_t> ChunkFixedPackStore::read_into_allocation_bound(size_t bytes) noexcept
{
    if (bytes == 0 || bytes > kMaxChunkPackBytes) {
        return {};
    }
    return AllocationBound(chunk_framed_length(bytes)).add(shared_allocation_bound<ChunkPackWindow>()).value();
}

size_t ChunkPinReleaseQueue::pin_allocation_bound() noexcept
{
    return sizeof(ChunkPinReleaseNode) + shared_allocation_bound<ChunkPinOwner>();
}
} // namespace crowdb::tree::detail
