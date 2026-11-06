// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "chunk/chunk_backing.h"
#include "chunk/chunk_frame.h"
#include "chunk/chunk_physical_registry.h"

namespace crowdb::tree
{
bool ChunkBackingDomain::supported() const noexcept
{
    return backend_id != 0 && tree != 0 && epoch != 0 && mirrors != 0 && mirrors <= detail::kMaxMirrorCopies &&
           max_chunk_bytes != 0 && max_chunk_bytes <= detail::kMaxChunkPackBytes && catalog &&
           catalog->stage_schema_supported() && catalog->protection() != nullptr && catalog->pin_release_queue() &&
           transport && transport->fixed_staging_supported();
}

bool chunk_backing_matches(const ChunkPackBacking &backing, uint64_t domain, uint64_t length) noexcept
{
    const auto &pack = backing.pack;
    return backing.domain && backing.domain->supported() && backing.domain->backend_id == domain && backing.pin &&
           pack.stage.owner_epoch != 0 && (pack.stage.operation_high != 0 || pack.stage.operation_low != 0) &&
           !pack.chunk.empty() && length != 0 && length == pack.length && length <= detail::kMaxChunkPackBytes &&
           detail::valid_page_purpose(pack.purpose) && detail::chunk_purpose(pack.chunk) == pack.purpose &&
           pack.offset <= UINT64_MAX - detail::chunk_framed_length(pack.length);
}

std::array<uint64_t, 8> chunk_backing_identity(const ChunkPackBacking &backing) noexcept
{
    const auto &pack = backing.pack;
    return {pack.stage.owner_epoch,
            pack.stage.operation_high,
            pack.stage.operation_low,
            pack.chunk.high,
            pack.chunk.low,
            pack.offset,
            pack.length,
            (uint64_t{pack.checksum} << 32U) | static_cast<uint32_t>(pack.purpose)};
}

bool same_chunk_backing(const std::shared_ptr<const ChunkPackBacking> &left,
                        const std::shared_ptr<const ChunkPackBacking> &right) noexcept
{
    if (!left || !right) {
        return left == right;
    }
    return chunk_backing_identity(*left) == chunk_backing_identity(*right) && left->pack.length == right->pack.length &&
           left->pack.checksum == right->pack.checksum && left->pack.purpose == right->pack.purpose && left->domain &&
           right->domain && left->domain->backend_id == right->domain->backend_id;
}

size_t chunk_backing_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<ChunkPackBacking>() + detail::ChunkPinReleaseQueue::pin_allocation_bound() +
           ChunkPhysicalRegistry::pack_allocation_bound();
}

Status read_chunk_backing(const ChunkPackBacking &backing, std::span<uint8_t> output,
                          const std::shared_ptr<AllocationBudget> &budget)
{
    return backing.read != nullptr ? backing.read(backing, output, budget)
                                   : Status::corruption("immutable pack has no read capability");
}

Status write_chunk_backing(ChunkPackBacking &backing, std::span<const uint8_t> input,
                           const std::shared_ptr<AllocationBudget> &budget)
{
    return backing.write != nullptr ? backing.write(backing, input, budget)
                                    : Status::invalid_argument("immutable pack has no write capability");
}
} // namespace crowdb::tree
