// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_runtime_ids.h"

#include "chunk_purpose.h"

#include <random>

namespace crowdb::tree::detail
{
ChunkRuntimeIds::ChunkRuntimeIds(uint64_t epoch, uint64_t high, uint64_t low) noexcept
    : epoch_(epoch),
      high_(high & 0x00FFFFFFFFFFFFFFULL),
      low_(low & 0xFFFFFFFF00000000ULL)
{
}

Status ChunkRuntimeIds::create(uint64_t epoch, const std::shared_ptr<AllocationBudget> &metadata,
                               std::shared_ptr<ChunkRuntimeIds> *out)
try {
    if (epoch == 0 || !metadata || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    std::random_device random;
    const auto         high = (uint64_t{random()} << 32U) | random();
    const auto         low  = (uint64_t{random()} << 32U) | random();
    *out = std::allocate_shared<ChunkRuntimeIds>(MetadataAllocator<ChunkRuntimeIds>(metadata), epoch, high, low);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (const std::exception &) {
    return Status::unavailable("Chunk operation namespace could not be initialized");
}

Status ChunkRuntimeIds::next(PagePurpose purpose, ChunkStageId *stage, ChunkId *chunk) noexcept
{
    if (!valid_page_purpose(purpose) || stage == nullptr || chunk == nullptr || epoch_ == 0) {
        return Status::invalid_argument();
    }
    auto value = next_.load(std::memory_order_relaxed);
    do {
        if (value > UINT32_MAX) {
            return Status::resource_exhausted();
        }
    } while (!next_.compare_exchange_weak(value, value + 1, std::memory_order_relaxed));
    *stage = {.owner_epoch = epoch_, .operation_high = high_, .operation_low = low_ | value};
    *chunk = {(uint64_t{static_cast<uint32_t>(purpose)} << 56U) | high_, low_ | value};
    return Status::Ok();
}
} // namespace crowdb::tree::detail
