// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_stage.h"
#include "chunk_transport.h"

namespace crowdb::tree::detail
{
// One random 88-bit namespace is prepared at open. Allocation on a CPU worker
// is a bounded atomic counter; exhaustion never wraps or starts a new
// namespace.
class ChunkRuntimeIds
{
  public:
    ChunkRuntimeIds(uint64_t epoch, uint64_t high, uint64_t low) noexcept;
    static Status create(uint64_t epoch, const std::shared_ptr<AllocationBudget> &metadata,
                         std::shared_ptr<ChunkRuntimeIds> *out);
    Status        next(PagePurpose purpose, ChunkStageId *stage, ChunkId *chunk) noexcept;

  private:
    uint64_t              epoch_;
    uint64_t              high_;
    uint64_t              low_;
    std::atomic<uint64_t> next_{1};
};
} // namespace crowdb::tree::detail
