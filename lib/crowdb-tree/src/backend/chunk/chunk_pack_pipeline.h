// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_cancellation.h"
#include "crowdb-tree/backend/async_page_store.h"

#include <memory>

namespace crowdb::tree::detail
{

class ChunkPageStore;

class ChunkPackPipeline
{
  public:
    static std::shared_ptr<ChunkPackPipeline> start(ChunkPageStore *store, uint64_t expected_generation,
                                                    ChunkCancellation cancellation, AsyncCompletion completion,
                                                    Status *start_status);

    virtual ~ChunkPackPipeline() = default;

    virtual Status finish(ChunkCancellation cancellation, Status io_status) = 0;
};

} // namespace crowdb::tree::detail
