// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/backing_ref.h"
#include "crowdb-tree/backend/async_page_store.h"

namespace crowdb::tree
{
// All paths retain the callback's exact buffer owner through the backend tail.
// Chunk reads require a scheduled backend; they never fall back to byte offsets.
void submit_backing_read(AsyncPageStore &store, std::shared_ptr<const BackingRef> backing, void *buffer, size_t bytes,
                         std::shared_ptr<AllocationBudget> metadata, AsyncCompletion complete);
void submit_chunk_write(AsyncPageStore &store, std::shared_ptr<ChunkPackBacking> backing,
                        std::span<const uint8_t> input, std::shared_ptr<AllocationBudget> metadata,
                        AsyncCompletion complete);
} // namespace crowdb::tree
