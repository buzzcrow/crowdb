// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backing_io.h"

#include "backend/local/scheduled_page_store.h"
#include "chunk/chunk_backing.h"

namespace crowdb::tree
{
void submit_backing_read(AsyncPageStore &store, std::shared_ptr<const BackingRef> backing, void *buffer, size_t bytes,
                         std::shared_ptr<AllocationBudget> metadata, AsyncCompletion complete)
{
    if (!backing || !backing->reloadable() || buffer == nullptr || bytes != backing->length) {
        complete.complete(Status::invalid_argument("read requires one exact immutable backing"));
        return;
    }
    if (backing->storage == BackingStorage::kDurableBlob) {
        store.submit_read(backing->address, buffer, bytes, std::move(complete));
        return;
    }
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(&store);
    if (scheduled == nullptr || !metadata) {
        complete.complete(Status::not_supported("Chunk reads require a paid backend lane"));
        return;
    }
    const std::span<uint8_t> output(static_cast<uint8_t *>(buffer), bytes);
    auto                     status =
        scheduled->submit_step([pack     = backing->chunk, output,
                                metadata = std::move(metadata)] { return read_chunk_backing(*pack, output, metadata); },
                               complete, bytes, 0);
    if (!status.ok()) {
        complete.complete(std::move(status));
    }
}

void submit_chunk_write(AsyncPageStore &store, std::shared_ptr<ChunkPackBacking> backing,
                        std::span<const uint8_t> input, std::shared_ptr<AllocationBudget> metadata,
                        AsyncCompletion complete)
{
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(&store);
    if (scheduled == nullptr || !backing || !metadata) {
        complete.complete(Status::not_supported("Chunk writes require a paid backend lane"));
        return;
    }
    scheduled->submit_chunk(std::move(backing), input, std::move(complete));
}
} // namespace crowdb::tree
