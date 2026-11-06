// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "chunk_backing.h"
#include "crowdb-tree/backend/async_page_store.h"

#include <array>
#include <atomic>

namespace crowdb::tree
{
class ScheduledPageStore;

// Two independent backend writes drain ready pages without a batching timer.
// Readers use the backend directly and never enter this queue.
class ChunkWriteQueue : public std::enable_shared_from_this<ChunkWriteQueue>
{
    struct Request;
    struct Batch;

  public:
    explicit ChunkWriteQueue(std::shared_ptr<AllocationBudget> metadata) : metadata_(std::move(metadata))
    {
    }

    void submit(std::shared_ptr<ScheduledPageStore> store, std::shared_ptr<ChunkPackBacking> backing,
                std::span<const uint8_t> input, AsyncCompletion complete);

  private:
    void push(Request *request) noexcept;
    void start(size_t pipe) noexcept;
    void wake() noexcept;

    std::shared_ptr<AllocationBudget> metadata_;
    std::atomic<Request *>            pending_{nullptr};
    std::array<std::atomic<bool>, 2>  active_{};
};
} // namespace crowdb::tree
