// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/status.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace crowdb::tree::detail
{

struct ChunkLayout
{
    uint64_t chunk_id           = 0;
    uint64_t logical_capacity   = 0;
    uint64_t acknowledged_bytes = 0;
    bool     sealed             = false;
};

class ChunkTransport
{
  public:
    virtual ~ChunkTransport() = default;

    virtual Status allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, uint64_t *chunk_id) = 0;
    virtual Status write_mirror(uint64_t chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                                size_t length)                                                                = 0;
    virtual Status advance_write(uint64_t chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes)     = 0;
    virtual Status read_mirror(uint64_t chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data,
                               size_t length) const                                                           = 0;
    virtual Status query_chunk(uint64_t chunk_id, ChunkLayout *layout) const                                  = 0;
    virtual Status seal_chunk(uint64_t chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes)           = 0;
};

// Lock-free immutable-snapshot transport for tests and embedded use. It models
// the same allocation, three-mirror write, acknowledged-cursor, and seal
// boundaries as the production RPC transport.
class MemoryChunkTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, uint64_t *chunk_id) override;
    Status write_mirror(uint64_t chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                        size_t length) override;
    Status advance_write(uint64_t chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes) override;
    Status read_mirror(uint64_t chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data,
                       size_t length) const override;
    Status query_chunk(uint64_t chunk_id, ChunkLayout *layout) const override;
    Status seal_chunk(uint64_t chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes) override;

    void corrupt_mirror(uint64_t chunk_id, uint32_t mirror_index, uint64_t offset);

    void inject_unavailable(bool unavailable)
    {
        unavailable_.store(unavailable, std::memory_order_release);
    }

  private:
    struct Chunk
    {
        ChunkLayout                         layout;
        uint64_t                            owner_epoch = 0;
        std::array<std::vector<uint8_t>, 3> mirrors;
    };

    using Chunks = std::vector<Chunk>;

    template <typename Mutation> Status mutate(uint64_t chunk_id, Mutation mutation);

    std::atomic<uint64_t>                      next_chunk_id_{1};
    std::atomic<std::shared_ptr<const Chunks>> chunks_;
    std::atomic<bool>                          unavailable_{false};
};

} // namespace crowdb::tree::detail
