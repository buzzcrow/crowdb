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

struct ChunkId
{
    uint64_t high = 0;
    uint64_t low  = 0;

    constexpr ChunkId() = default;

    constexpr ChunkId(uint64_t compact) : low(compact)
    {
    }

    constexpr ChunkId(uint64_t high_bits, uint64_t low_bits) : high(high_bits), low(low_bits)
    {
    }

    constexpr bool operator==(const ChunkId &) const = default;

    [[nodiscard]] constexpr bool empty() const
    {
        return high == 0 && low == 0;
    }
};

struct ChunkLayout
{
    ChunkId  chunk_id;
    uint64_t logical_capacity   = 0;
    uint64_t acknowledged_bytes = 0;
    bool     sealed             = false;
};

class ChunkTransport
{
  public:
    virtual ~ChunkTransport() = default;

    virtual Status allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id) = 0;
    virtual Status write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                                size_t length)                                                               = 0;
    virtual Status advance_write(ChunkId chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes)     = 0;
    virtual Status read_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data,
                               size_t length) const                                                          = 0;
    virtual Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const                                  = 0;
    virtual Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes)           = 0;
};

// Lock-free immutable-snapshot transport for tests and embedded use. It models
// the same allocation, three-mirror write, acknowledged-cursor, and seal
// boundaries as the production RPC transport.
class MemoryChunkTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id) override;
    Status write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                        size_t length) override;
    Status advance_write(ChunkId chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes) override;
    Status read_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data,
                       size_t length) const override;
    Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const override;
    Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes) override;

    void corrupt_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset);

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

    template <typename Mutation> Status mutate(ChunkId chunk_id, Mutation mutation);

    std::atomic<uint64_t>                      next_chunk_id_{1};
    std::atomic<std::shared_ptr<const Chunks>> chunks_;
    std::atomic<bool>                          unavailable_{false};
};

} // namespace crowdb::tree::detail
