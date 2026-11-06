// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_io_context.h"
#include "crowdb-common/atomic_shared_ptr.h"
#include "crowdb-tree/backend/page_purpose.h"
#include "crowdb-tree/status.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace crowdb::tree
{
class AllocationBudget;
class ChunkPhysicalRegistry;
} // namespace crowdb::tree

namespace crowdb::tree::detail
{

using common::AtomicSharedPtr;

inline constexpr uint32_t kMaxMirrorCopies = 5;

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

    constexpr bool operator==(const ChunkId /*unused*/ &) const = default;

    [[nodiscard]] constexpr bool empty() const
    {
        return high == 0 && low == 0;
    }
};

// One immutable physical allocation, including every rounded mirror segment.
struct ChunkAllocationIdentity
{
    ChunkId  chunk;
    uint64_t owner_epoch    = 0;
    uint64_t created_ms     = 0;
    uint64_t physical_bytes = 0;
};

struct ChunkLayout
{
    ChunkId  chunk_id;
    uint64_t logical_capacity   = 0;
    uint64_t acknowledged_bytes = 0;
    bool     sealed             = false;
};

struct ChunkTransportCompletion
{
    void (*complete_fn)(void *context, Status status) = nullptr;
    bool (*stop_requested_fn)(void *context)          = nullptr;
    void *context                                     = nullptr;

    void complete(Status status) const
    {
        if (complete_fn != nullptr) {
            complete_fn(context, std::move(status));
        }
    }

    [[nodiscard]] bool stop_requested() const
    {
        return stop_requested_fn != nullptr && stop_requested_fn(context);
    }
};

struct ChunkRegistryPool;

class ChunkTransport
{
  public:
    virtual ~ChunkTransport();
    // Host open only. Registry identity is stable across writer incarnations
    // for this transport, retirement domain and logical Tree ID.
    Status physical_registry(uint64_t tree, const std::shared_ptr<AllocationBudget> &metadata,
                             std::shared_ptr<ChunkPhysicalRegistry> *out);

    virtual Status allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id,
                                         PagePurpose purpose) = 0;
    // The ordered pack planner prepares capacity before submitting mirror I/O.
    // Allocation capacity is a ceiling; RPC-backed chunks grow in small strips.
    virtual Status ensure_capacity(ChunkId chunk_id, uint64_t required_bytes);

    // Fixed identity is durably protected before this call. An unknown result
    // never authorizes generating another identity or releasing that protection.
    [[nodiscard]] virtual bool fixed_staging_supported() const noexcept
    {
        return false;
    }

    virtual Status allocate_fixed_chunk(ChunkId /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/,
                                        PagePurpose /*unused*/)
    {
        return Status::not_supported();
    }

    [[nodiscard]] virtual Status verify_fixed_read_fence(ChunkId /*unused*/, uint32_t /*unused*/,
                                                         ChunkIoContext /*unused*/) const
    {
        return Status::not_supported();
    }

    virtual Status write_fixed_mirror(ChunkId /*unused*/, uint32_t /*unused*/, uint64_t /*unused*/,
                                      const uint8_t * /*unused*/, size_t /*unused*/, ChunkIoContext /*unused*/)
    {
        return Status::not_supported();
    }

    virtual Status read_fixed_mirror(ChunkId /*unused*/, uint32_t /*unused*/, uint64_t /*unused*/, uint8_t * /*unused*/,
                                     size_t /*unused*/, ChunkIoContext /*unused*/) const
    {
        return Status::not_supported();
    }

    // Called only after the creator's fixed request ages are excluded. A
    // successful barrier waits for all already accepted ordered disk writes.
    [[nodiscard]] virtual Status drain_fixed_chunk(ChunkId /*unused*/) const
    {
        return Status::not_supported();
    }

    // Always query authority, not a cached route/layout. A different physical
    // owner cannot be reclaimed using an old stage's logical reference.
    virtual Status inspect_fixed_allocation(ChunkId /*unused*/, uint64_t /*unused*/,
                                            ChunkAllocationIdentity * /*unused*/) const
    {
        return Status::not_supported();
    }

    // Success means ChunkDB has completed physical deletion. Lost replies retain
    // protection and may retry the same immutable identity.
    virtual Status delete_chunk(ChunkId /*unused*/)
    {
        return Status::not_supported();
    }

    virtual Status write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                                size_t length) = 0;
    virtual void   submit_write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                                       size_t length, ChunkTransportCompletion completion);
    virtual Status advance_write(ChunkId chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes) = 0;
    virtual Status read_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data,
                               size_t length) const                                                      = 0;
    virtual Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const                              = 0;
    virtual Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes)       = 0;

  private:
    std::atomic<ChunkRegistryPool *> registry_pool_{nullptr};
};

// Lock-free immutable-snapshot transport for tests and embedded use. It models
// the same allocation, mirror write, acknowledged-cursor, and seal
// boundaries as the production RPC transport.
class MemoryChunkTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id,
                                 PagePurpose purpose) override;
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
        ChunkLayout                                        layout;
        uint64_t                                           owner_epoch = 0;
        std::array<std::vector<uint8_t>, kMaxMirrorCopies> mirrors;
    };

    using Chunks = std::vector<Chunk>;

    template <typename Mutation> Status mutate(ChunkId chunk_id, Mutation mutation);

    std::atomic<uint64_t>         next_chunk_id_{1};
    AtomicSharedPtr<const Chunks> chunks_;
    std::atomic<bool>             unavailable_{false};
};

} // namespace crowdb::tree::detail
