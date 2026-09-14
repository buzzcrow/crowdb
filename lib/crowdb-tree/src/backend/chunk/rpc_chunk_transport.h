// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_transport.h"
#include "crowdb-tree/c_api.h"

#include <memory>

namespace crowdb::tree::detail
{

// Direct ChunkDB/DiskIO FlatBuffers transport over the callback C++ RPC slab.
// Wire types and RPC implementation headers remain confined to its translation
// unit so neither the public tree ABI nor local backend objects depend on them.
class RpcChunkTransport final : public ChunkTransport
{
  public:
    explicit RpcChunkTransport(const ct_chunk_rpc_transport_options &options);
    ~RpcChunkTransport() override;

    Status allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id) override;
    Status write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                        size_t length) override;
    void   submit_write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                               size_t length, ChunkTransportCompletion completion) override;
    Status advance_write(ChunkId chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes) override;
    Status read_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data,
                       size_t length) const override;
    Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const override;
    Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes) override;

    [[nodiscard]] bool valid() const;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace crowdb::tree::detail
