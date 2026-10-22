// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA
namespace crowdb::rpc
{
struct RdmaBufferPool::Arena : BufferPool, std::enable_shared_from_this<Arena>
{
    struct Entry
    {
        std::atomic<Buffer *> buffer{nullptr};
        std::atomic<uint32_t> next{0};
    };

    std::shared_ptr<void>                registration_owner;
    ibv_mr                              *mr     = nullptr;
    void                                *region = nullptr;
    uint64_t                             bytes;
    uint32_t                             capacity;
    std::unique_ptr<Entry[]>             entries;
    std::array<std::atomic<uint64_t>, 4> free_heads{};
    std::atomic<uint64_t>                unused_entries{0};
    std::atomic<uint32_t>                next_entry{0};
    std::atomic<uint32_t>                outstanding{0};
    std::atomic<uint64_t>                next_offset{0};
    std::atomic<uint64_t>                quarantined{0};

    Arena(ibv_pd *pd, uint64_t size, uint32_t count, std::shared_ptr<void> owner);
    ~Arena() override;
    Buffer  *alloc(uint32_t requested) override;
    void     recycle(Buffer *buffer) override;
    Buffer  *fresh(uint32_t size);
    Buffer  *pop(size_t bucket);
    void     push(Buffer *buffer);
    uint32_t pop_entry(std::atomic<uint64_t> &head);
    void     push_entry(std::atomic<uint64_t> &head, uint32_t index);
    void     quarantine_inflight();
};
} // namespace crowdb::rpc
#endif
