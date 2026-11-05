// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "common/test_chunk_root_catalog.h"

#include <functional>

namespace crowdb::tree::test_chunk
{
class TestRootTransport : public detail::ChunkTransport
{
  public:
    explicit TestRootTransport(std::shared_ptr<TestRootCatalog> catalog) : catalog(std::move(catalog))
    {
    }

    std::shared_ptr<TestRootCatalog>                                   catalog;
    detail::MemoryChunkTransport                                       memory;
    std::map<std::array<uint64_t, 2>, detail::ChunkAllocationIdentity> receipts;
    std::function<void()>                                              before_allocate;
    std::map<std::array<uint64_t /*unused*/, 2>, detail::ChunkId>      ids;

    bool fixed_staging_supported() const noexcept override
    {
        return true;
    }

    Status allocate_fixed_chunk(detail::ChunkId id, uint64_t bytes, uint64_t epoch, PagePurpose purpose) override
    {
        if (before_allocate) {
            before_allocate();
        }
        bool protected_allocation = false;
        for (const auto &[stage, record] : catalog->records) {
            (void)stage;
            protected_allocation |= record.chunk_high == id.high && record.chunk_low == id.low &&
                                    record.state == detail::ChunkStageState::Reserved;
        }
        if (!protected_allocation) {
            return Status::corruption();
        }
        detail::ChunkId physical;
        auto            status = memory.allocate_mirror_chunk(bytes, epoch, &physical, purpose);
        if (status.ok()) {
            ids.emplace(std::array{id.high, id.low}, physical);
            receipts.emplace(std::array{id.high, id.low}, detail::ChunkAllocationIdentity{.chunk       = id,
                                                                                          .owner_epoch = epoch,
                                                                                          .created_ms  = physical.low,
                                                                                          .physical_bytes = bytes * 2});
        }
        return status;
    }

    Status inspect_fixed_allocation(detail::ChunkId id, uint64_t epoch,
                                    detail::ChunkAllocationIdentity *out) const override
    {
        auto found = receipts.find({id.high, id.low});
        if (found == receipts.end()) {
            return Status::not_found();
        }
        if (found->second.owner_epoch != epoch) {
            return Status::unavailable();
        }
        *out = found->second;
        return {};
    }

    Status allocate_mirror_chunk(uint64_t /*unused*/, uint64_t /*unused*/, detail::ChunkId * /*unused*/,
                                 PagePurpose /*unused*/) override
    {
        return Status::not_supported();
    }

    Status verify_fixed_read_fence(detail::ChunkId /*unused*/, uint32_t /*unused*/,
                                   detail::ChunkIoContext /*unused*/) const override
    {
        return Status::Ok();
    }

    Status write_fixed_mirror(detail::ChunkId id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t size,
                              detail::ChunkIoContext /*unused*/) override
    {
        return write_mirror(id, mirror, offset, data, size);
    }

    Status read_fixed_mirror(detail::ChunkId id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t size,
                             detail::ChunkIoContext /*unused*/) const override
    {
        return read_mirror(id, mirror, offset, data, size);
    }

    Status write_mirror(detail::ChunkId id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t size) override
    {
        return memory.write_mirror(ids.at({id.high, id.low}), mirror, offset, data, size);
    }

    Status read_mirror(detail::ChunkId id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t size) const override
    {
        return memory.read_mirror(ids.at({id.high, id.low}), mirror, offset, data, size);
    }

    Status advance_write(detail::ChunkId id, uint64_t expected, uint64_t bytes) override
    {
        return memory.advance_write(ids.at({id.high, id.low}), expected, bytes);
    }

    Status seal_chunk(detail::ChunkId id, uint64_t epoch, uint64_t bytes) override
    {
        return memory.seal_chunk(ids.at({id.high, id.low}), epoch, bytes);
    }

    Status query_chunk(detail::ChunkId id, detail::ChunkLayout *out) const override
    {
        return memory.query_chunk(ids.at({id.high, id.low}), out);
    }
};

} // namespace crowdb::tree::test_chunk
