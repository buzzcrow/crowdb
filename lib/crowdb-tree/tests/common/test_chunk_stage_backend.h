// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "common/test_stage_catalog.h"

#include <gtest/gtest.h>

#include <functional>

namespace crowdb::tree::detail
{
class TestFixedTransport final : public ChunkTransport
{
  public:
    explicit TestFixedTransport(std::shared_ptr<TestStageCatalog> catalog) : catalog(std::move(catalog))
    {
    }

    std::shared_ptr<TestStageCatalog> catalog;
    MemoryChunkTransport              memory;
    bool                              unknown_allocate = false;
    bool                              fail_fence       = false;
    bool                              fail_write       = false;
    size_t                            allocations      = 0;

    uint64_t              physical_epoch = 1;
    std::function<void()> before_drain;
    bool                  absent_after_delete = false;
    bool                  fail_drain          = false;
    mutable size_t        drains              = 0;
    bool                  unknown_delete      = false;
    bool                  physically_deleted  = false;
    size_t                deletions           = 0;

    [[nodiscard]] Status drain_fixed_chunk(ChunkId /*unused*/) const override
    {
        ++drains;
        if (before_drain) {
            before_drain();
        }
        if (physically_deleted && absent_after_delete) {
            return Status::not_found();
        }
        return fail_drain ? Status::unavailable() : Status::Ok();
    }

    Status inspect_fixed_allocation(ChunkId id, uint64_t epoch, ChunkAllocationIdentity *out) const override
    {
        if (physically_deleted && absent_after_delete) {
            return Status::not_found();
        }
        if (epoch != physical_epoch) {
            return Status::unavailable();
        }
        *out = {.chunk = id, .owner_epoch = epoch, .created_ms = 1, .physical_bytes = 8192};
        return Status::Ok();
    }

    Status delete_chunk(ChunkId /*id*/) override
    {
        ++deletions;
        physically_deleted = true;
        return unknown_delete ? Status::unavailable() : Status::Ok();
    }

    bool fixed_staging_supported() const noexcept override
    {
        return true;
    }

    Status allocate_fixed_chunk(ChunkId id, uint64_t capacity, uint64_t epoch, PagePurpose purpose) override
    {
        if (!catalog->record.has_value()) {
            ADD_FAILURE() << "allocation submitted before its durable stage";
            return Status::corruption();
        }
        EXPECT_EQ(catalog->record->state, ChunkStageState::Reserved);
        EXPECT_EQ(catalog->record->chunk_high, id.high);
        EXPECT_EQ(catalog->record->chunk_low, id.low);
        ++allocations;
        ChunkId result;
        auto    status = memory.allocate_mirror_chunk(capacity, epoch, &result, purpose);
        EXPECT_EQ(result, id);
        return unknown_allocate ? Status::unavailable() : status;
    }

    Status allocate_mirror_chunk(uint64_t /*unused*/, uint64_t /*unused*/, ChunkId * /*unused*/,
                                 PagePurpose /*unused*/) override
    {
        return Status::not_supported();
    }

    [[nodiscard]] Status verify_fixed_read_fence(ChunkId /*unused*/, uint32_t /*unused*/,
                                                 ChunkIoContext /*unused*/) const override
    {
        return fail_fence ? Status::not_supported() : Status::Ok();
    }

    Status write_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length,
                              ChunkIoContext /*unused*/) override
    {
        return write_mirror(id, mirror, offset, data, length);
    }

    Status read_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length,
                             ChunkIoContext /*unused*/) const override
    {
        return read_mirror(id, mirror, offset, data, length);
    }

    Status write_mirror(ChunkId id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length) override
    {
        return fail_write ? Status::unavailable() : memory.write_mirror(id, mirror, offset, data, length);
    }

    Status advance_write(ChunkId id, uint64_t expected, uint64_t bytes) override
    {
        return memory.advance_write(id, expected, bytes);
    }

    Status read_mirror(ChunkId id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length) const override
    {
        return memory.read_mirror(id, mirror, offset, data, length);
    }

    Status query_chunk(ChunkId id, ChunkLayout *out) const override
    {
        return memory.query_chunk(id, out);
    }

    Status seal_chunk(ChunkId id, uint64_t epoch, uint64_t bytes) override
    {
        return memory.seal_chunk(id, epoch, bytes);
    }
};
} // namespace crowdb::tree::detail
