// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "c_api_owned_budget.h"
#include "crowdb-tree/snapshot/snapshot_io.h"

#include <cstdlib>
#include <cstring>

using namespace crowdb::tree;

struct ct_export
{
    std::shared_ptr<AllocationBudget> metadata;
    std::unique_ptr<SnapshotExport>   exp;
    SnapshotChunk                     pending{};
    uint64_t                          pending_offset = 0;
    bool                              pending_valid  = false;
    bool                              pending_done   = false;

    Status prepare(uint64_t offset)
    {
        if (offset != (pending_valid ? pending_offset : exp->offset())) {
            return Status::invalid_argument("snapshot offset is not sequential");
        }
        if (pending_valid) {
            return Status::Ok();
        }
        auto status = exp->next_owned(&pending, &pending_done);
        if (status.ok()) {
            pending_offset = offset;
            pending_valid  = true;
        }
        return status;
    }

    void consumed()
    {
        pending       = {};
        pending_valid = false;
    }
};

struct ct_snapshot_chunk
{
    SnapshotChunk value{};
};

static_assert(sizeof(ct_snapshot_chunk) <= kSnapshotChunkHandleBytes);

ct_status ct_snapshot_export_begin(ct_tree *tree, size_t chunk_bytes, ct_export **out)
try {
    if (tree == nullptr || out == nullptr || chunk_bytes == 0) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *out                                     = nullptr;
    auto                             *native = c_api_tree(tree);
    auto                             *core   = TreeCoreAccess::get(*native);
    std::shared_ptr<AllocationBudget> budget = core != nullptr ? core->binding()->lifecycle_budget() : nullptr;
    std::unique_ptr<SnapshotExport>   stream;
    auto status = snapshot_export_begin(*native, snapshot_format::kPortable, chunk_bytes, &stream);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    MetadataAllocator<ct_export> allocator(budget);
    auto                        *result = allocator.allocate(1);
    std::construct_at(result);
    result->metadata = std::move(budget);
    result->exp      = std::move(stream);
    *out             = result;
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

uint64_t ct_snapshot_export_at_slot(const ct_export *e)
{
    return e == nullptr ? 0 : e->exp->at_slot();
}

uint64_t ct_snapshot_export_total_bytes(const ct_export *e)
{
    return e == nullptr ? 0 : static_cast<uint64_t>(e->exp->total_bytes());
}

uint32_t ct_snapshot_export_final_crc32c(const ct_export *e)
{
    return e == nullptr ? 0 : e->exp->final_crc32c();
}

size_t ct_snapshot_export_chunk_bytes(const ct_export *e)
{
    return e == nullptr ? 0 : e->exp->chunk_bytes();
}

uint64_t ct_snapshot_export_offset(const ct_export *e)
{
    if (e == nullptr) {
        return 0;
    }
    return e->pending_valid ? e->pending_offset : e->exp->offset();
}

ct_status ct_snapshot_export_next(ct_export *e, uint64_t offset, ct_buf *chunk, int32_t *done)
try {
    if (e == nullptr || chunk == nullptr || done == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    const auto status = e->prepare(offset);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    const auto bytes = e->pending.bytes;
    uint8_t   *data  = nullptr;
    if (!bytes.empty()) {
        data = static_cast<uint8_t *>(std::malloc(bytes.size()));
        if (data == nullptr) {
            throw std::bad_alloc();
        }
        std::memcpy(data, bytes.data(), bytes.size());
    }
    *chunk = {.data = data, .len = bytes.size()};
    *done  = e->pending_done ? 1 : 0;
    e->consumed();
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_status ct_snapshot_export_next_v2(ct_export *e, uint64_t offset, ct_snapshot_chunk **out, int32_t *done)
try {
    if (e == nullptr || out == nullptr || done == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *out              = nullptr;
    const auto status = e->prepare(offset);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    MetadataAllocator<ct_snapshot_chunk> allocator(e->pending.allocation_budget);
    auto                                *result = allocator.allocate(1);
    std::construct_at(result, ct_snapshot_chunk{std::move(e->pending)});
    *out  = result;
    *done = e->pending_done ? 1 : 0;
    e->consumed();
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_status ct_snapshot_chunk_data(const ct_snapshot_chunk *chunk, const uint8_t **data, size_t *len)
{
    if (chunk == nullptr || data == nullptr || len == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *data = chunk->value.bytes.data();
    *len  = chunk->value.bytes.size();
    return 0;
}

int32_t ct_snapshot_chunk_owned_budget(const ct_snapshot_chunk *chunk, ct_owned_budget **out)
{
    if (chunk == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    return c_api_owned_budget(chunk->value.allocation_budget, out);
}

void ct_snapshot_chunk_free(ct_snapshot_chunk *chunk)
{
    if (chunk != nullptr) {
        MetadataAllocator<ct_snapshot_chunk> allocator(chunk->value.allocation_budget);
        std::destroy_at(chunk);
        allocator.deallocate(chunk, 1);
    }
}

void ct_snapshot_export_end(ct_export *e)
{
    if (e != nullptr) {
        MetadataAllocator<ct_export> allocator(e->metadata);
        std::destroy_at(e);
        allocator.deallocate(e, 1);
    }
}
