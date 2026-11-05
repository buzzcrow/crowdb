// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/chunk_stage_c_api.h"

#include "chunk_c_api_internal.h"

#include <cstdlib>
#include <cstring>

namespace
{
using namespace crowdb::tree;
using namespace crowdb::tree::detail;

ct_status code(Status status)
{
    return static_cast<ct_status>(status.code());
}

ChunkStageId internal(ct_chunk_stage_id id)
{
    return {.owner_epoch = id.owner_epoch, .operation_high = id.operation_high, .operation_low = id.operation_low};
}

Status decode_optional(const uint8_t *data, size_t length, std::optional<ChunkStageRecord> *out)
{
    if (length == 0) {
        out->reset();
        return Status::Ok();
    }
    if (data == nullptr) {
        return Status::invalid_argument();
    }
    ChunkStageRecord record;
    auto             status = decode_stage_record({data, length}, &record);
    if (status.ok()) {
        *out = std::move(record);
    }
    return status;
}

ct_status output(std::span<const uint8_t> bytes, ct_buf *out)
{
    auto *buffer = static_cast<uint8_t *>(std::malloc(bytes.size()));
    if (buffer == nullptr) {
        return code(Status::resource_exhausted());
    }
    std::memcpy(buffer, bytes.data(), bytes.size());
    *out = {.data = buffer, .len = bytes.size()};
    return 0;
}
} // namespace
ct_status ct_callback_root_catalog_open_v2(const ct_root_catalog_callbacks         *callbacks,
                                           const ct_root_catalog_staging_callbacks *staging, void *context,
                                           ct_root_catalog **out)
try {
    if (callbacks == nullptr || callbacks->load == nullptr || callbacks->free_blob == nullptr ||
        callbacks->store == nullptr || callbacks->publish == nullptr ||
        callbacks->allocate_reference_segment_id == nullptr || staging == nullptr || staging->version != 1 ||
        staging->struct_bytes != sizeof(*staging) || staging->capabilities != CT_CHUNK_STAGE_SCHEMA_V1 ||
        staging->load == nullptr || staging->compare == nullptr || staging->list == nullptr || out == nullptr ||
        *out != nullptr) {
        return code(Status::invalid_argument());
    }
    auto handle       = std::make_unique<ct_root_catalog>();
    handle->transport = std::make_shared<MemoryChunkTransport>();
    handle->catalog   = std::make_shared<CallbackRootCatalog>(*callbacks, *staging, context);
    *out              = handle.release();
    return 0;
}
catch (const std::bad_alloc &) {
    return code(Status::resource_exhausted());
}
catch (...) {
    return code(Status::internal_error());
}

uint64_t ct_root_catalog_stage_capabilities(const ct_root_catalog *catalog)
{
    return catalog != nullptr && catalog->catalog && catalog->catalog->stage_schema_supported()
             ? CT_CHUNK_STAGE_SCHEMA_V1
             : 0;
}
ct_status ct_root_catalog_stage_load(ct_root_catalog *catalog, uint64_t tree_id, ct_chunk_stage_id id, ct_buf *out)
try {
    if (catalog == nullptr || !catalog->catalog || out == nullptr || out->data != nullptr || out->len != 0) {
        return code(Status::invalid_argument());
    }
    std::optional<ChunkStageRecord> record;
    auto                            status = catalog->catalog->load_stage(tree_id, internal(id), &record);
    if (!status.ok()) {
        return code(status);
    }
    if (!record) {
        return code(Status::not_found());
    }
    std::vector<uint8_t> bytes;
    status = encode_stage_record(*record, &bytes);
    return status.ok() ? output(bytes, out) : code(status);
}
catch (const std::bad_alloc &) {
    return code(Status::resource_exhausted());
}
catch (...) {
    return code(Status::internal_error());
}

ct_status ct_root_catalog_stage_compare(ct_root_catalog *catalog, uint64_t tree_id, uint64_t owner_epoch,
                                        const uint8_t *expected, size_t expected_length, const uint8_t *next,
                                        size_t next_length)
try {
    if (catalog == nullptr || !catalog->catalog) {
        return code(Status::invalid_argument());
    }
    std::optional<ChunkStageRecord> old_record;
    std::optional<ChunkStageRecord> new_record;
    auto                            status = decode_optional(expected, expected_length, &old_record);
    if (status.ok()) {
        status = decode_optional(next, next_length, &new_record);
    }
    return code(status.ok() ? catalog->catalog->compare_stage(tree_id, owner_epoch, old_record, new_record) : status);
}
catch (const std::bad_alloc &) {
    return code(Status::resource_exhausted());
}
catch (...) {
    return code(Status::internal_error());
}

ct_status ct_root_catalog_stage_list(ct_root_catalog *catalog, uint64_t tree_id, const ct_chunk_stage_id *after,
                                     uint32_t limit, ct_buf *out)
try {
    if (catalog == nullptr || !catalog->catalog || out == nullptr || out->data != nullptr || out->len != 0) {
        return code(Status::invalid_argument());
    }
    std::vector<ChunkStageRecord> records;
    auto                          status = catalog->catalog->list_stages(
        tree_id, after != nullptr ? std::optional(internal(*after)) : std::nullopt, limit, &records);
    if (!status.ok()) {
        return code(status);
    }
    std::vector<uint8_t> bytes;
    status = encode_stage_list(records, &bytes);
    return status.ok() ? output(bytes, out) : code(status);
}
catch (const std::bad_alloc &) {
    return code(Status::resource_exhausted());
}
catch (...) {
    return code(Status::internal_error());
}
