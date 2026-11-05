// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_page_store.h"

namespace crowdb::tree::detail
{
namespace
{
Status status_from_callback(ct_status code)
{
    switch (code) {
    case 0:
        return Status::Ok();
    case -1:
        return Status::not_found();
    case -2:
        return Status::invalid_argument();
    case -3:
        return Status::corruption();
    case -4:
        return Status::io_error();
    case -5:
        return Status::not_supported();
    case -6:
        return Status::internal_error();
    case -7:
        return Status::resource_exhausted();
    default:
        return Status::unavailable();
    }
}

struct CallbackBlob
{
    ct_root_catalog_callbacks callbacks;
    void                     *context;
    const uint8_t            *data   = nullptr;
    size_t                    length = 0;

    ~CallbackBlob()
    {
        if (data != nullptr) {
            callbacks.free_blob(context, data, length);
        }
    }
};

ct_chunk_stage_id external(ChunkStageId id)
{
    return {.owner_epoch = id.owner_epoch, .operation_high = id.operation_high, .operation_low = id.operation_low};
}
} // namespace

CallbackRootCatalog::CallbackRootCatalog(ct_root_catalog_callbacks callbacks, ct_root_catalog_staging_callbacks staging,
                                         void *context)
    : staging_(staging),
      callbacks_(callbacks),
      context_(context)
{
}

bool CallbackRootCatalog::stage_schema_supported() const noexcept
{
    return staging_.version == 1 && staging_.struct_bytes == sizeof(staging_) &&
           (staging_.capabilities & CT_CHUNK_STAGE_SCHEMA_V1) != 0 && staging_.load != nullptr &&
           staging_.compare != nullptr && staging_.list != nullptr;
}

Status CallbackRootCatalog::load_stage(uint64_t tree_id, ChunkStageId id, std::optional<ChunkStageRecord> *out) const
{
    if (!stage_schema_supported()) {
        return Status::not_supported();
    }
    if (out == nullptr) {
        return Status::invalid_argument();
    }
    CallbackBlob blob{.callbacks = callbacks_, .context = context_};
    auto status = status_from_callback(staging_.load(context_, tree_id, external(id), &blob.data, &blob.length));
    if (status.code() == Code::kNotFound) {
        out->reset();
        return Status::Ok();
    }
    if (!status.ok()) {
        return status;
    }
    if (blob.data == nullptr || blob.length > kMaxStageBytes) {
        return Status::corruption();
    }
    ChunkStageRecord record = out->value_or(ChunkStageRecord{});
    record.packs.clear();
    status = decode_stage_record({blob.data, blob.length}, &record);
    if (!status.ok()) {
        return status;
    }
    if (record.tree_id != tree_id || record.id != id) {
        return Status::corruption();
    }
    *out = std::move(record);
    return Status::Ok();
}

Status CallbackRootCatalog::compare_stage(uint64_t tree_id, uint64_t owner_epoch,
                                          const std::optional<ChunkStageRecord> &expected,
                                          const std::optional<ChunkStageRecord> &next)
{
    if (!stage_schema_supported()) {
        return Status::not_supported();
    }
    auto status = validate_stage_transition(expected, next);
    if (!status.ok()) {
        return status;
    }
    if ((expected && expected->tree_id != tree_id) || (next && next->tree_id != tree_id) || owner_epoch == 0) {
        return Status::invalid_argument();
    }
    std::shared_ptr<AllocationBudget> budget;
    if (next) {
        budget = next->packs.get_allocator().owner();
    }
    else if (expected) {
        budget = expected->packs.get_allocator().owner();
    }
    ChunkStageBytes old_bytes{MetadataAllocator<uint8_t>(budget)};
    ChunkStageBytes new_bytes{MetadataAllocator<uint8_t>(budget)};
    if (expected) {
        status = encode_stage_record_owned(*expected, &old_bytes);
    }
    if (status.ok() && next) {
        status = encode_stage_record_owned(*next, &new_bytes);
    }
    if (!status.ok()) {
        return status;
    }
    return status_from_callback(staging_.compare(context_, tree_id, owner_epoch, old_bytes.data(), old_bytes.size(),
                                                 new_bytes.data(), new_bytes.size()));
}

Status CallbackRootCatalog::list_stages(uint64_t tree_id, std::optional<ChunkStageId> after, uint32_t limit,
                                        std::vector<ChunkStageRecord> *out) const
{
    if (!stage_schema_supported()) {
        return Status::not_supported();
    }
    if (out == nullptr || limit == 0 || limit > kMaxStageScan) {
        return Status::invalid_argument();
    }
    const auto   cursor = external(after.value_or(ChunkStageId{}));
    CallbackBlob blob{.callbacks = callbacks_, .context = context_};
    auto         status = status_from_callback(
        staging_.list(context_, tree_id, after ? &cursor : nullptr, limit, &blob.data, &blob.length));
    if (!status.ok()) {
        return status;
    }
    if (blob.data == nullptr || blob.length > 4 + (kMaxStageScan * (4 + kMaxStageBytes))) {
        return Status::corruption();
    }
    std::vector<ChunkStageRecord> records;
    status = decode_stage_list({blob.data, blob.length}, &records);
    if (!status.ok()) {
        return status;
    }
    if (records.size() > limit) {
        return Status::corruption();
    }
    for (const auto &record : records) {
        if (record.tree_id != tree_id || (after && record.id <= *after)) {
            return Status::corruption();
        }
        after = record.id;
    }
    *out = std::move(records);
    return Status::Ok();
}
} // namespace crowdb::tree::detail
