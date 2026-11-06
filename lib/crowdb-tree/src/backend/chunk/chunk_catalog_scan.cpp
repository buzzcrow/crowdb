// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_protection_callback.h"
#include "crowdb-tree/maptable/frame_page.h"

namespace crowdb::tree::detail
{
namespace pc = protection_callback;

Status CallbackRootCatalog::next_stage(uint64_t tree, std::optional<ChunkStageId> after,
                                       const std::shared_ptr<AllocationBudget> &budget,
                                       std::optional<ChunkStageRecord>         *out) const
try {
    if (!stage_schema_supported()) {
        return Status::not_supported();
    }
    if (tree == 0 || !budget || out == nullptr || (after && !pc::valid(*after))) {
        return Status::invalid_argument();
    }
    const auto cursor = pc::external(after.value_or(ChunkStageId{}));

    struct Blob
    {
        ct_root_catalog_callbacks callbacks;
        void                     *context;
        const uint8_t            *data   = nullptr;
        size_t                    length = 0;

        ~Blob()
        {
            if (data != nullptr) {
                callbacks.free_blob(context, data, length);
            }
        }
    } blob{.callbacks = callbacks_, .context = context_};

    auto status = pc::status(staging_.list(context_, tree, after ? &cursor : nullptr, 1, &blob.data, &blob.length));
    if (!status.ok()) {
        return status;
    }
    if (blob.data == nullptr || blob.length < 4 || blob.length > 8 + kMaxStageBytes) {
        return Status::corruption();
    }
    const auto count = frame_u32(blob.data, 0);
    if (count == 0 && blob.length == 4) {
        out->reset();
        return Status::Ok();
    }
    if (count != 1 || blob.length < 8 || frame_u32(blob.data, 4) != blob.length - 8) {
        return Status::corruption();
    }
    auto record =
        ChunkStageRecord{.tree_id             = 0,
                         .id                  = {},
                         .revision            = 0,
                         .chunk_high          = 0,
                         .chunk_low           = 0,
                         .capacity            = 0,
                         .manifest_generation = 0,
                         .purpose             = 0,
                         .state               = ChunkStageState::Reserved,
                         .packs = decltype(ChunkStageRecord::packs)(MetadataAllocator<ChunkStagePack>(budget))};
    status = decode_stage_record({blob.data + 8, blob.length - 8}, &record);
    if (!status.ok()) {
        return status;
    }
    if (record.tree_id != tree || (after && record.id <= *after)) {
        return Status::corruption();
    }
    *out = std::move(record);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status CallbackRootCatalog::next_candidate(uint64_t tree, std::optional<ChunkStageId> after,
                                           std::optional<ChunkCandidateStatus> *out) const
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || out == nullptr || (after && !pc::valid(*after))) {
        return Status::invalid_argument();
    }
    ct_chunk_candidate_status wire{};
    size_t                    count  = 1;
    const auto                cursor = pc::external(after.value_or(ChunkStageId{}));
    auto status = pc::status(protection_.list(context_, tree, after ? &cursor : nullptr, 1, &wire, &count));
    if (!status.ok()) {
        return status;
    }
    if (count == 0) {
        out->reset();
        return Status::Ok();
    }
    if (count != 1) {
        return Status::corruption();
    }
    ChunkCandidateStatus value;
    status = pc::decode(wire, &value);
    if (!status.ok() || (after && value.id <= *after)) {
        return Status::corruption();
    }
    *out = value;
    return Status::Ok();
}
} // namespace crowdb::tree::detail
