// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_page_store.h"

namespace crowdb::tree::detail::protection_callback
{
inline bool valid(ChunkStageId id) noexcept
{
    return id.owner_epoch != 0 && (id.operation_high != 0 || id.operation_low != 0);
}

inline ct_chunk_stage_id external(ChunkStageId id) noexcept
{
    return {.owner_epoch = id.owner_epoch, .operation_high = id.operation_high, .operation_low = id.operation_low};
}

inline ChunkStageId internal(ct_chunk_stage_id id) noexcept
{
    return {.owner_epoch = id.owner_epoch, .operation_high = id.operation_high, .operation_low = id.operation_low};
}

inline Status status(ct_status code)
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

inline bool supported(const ct_root_catalog_protection_callbacks &c) noexcept
{
    return c.version == 1 && c.struct_bytes == sizeof(c) && c.capabilities == CT_CHUNK_PROTECTION_SCHEMA_V1 &&
           (c.check_reader != nullptr) && (c.writer_quiesced != nullptr) && (c.begin != nullptr) &&
           (c.load != nullptr) && (c.list != nullptr) && (c.prepare != nullptr) && (c.finish != nullptr) &&
           (c.release != nullptr) && (c.pin != nullptr) && (c.prune != nullptr);
}

inline Status decode(const ct_chunk_candidate_status &wire, ChunkCandidateStatus *out)
{
    auto id = internal(wire.id);
    if (!valid(id) || wire.state < 1 || wire.state > 5 || wire.generation == 0 ||
        wire.reference_count > kMaxCandidateReferences || wire.prepared_references > wire.reference_count ||
        wire.prepared_windows > wire.prepared_references || wire.released_windows > wire.prepared_windows) {
        return Status::corruption();
    }
    *out = {.id                  = id,
            .generation          = wire.generation,
            .reference_count     = wire.reference_count,
            .prepared_references = wire.prepared_references,
            .prepared_windows    = wire.prepared_windows,
            .released_windows    = wire.released_windows,
            .state               = static_cast<ChunkCandidateState>(wire.state)};
    return Status::Ok();
}
} // namespace crowdb::tree::detail::protection_callback
