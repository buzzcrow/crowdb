// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_protection_callback.h"
#include "crowdb-common/crc32c.h"

#include <array>

namespace crowdb::tree::detail
{
namespace pc = protection_callback;

Status CallbackRootCatalog::prepare_references(uint64_t tree, ChunkStageId id, uint64_t window,
                                               std::span<const ChunkStageId> references)
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || !pc::valid(id) || references.empty() || references.size() > kMaxReferenceWindow) {
        return Status::invalid_argument();
    }
    std::array<ct_chunk_stage_id, kMaxReferenceWindow> wire{};
    for (size_t i = 0; i < references.size(); ++i) {
        if (!pc::valid(references[i]) || ((i != 0U) && references[i - 1] >= references[i])) {
            return Status::invalid_argument();
        }
        wire[i] = pc::external(references[i]);
    }
    return pc::status(protection_.prepare(context_, tree, pc::external(id), window, wire.data(), references.size()));
}

Status CallbackRootCatalog::finish_candidate(uint64_t tree, ChunkStageId id, uint32_t action)
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || !pc::valid(id) || action < CT_CHUNK_CANDIDATE_COMMIT || action > CT_CHUNK_CANDIDATE_RECOVER) {
        return Status::invalid_argument();
    }
    return pc::status(protection_.finish(context_, tree, pc::external(id), action));
}

Status CallbackRootCatalog::release_references(uint64_t tree, ChunkStageId id, uint64_t window)
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || !pc::valid(id)) {
        return Status::invalid_argument();
    }
    return pc::status(protection_.release(context_, tree, pc::external(id), window));
}

Status CallbackRootCatalog::change_pin(uint64_t tree, ChunkStageId stage, ChunkStageId pin, bool acquire)
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || !pc::valid(stage) || !pc::valid(pin)) {
        return Status::invalid_argument();
    }
    return pc::status(protection_.pin(context_, tree, pc::external(stage), pc::external(pin), acquire ? 1 : 0));
}

Status CallbackRootCatalog::prune_pins(uint64_t tree, ChunkStageId stage, std::optional<ChunkStageId> after,
                                       uint32_t limit, ChunkPinProgress *out)
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || !pc::valid(stage) || limit == 0 || limit > kMaxStageScan || out == nullptr ||
        (after && !pc::valid(*after))) {
        return Status::invalid_argument();
    }
    const auto            cursor = pc::external(after.value_or(ChunkStageId{}));
    ct_chunk_pin_progress result{};
    auto                  status =
        pc::status(protection_.prune(context_, tree, pc::external(stage), after ? &cursor : nullptr, limit, &result));
    if (!status.ok()) {
        return status;
    }
    const auto next = pc::internal(result.after);
    if (result.examined > limit || result.released > result.examined || result.has_after > 1 || result.more > 1 ||
        ((result.examined != 0) != static_cast<bool>(result.has_after)) ||
        ((result.more != 0U) && (result.has_after == 0U)) ||
        ((result.has_after != 0U) && (!pc::valid(next) || (after && next <= *after)))) {
        return Status::corruption();
    }
    *out = {.after    = (result.has_after != 0U) ? std::optional(next) : std::nullopt,
            .examined = result.examined,
            .released = result.released,
            .more     = result.more != 0};
    return Status::Ok();
}

uint32_t chunk_reference_checksum(uint32_t initial, std::span<const ChunkStageId> references) noexcept
{
    for (const auto &id : references) {
        for (const uint64_t word : {id.owner_epoch, id.operation_high, id.operation_low}) {
            std::array<uint8_t, 8> bytes{};
            for (size_t i = 0; i < bytes.size(); ++i) {
                bytes[i] = static_cast<uint8_t>(word >> (8 * i));
            }
            initial = crowdb::common::crc32c_update(initial, bytes.data(), bytes.size());
        }
    }
    return initial;
}
} // namespace crowdb::tree::detail
