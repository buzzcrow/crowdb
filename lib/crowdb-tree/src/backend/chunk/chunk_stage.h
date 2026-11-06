// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/status.h"

#include <compare>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace crowdb::tree::detail
{
inline constexpr size_t   kMaxStagePacks = 1024;
inline constexpr uint32_t kMaxStageScan  = 64;
inline constexpr size_t   kMaxStageBytes = 100 + (16 * kMaxStagePacks);

struct ChunkStageId
{
    uint64_t owner_epoch                             = 0;
    uint64_t operation_high                          = 0;
    uint64_t operation_low                           = 0;
    auto     operator<=>(const ChunkStageId &) const = default;
};
enum class ChunkStageState : uint8_t { Reserved = 1, Writing, Reloadable, Referenced, Retired, Deleting, Reclaimed };

struct ChunkStagePack
{
    uint64_t offset                                   = 0;
    uint32_t length                                   = 0;
    uint32_t checksum                                 = 0;
    bool     operator==(const ChunkStagePack &) const = default;
};

struct ChunkStageRecord
{
    uint64_t                                                       tree_id = 0;
    ChunkStageId                                                   id;
    uint64_t                                                       revision            = 0;
    uint64_t                                                       chunk_high          = 0;
    uint64_t                                                       chunk_low           = 0;
    uint64_t                                                       capacity            = 0;
    uint64_t                                                       manifest_generation = 0;
    uint32_t                                                       purpose             = 0;
    ChunkStageState                                                state               = ChunkStageState::Reserved;
    std::vector<ChunkStagePack, MetadataAllocator<ChunkStagePack>> packs;
    bool                                                           operator==(const ChunkStageRecord &) const = default;
};

Status validate_stage_record(const ChunkStageRecord &record);
Status validate_stage_transition(const std::optional<ChunkStageRecord> &expected,
                                 const std::optional<ChunkStageRecord> &next);
using ChunkStageBytes = std::vector<uint8_t, MetadataAllocator<uint8_t>>;
Status encode_stage_record(const ChunkStageRecord &, std::vector<uint8_t> *);
Status encode_stage_record_owned(const ChunkStageRecord &, ChunkStageBytes *);
Status decode_stage_record(std::span<const uint8_t>, ChunkStageRecord *);
Status encode_stage_list(std::span<const ChunkStageRecord>, std::vector<uint8_t> *);
Status decode_stage_list(std::span<const uint8_t>, std::vector<ChunkStageRecord> *);
} // namespace crowdb::tree::detail
