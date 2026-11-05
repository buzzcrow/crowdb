// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_stage.h"

#include <algorithm>

namespace crowdb::tree::detail
{
Status validate_stage_record(const ChunkStageRecord &record)
{
    using State = ChunkStageState;
    if (record.tree_id == 0 || record.id.owner_epoch == 0 ||
        (record.id.operation_high == 0 && record.id.operation_low == 0) || record.revision == 0 ||
        record.capacity == 0 || record.capacity > UINT32_MAX || (record.purpose != 2 && record.purpose != 3) ||
        record.chunk_high >> 56 != record.purpose || record.packs.size() > kMaxStagePacks ||
        record.state < State::Reserved || record.state > State::Reclaimed ||
        ((record.state == State::Reloadable || record.state == State::Referenced) && record.packs.empty()) ||
        (record.state == State::Referenced && record.manifest_generation == 0) ||
        (record.state <= State::Reloadable && record.manifest_generation != 0)) {
        return Status::invalid_argument("invalid staged chunk identity, state, or geometry");
    }
    uint64_t end = 0;
    for (const auto &pack : record.packs) {
        if (pack.length == 0 || pack.length > 256U * 1024U * 1024U || pack.offset < end ||
            pack.offset > record.capacity || pack.length > record.capacity - pack.offset) {
            return Status::invalid_argument("invalid staged pack range");
        }
        end = pack.offset + pack.length;
    }
    return Status::Ok();
}

Status validate_stage_transition(const std::optional<ChunkStageRecord> &expected,
                                 const std::optional<ChunkStageRecord> &next)
{
    using State = ChunkStageState;
    if ((expected && !validate_stage_record(*expected).ok()) || (next && !validate_stage_record(*next).ok())) {
        return Status::invalid_argument("invalid staged transition record");
    }
    if (!expected || !next) {
        return ((!expected && next && next->revision == 1 && next->state == State::Reserved) ||
                (expected && !next && expected->state == State::Reclaimed))
                 ? Status::Ok()
                 : Status::invalid_argument("invalid staged creation or cleanup");
    }
    const auto &old   = *expected;
    const auto &value = *next;
    if (old.tree_id != value.tree_id || old.id != value.id || old.chunk_high != value.chunk_high ||
        old.chunk_low != value.chunk_low || old.capacity != value.capacity || old.purpose != value.purpose ||
        old.revision == UINT64_MAX || value.revision != old.revision + 1 || value.packs.size() < old.packs.size() ||
        !std::equal(old.packs.begin(), old.packs.end(), value.packs.begin()) ||
        (value.packs != old.packs && old.state != State::Reserved && old.state != State::Writing) ||
        value.manifest_generation < old.manifest_generation) {
        return Status::invalid_argument("staged transition changes immutable identity or refs");
    }
    bool valid = false;
    switch (old.state) {
    case State::Reserved:
        valid = value.state == State::Writing || value.state == State::Retired;
        break;
    case State::Writing:
        valid = value.state == State::Writing || value.state == State::Reloadable || value.state == State::Retired;
        break;
    case State::Reloadable:
    case State::Referenced:
        valid = value.state == State::Referenced || value.state == State::Retired;
        break;
    case State::Retired:
        valid = value.state == State::Deleting;
        break;
    case State::Deleting:
        valid = value.state == State::Reclaimed;
        break;
    case State::Reclaimed:
        break;
    }
    return valid ? Status::Ok() : Status::invalid_argument("invalid staged state transition");
}
} // namespace crowdb::tree::detail
