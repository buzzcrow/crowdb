// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_collector.h"
#include "backend/chunk/chunk_pin_release.h"
#include "common/test_chunk_stage_backend.h"

namespace crowdb::tree::detail
{
class TestCollectorCatalog final : public TestStageCatalog, public ChunkProtectionCatalog
{
  public:
    std::shared_ptr<ChunkPinReleaseQueue> queue;

    std::shared_ptr<ChunkPinReleaseQueue> pin_release_queue() const noexcept override
    {
        return queue;
    }

    Status load_root_owned(uint64_t /*unused*/, uint64_t /*unused*/, ChunkRootBytes * /*unused*/) const override
    {
        return Status::not_found();
    }

    std::optional<ChunkCandidateStatus> candidate;
    uint64_t                            references         = 0;
    uint64_t                            pins               = 0;
    bool                                recovered          = true;
    bool                                writer_live        = false;
    bool                                lose_release       = false;
    size_t                              pin_calls          = 0;
    size_t                              release_calls      = 0;
    uint32_t                            largest_pin_window = 0;

    ChunkProtectionCatalog *protection() noexcept override
    {
        return this;
    }

    Status check_read_owner() const override
    {
        return Status::Ok();
    }

    Status writer_quiesced(uint64_t /*tree*/, uint64_t /*epoch*/) override
    {
        return Status::Ok();
    }

    Status load_root_bytes(uint64_t /*tree*/, uint64_t /*generation*/, std::vector<uint8_t> * /*out*/) const override
    {
        return Status::not_supported();
    }

    Status begin_candidate(uint64_t /*tree*/, const ChunkManifestCandidate & /*unused*/) override
    {
        return Status::not_supported();
    }

    Status prepare_references(uint64_t /*tree*/, ChunkStageId /*unused*/, uint64_t /*window*/,
                              std::span<const ChunkStageId> /*unused*/) override
    {
        return Status::not_supported();
    }

    Status list_candidates(uint64_t /*tree*/, std::optional<ChunkStageId> /*after*/, uint32_t /*limit*/,
                           std::vector<ChunkCandidateStatus> * /*out*/) const override
    {
        return Status::not_supported();
    }

    Status load_candidate(uint64_t /*tree*/, ChunkStageId id, std::optional<ChunkCandidateStatus> *out) const override
    {
        *out = candidate && candidate->id == id ? candidate : std::nullopt;
        return Status::Ok();
    }

    Status next_candidate(uint64_t /*unused*/, std::optional<ChunkStageId> after,
                          std::optional<ChunkCandidateStatus> *out) const override
    {
        *out = candidate && (!after || candidate->id > *after) ? candidate : std::nullopt;
        return Status::Ok();
    }

    Status next_stage(uint64_t /*unused*/, std::optional<ChunkStageId> after,
                      const std::shared_ptr<AllocationBudget> & /*unused*/,
                      std::optional<ChunkStageRecord> *out) const override
    {
        *out = record && (!after || record->id > *after) ? record : std::nullopt;
        return Status::Ok();
    }

    Status finish_candidate(uint64_t /*tree*/, ChunkStageId /*unused*/, uint32_t action) override
    {
        EXPECT_EQ(action, CT_CHUNK_CANDIDATE_RECOVER);
        if (!recovered || writer_live || !candidate) {
            return Status::unavailable();
        }
        candidate->state = ChunkCandidateState::Aborted;
        return Status::Ok();
    }

    Status release_references(uint64_t /*tree*/, ChunkStageId id, uint64_t window) override
    {
        if (!candidate || candidate->id != id || window != candidate->released_windows ||
            window >= candidate->prepared_windows || references == 0) {
            return Status::corruption();
        }
        ++release_calls;
        ++candidate->released_windows;
        --references;
        return lose_release ? Status::unavailable() : Status::Ok();
    }

    Status prune_pins(uint64_t /*tree*/, ChunkStageId /*unused*/, std::optional<ChunkStageId> after, uint32_t limit,
                      ChunkPinProgress *out) override
    {
        if (!recovered) {
            return Status::unavailable();
        }
        ++pin_calls;
        largest_pin_window   = std::max(largest_pin_window, limit);
        const uint64_t first = after ? after->operation_low : 0;
        const uint64_t count = std::min<uint64_t>(pins - std::min(pins, first), limit);
        *out = {.after    = count == 0 ? after
                                       : std::optional<ChunkStageId>(
                                             {.owner_epoch = 1, .operation_high = 1, .operation_low = first + count}),
                .examined = static_cast<uint32_t>(count),
                .released = 0,
                .more     = first + count < pins};
        return Status::Ok();
    }

    Status change_pin(uint64_t /*tree*/, ChunkStageId /*stage*/, ChunkStageId /*pin*/, bool /*acquire*/) override
    {
        return Status::not_supported();
    }

    Status compare_stage(uint64_t tree, uint64_t epoch, const std::optional<ChunkStageRecord> &expected,
                         const std::optional<ChunkStageRecord> &next) override
    {
        if (!recovered || references != 0 || pins != 0) {
            return Status::unavailable();
        }
        return TestStageCatalog::compare_stage(tree, epoch, expected, next);
    }
};

inline ChunkStageRecord test_collector_stage()
{
    return {
        .tree_id             = 17,
        .id                  = {.owner_epoch = 1, .operation_high = 10, .operation_low = 20},
        .revision            = 2,
        .chunk_high          = uint64_t{2}
                           << 56,
        .chunk_low           = 1,
        .capacity            = 4096,
        .manifest_generation = 0,
        .purpose             = 2,
        .state               = ChunkStageState::Reloadable,
        .packs               = {{.offset = 0, .length = 4096, .checksum = 1}}
    };
}
} // namespace crowdb::tree::detail
