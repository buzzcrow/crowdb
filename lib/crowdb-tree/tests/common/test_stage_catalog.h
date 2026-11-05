// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_fixed_pack.h"

namespace crowdb::tree::detail
{
class TestStageCatalog : public RootCatalog
{
  public:
    std::optional<ChunkStageRecord> record;
    mutable size_t                  manifest_loads = 0;

    bool stage_schema_supported() const noexcept override
    {
        return true;
    }

    Status load_stage(uint64_t /*unused*/, ChunkStageId /*unused*/, std::optional<ChunkStageRecord> *out) const override
    {
        *out = record;
        return Status::Ok();
    }

    Status compare_stage(uint64_t /*unused*/, uint64_t /*unused*/, const std::optional<ChunkStageRecord> &expected,
                         const std::optional<ChunkStageRecord> &next) override
    {
        auto status = validate_stage_transition(expected, next);
        if (!status.ok()) {
            return status;
        }
        if (record == next) {
            return Status::Ok();
        }
        if (record != expected) {
            return Status::unavailable();
        }
        record = next;
        return Status::Ok();
    }

    std::shared_ptr<const ChunkManifest> load(uint64_t /*unused*/) const override
    {
        ++manifest_loads;
        return {};
    }

    std::shared_ptr<const ChunkManifest> load_generation(uint64_t /*unused*/, uint64_t /*unused*/) const override
    {
        return {};
    }

    Status persist_reference_segment(uint64_t /*unused*/,
                                     std::shared_ptr<const ChunkReferenceSegmentImage> /*segment*/) override
    {
        return Status::not_supported();
    }

    std::shared_ptr<const ChunkReferenceSegmentImage> load_reference_segment(uint64_t /*unused*/,
                                                                             uint64_t /*unused*/) const override
    {
        return {};
    }

    uint64_t allocate_reference_segment_id(uint64_t /*unused*/) override
    {
        return 0;
    }

    uint64_t discard_reference_segments(uint64_t /*unused*/, const std::vector<uint64_t> & /*objects*/) override
    {
        return 0;
    }

    Status publish(uint64_t /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/,
                   std::shared_ptr<const ChunkManifest> /*manifest*/) override
    {
        return Status::not_supported();
    }

    Status pin_generation(uint64_t /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/) override
    {
        return Status::not_supported();
    }

    Status unpin_generation(uint64_t /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/) override
    {
        return Status::not_supported();
    }

    uint64_t reclaim_before(uint64_t /*unused*/, uint64_t /*unused*/) override
    {
        return 0;
    }

    uint64_t retained_manifest_count(uint64_t /*unused*/) const override
    {
        return 0;
    }

    uint64_t pinned_bytes(uint64_t /*unused*/) const override
    {
        return 0;
    }

    uint64_t oldest_pin_age_ms(uint64_t /*unused*/) const override
    {
        return 0;
    }
};

} // namespace crowdb::tree::detail
