// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_stage_gc.h"

namespace crowdb::tree::detail
{
struct ChunkCollectionProgress
{
    uint64_t candidates_examined    = 0;
    uint64_t reference_windows      = 0;
    uint64_t stages_examined        = 0;
    uint64_t pins_examined          = 0;
    uint64_t pins_released          = 0;
    uint64_t stages_removed         = 0;
    uint64_t logical_capacity_bytes = 0;
    bool     more                   = false;
};

// One accepted maintenance operation owns this fixed cursor exclusively. No
// mapping traversal or inferred age makes a stage eligible: the catalog CAS
// validates recovery, process exclusion, references and pins at retirement.
class ChunkCollector
{
  public:
    [[nodiscard]] static size_t allocation_bound() noexcept;
    Status                      step(RootCatalog &catalog, ChunkTransport &transport, uint64_t tree, uint64_t epoch,
                                     const std::shared_ptr<AllocationBudget> &budget, ChunkCollectionProgress *out,
                                     const std::shared_ptr<ChunkPhysicalRegistry> &registry = {});

  private:
    Status candidate_step(ChunkProtectionCatalog &catalog, uint64_t tree, ChunkCollectionProgress &out);
    Status stage_step(RootCatalog &catalog, ChunkTransport &transport, uint64_t tree, uint64_t epoch,
                      const std::shared_ptr<AllocationBudget> &budget, ChunkCollectionProgress &out,
                      const std::shared_ptr<ChunkPhysicalRegistry> &registry);
    std::optional<ChunkStageId> candidate_after_;
    std::optional<ChunkStageId> candidate_active_;
    std::optional<ChunkStageId> stage_after_;
    std::optional<ChunkStageId> stage_active_;
    std::optional<ChunkStageId> pin_after_;
    bool                        next_candidate_ = true;
    bool                        candidates_end_ = false;
    bool                        stages_end_     = false;
};
} // namespace crowdb::tree::detail
