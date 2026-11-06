// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_page_store.h"

namespace crowdb::tree
{
class ChunkPhysicalRegistry;
}

namespace crowdb::tree::detail
{
struct ChunkStageGcResult
{
    Status status;
    bool   deletion_acknowledged = false;
    bool   stage_removed         = false;
};

// Retirement is a separate ownership decision. This finite backend step only
// consumes a durably retired identity; time, absence, or a write error alone
// never authorize reclamation.
ChunkStageGcResult reclaim_retired_stage(RootCatalog &catalog, ChunkTransport &transport, uint64_t tree, uint64_t epoch,
                                         ChunkStageId id, const std::shared_ptr<AllocationBudget> &budget = {},
                                         const std::shared_ptr<ChunkPhysicalRegistry> &registry = {});
} // namespace crowdb::tree::detail
