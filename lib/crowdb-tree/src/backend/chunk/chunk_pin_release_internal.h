// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_pin_release.h"

namespace crowdb::tree::detail
{
struct ChunkPinReleaseNode
{
    std::shared_ptr<ChunkPinReleaseQueue> queue;
    std::shared_ptr<AllocationBudget>     budget;
    std::shared_ptr<RootCatalog>          catalog;
    uint64_t                              tree = 0;
    ChunkStageId                          stage{};
    ChunkStageId                          pin{};
    bool                                  armed  = false;
    bool                                  writer = false;
    ChunkPinReleaseNode                  *next   = nullptr;
};
} // namespace crowdb::tree::detail
