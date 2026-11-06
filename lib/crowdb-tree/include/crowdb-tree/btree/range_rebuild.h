// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/btree/key_range.h"
#include "crowdb-tree/btree/tree.h"

#include <cstdint>
#include <memory>

namespace crowdb::tree
{

struct RangeRebuildStats
{
    // Logical keys enumerated after route/range selection. Skipped subtrees
    // and binary-search-elided leaf records are not examined or filtered.
    uint64_t entries_examined = 0;
    uint64_t entries_emitted  = 0;
    uint64_t entries_filtered = 0;
    // Shared physical storage only; independently paid frame copies count as
    // rebuilt even when their encoded bytes can be copied unchanged.
    uint64_t pages_reused  = 0;
    uint64_t pages_rebuilt = 0;
    // Maximal child roots skipped by routing, including one root for an empty
    // selection. Descendants are not visited just to count them.
    uint64_t subtrees_skipped = 0;
};

// Build an independently mutable tree whose physical structure contains only
// keys in `range`. The source snapshot is pinned for the iterator lifetime and
// the destination publishes only after its own complete snapshot succeeds.
Status rebuild_range(Crowdbtree &source, const KeyRange &range, Config destination_options,
                     std::unique_ptr<Crowdbtree> *out, RangeRebuildStats *stats = nullptr);

} // namespace crowdb::tree
