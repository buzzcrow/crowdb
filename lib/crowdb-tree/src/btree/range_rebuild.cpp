// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "runtime_range_rebuild.h"

namespace crowdb::tree
{
Status rebuild_range(Crowdbtree &source, const KeyRange &range, Config options, std::unique_ptr<Crowdbtree> *out,
                     RangeRebuildStats *stats)
{
    return rebuild_range_runtime(source, range, std::move(options), out, stats);
}
} // namespace crowdb::tree
