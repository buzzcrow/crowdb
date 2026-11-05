// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>
#include <memory>

namespace crowdb::tree
{
struct CapturedDurablePages;

// Durable receipts have an owner independent of live representation rebase.
// This object deliberately retains neither source mappings nor overlay runs.
struct SnapshotBaseline
{
    uint64_t                                    generation        = 0;
    uint64_t                                    capture_token     = 0;
    uint64_t                                    coverage          = 0;
    uint64_t                                    snapshot_sequence = 0;
    uint64_t                                    backend_id        = 0;
    uint64_t                                    gc_floor          = 0;
    uint64_t                                    wal_offset        = 0;
    std::shared_ptr<const CapturedDurablePages> pages;
};
} // namespace crowdb::tree
