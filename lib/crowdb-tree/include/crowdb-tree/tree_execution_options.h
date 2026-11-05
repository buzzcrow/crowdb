// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstddef>

namespace crowdb::tree
{
// Work and allocation windows, independent of durable frame geometry.
struct TreeExecutionOptions
{
    size_t source_max_entries       = 4096;
    size_t source_max_key_bytes     = 1024 * 1024;
    size_t source_max_gap_entries   = 1024;
    size_t source_max_gap_key_bytes = 1024 * 1024;
    // Zero derives min(CPU workers, per-tree I/O permits).
    size_t flush_parallelism           = 0;
    size_t run_max_entries             = 128;
    size_t run_max_key_bytes           = 256 * 1024;
    size_t run_key_block_bytes         = 1024;
    size_t run_max_bytes               = 1024 * 1024;
    size_t install_run_window          = 64;
    size_t maintenance_visits_per_step = 64;
    size_t maintenance_max_visits      = 4096;
    size_t maintenance_max_candidates  = 256;
    size_t maintenance_max_steps       = 1024;
    size_t maintenance_metadata_bytes  = 1024 * 1024;
    size_t cache_max_pages             = 256;
};
} // namespace crowdb::tree
