// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <cstddef>
#include <cstdint>

extern "C" {
// Complete tuning values. Initialize with ct_tree_tuning_default before editing.
struct ct_tree_tuning
{
    size_t   struct_size;
    size_t   memtable_flush_bytes;
    uint32_t memtable_flush_entries;
    uint32_t max_memtable_count;
    size_t   leaf_split_bytes;
    size_t   leaf_merge_bytes;
    uint32_t inner_max_keys;
    uint32_t inner_merge_keys;
    size_t   max_key_size;
    size_t   source_max_entries;
    size_t   source_max_key_bytes;
    size_t   source_max_gap_entries;
    size_t   source_max_gap_key_bytes;
    size_t   flush_parallelism;
    size_t   run_max_entries;
    size_t   run_max_key_bytes;
    size_t   run_key_block_bytes;
    size_t   run_max_bytes;
    size_t   install_run_window;
    size_t   maintenance_visits_per_step;
    size_t   maintenance_max_visits;
    size_t   maintenance_max_candidates;
    size_t   maintenance_max_steps;
    size_t   maintenance_metadata_bytes;
    size_t   cache_max_pages;
};

void ct_tree_tuning_default(ct_tree_tuning *out);
}
