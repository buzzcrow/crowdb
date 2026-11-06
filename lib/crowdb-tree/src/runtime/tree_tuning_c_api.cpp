// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_tuning_c_api.h"

using namespace crowdb::tree;

void ct_tree_tuning_default(ct_tree_tuning *out)
{
    if (out == nullptr) {
        return;
    }
    const Config defaults;
    *out = {.struct_size                 = sizeof(*out),
            .memtable_flush_bytes        = defaults.memtable_flush_bytes,
            .memtable_flush_entries      = defaults.memtable_flush_entries,
            .max_memtable_count          = defaults.max_memtable_count,
            .leaf_split_bytes            = defaults.leaf_split_bytes,
            .leaf_merge_bytes            = defaults.leaf_merge_bytes,
            .inner_max_keys              = defaults.inner_max_keys,
            .inner_merge_keys            = defaults.inner_merge_keys,
            .max_key_size                = defaults.max_key_size,
            .source_max_entries          = defaults.execution.source_max_entries,
            .source_max_key_bytes        = defaults.execution.source_max_key_bytes,
            .source_max_gap_entries      = defaults.execution.source_max_gap_entries,
            .source_max_gap_key_bytes    = defaults.execution.source_max_gap_key_bytes,
            .flush_parallelism           = defaults.execution.flush_parallelism,
            .run_max_entries             = defaults.execution.run_max_entries,
            .run_max_key_bytes           = defaults.execution.run_max_key_bytes,
            .run_key_block_bytes         = defaults.execution.run_key_block_bytes,
            .run_max_bytes               = defaults.execution.run_max_bytes,
            .install_run_window          = defaults.execution.install_run_window,
            .maintenance_visits_per_step = defaults.execution.maintenance_visits_per_step,
            .maintenance_max_visits      = defaults.execution.maintenance_max_visits,
            .maintenance_max_candidates  = defaults.execution.maintenance_max_candidates,
            .maintenance_max_steps       = defaults.execution.maintenance_max_steps,
            .maintenance_metadata_bytes  = defaults.execution.maintenance_metadata_bytes,
            .cache_max_pages             = defaults.execution.cache_max_pages};
}

Status crowdb::tree::validate_tree_execution(const TreeExecutionOptions &options) noexcept
{
    if (options.source_max_entries == 0 || options.source_max_key_bytes == 0 || options.source_max_gap_entries == 0 ||
        options.source_max_gap_key_bytes == 0 || options.run_max_entries == 0 || options.run_max_key_bytes == 0 ||
        options.run_key_block_bytes == 0 || options.run_max_bytes == 0 || options.install_run_window == 0 ||
        options.maintenance_visits_per_step == 0 || options.maintenance_max_visits == 0 ||
        options.maintenance_max_candidates == 0 || options.maintenance_max_steps == 0 ||
        options.maintenance_metadata_bytes == 0 || options.cache_max_pages == 0) {
        return Status::invalid_argument("Tree execution windows must be positive");
    }
    if (options.run_key_block_bytes > options.run_max_key_bytes ||
        options.maintenance_visits_per_step > options.maintenance_max_visits) {
        return Status::invalid_argument("Tree execution step exceeds its enclosing window");
    }
    return {};
}

Status crowdb::tree::configure_tree_tuning(const ct_tree_tuning *options, Config *out) noexcept
{
    if (out == nullptr || (options != nullptr && options->struct_size != sizeof(*options))) {
        return Status::invalid_argument("Tree tuning struct size mismatch");
    }
    if (options == nullptr) {
        return {};
    }
    out->memtable_flush_bytes                  = options->memtable_flush_bytes;
    out->memtable_flush_entries                = options->memtable_flush_entries;
    out->max_memtable_count                    = options->max_memtable_count;
    out->leaf_split_bytes                      = options->leaf_split_bytes;
    out->leaf_merge_bytes                      = options->leaf_merge_bytes;
    out->inner_max_keys                        = options->inner_max_keys;
    out->inner_merge_keys                      = options->inner_merge_keys;
    out->max_key_size                          = options->max_key_size;
    out->execution.source_max_entries          = options->source_max_entries;
    out->execution.source_max_key_bytes        = options->source_max_key_bytes;
    out->execution.source_max_gap_entries      = options->source_max_gap_entries;
    out->execution.source_max_gap_key_bytes    = options->source_max_gap_key_bytes;
    out->execution.flush_parallelism           = options->flush_parallelism;
    out->execution.run_max_entries             = options->run_max_entries;
    out->execution.run_max_key_bytes           = options->run_max_key_bytes;
    out->execution.run_key_block_bytes         = options->run_key_block_bytes;
    out->execution.run_max_bytes               = options->run_max_bytes;
    out->execution.install_run_window          = options->install_run_window;
    out->execution.maintenance_visits_per_step = options->maintenance_visits_per_step;
    out->execution.maintenance_max_visits      = options->maintenance_max_visits;
    out->execution.maintenance_max_candidates  = options->maintenance_max_candidates;
    out->execution.maintenance_max_steps       = options->maintenance_max_steps;
    out->execution.maintenance_metadata_bytes  = options->maintenance_metadata_bytes;
    out->execution.cache_max_pages             = options->cache_max_pages;
    return validate_tree_execution(out->execution);
}
