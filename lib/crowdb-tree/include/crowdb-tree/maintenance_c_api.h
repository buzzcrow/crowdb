// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>

extern "C" {
struct ct_tree;
struct ct_future;

struct ct_flush_result_v1
{
    uint32_t struct_size;
    uint32_t version;
    int32_t  terminal_status;
    int32_t  handoff_status;
    int32_t  maintenance_status;
    uint32_t phase;
    uint64_t batch_id;
    uint64_t floor;
    uint64_t frontier;
    uint64_t published_coverage;
    uint64_t deferred_pages;
    int32_t  handoff_completed;
    int32_t  handoff_advanced;
};

struct ct_checkpoint_result_v1
{
    uint32_t struct_size;
    uint32_t version;
    int32_t  terminal_status;
    int32_t  durable_committed;
    int32_t  writeback_completed;
    uint64_t durable_coverage;
    uint64_t snapshot_sequence;
    uint64_t checkpoint_token;
    uint64_t captured_coverage;
    uint64_t primary_frames;
    uint64_t written_frames;
    uint64_t reused_frames;
    uint64_t rebased_pages;
    uint64_t rebase_deferred;
};

struct ct_compaction_options_v1
{
    uint32_t struct_size;
    uint32_t version;
    double   free_threshold;
    uint64_t max_relocation_bytes;
};

struct ct_compaction_result_v1
{
    uint32_t struct_size;
    uint32_t version;
    int32_t  terminal_status;
    uint64_t blocks_selected;
    uint64_t pages_relocated;
    uint64_t bytes_relocated;
    uint64_t blocks_deleted;
};

// Runtime-only bounded pass. Null options select threshold 0.70 and one
// eligible block; explicit max_relocation_bytes=0 disables relocation.
int32_t ct_compact_sparse_blocks_async(ct_tree *tree, const ct_compaction_options_v1 *options, ct_future **out);
int32_t ct_future_poll_compaction(ct_future *future, int32_t *done, ct_compaction_result_v1 *out);

// Exact checkpoint frontier; publication/flush alone never advances it.
// Returns zero for a null handle or a Tree with no durable checkpoint.
uint64_t ct_durable_coverage(ct_tree *tree);

// Runtime Trees only. A successful start returns an owned future, including
// immediate operation failures. Admission failures return no future.
int32_t ct_flush_report_async(ct_tree *tree, ct_future **out);
int32_t ct_writeback_async(ct_tree *tree, ct_future **out);
int32_t ct_checkpoint_report_async(ct_tree *tree, ct_future **out);

// Initialize struct_size=sizeof(*out), version=1. A poll error leaves the
// future and output unchanged. done=0 retains the future. done=1 copies the
// report and consumes the future, even when report.terminal_status is an error.
// Stage errors never discard an accepted operation's progress report.
// ct_future_free may abandon a pending report; accepted callbacks retain all
// native operation state until terminal. Legacy ct_future_poll rejects it.
int32_t ct_future_poll_flush_report(ct_future *future, int32_t *done, ct_flush_result_v1 *out);
int32_t ct_future_poll_checkpoint_report(ct_future *future, int32_t *done, ct_checkpoint_result_v1 *out);
}
