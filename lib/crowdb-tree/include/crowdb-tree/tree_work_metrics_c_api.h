// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <cstdint>

extern "C" {
struct ct_tree;
struct ct_work_metrics_collector;

enum { CT_TREE_WORK_STAGES_V1 = 21, CT_TREE_WORK_OUTCOMES_V1 = 5 };

struct ct_tree_work_stage_v1
{
    uint64_t attempts;
    uint64_t inflight;
    uint64_t total_count;
    uint64_t total_sum_ns;
    uint64_t total_max_ns;
    uint64_t outcomes[CT_TREE_WORK_OUTCOMES_V1];
    uint64_t window_count;
    uint64_t window_sum_ns;
    uint64_t window_max_ns;
};

// Stage order is fixed by v1. Totals are non-resetting relaxed samples;
// window fields are populated only by the single collector capture.
struct ct_tree_work_snapshot_v1
{
    uint32_t              struct_size;
    uint32_t              version;
    uint64_t              window_sequence;
    uint64_t              sampled_at_ns;
    ct_tree_work_stage_v1 stages[CT_TREE_WORK_STAGES_V1];
};

// Exactly one collector per runtime Tree. Legacy Trees and a second claim
// return Unavailable. The handle retains the metrics ledger after Tree close.
int32_t ct_work_metrics_collector_create(ct_tree *tree, ct_work_metrics_collector **out);
void    ct_work_metrics_collector_free(ct_work_metrics_collector *collector);
// Set size/version=1. Concurrent capture returns Unavailable without draining.
// Distribute one returned value to all consumers; copying never resets samples.
int32_t ct_work_metrics_capture(ct_work_metrics_collector *collector, ct_tree_work_snapshot_v1 *out);
int32_t ct_work_metrics_totals(ct_tree *tree, ct_tree_work_snapshot_v1 *out);

// Independent, non-draining age schema. Samples are approximate during work.
// A full diagnostic window never rejects an operation: untracked reports lost
// coverage, and observed_oldest_age_ns only covers the tracked subset. Treat it
// as the oldest active age only when complete is 1. An empty stage has age 0.
struct ct_tree_work_stage_age_v1
{
    uint64_t inflight;
    uint64_t tracked;
    uint64_t untracked;
    uint64_t observed_oldest_age_ns;
    uint64_t complete;
};

struct ct_tree_work_ages_v1
{
    uint32_t                  struct_size;
    uint32_t                  version;
    uint64_t                  sampled_at_ns;
    ct_tree_work_stage_age_v1 stages[CT_TREE_WORK_STAGES_V1];
};

int32_t ct_work_metrics_ages(ct_tree *tree, ct_tree_work_ages_v1 *out);
// Valid after Tree close, while the collector handle remains alive.
int32_t ct_work_metrics_collector_ages(ct_work_metrics_collector *collector, ct_tree_work_ages_v1 *out);

// Independent versioned schema; sampling never drains the stage collector.
// Counters are cumulative except active_steps, which is a relaxed gauge.
struct ct_tree_flush_dispatch_v1
{
    uint32_t struct_size;
    uint32_t version;
    uint64_t sampled_at_ns;
    uint64_t inline_probes;
    uint64_t inline_ranges;
    uint64_t queued_steps;
    uint64_t prepared_runs;
    uint64_t prepared_entries;
    uint64_t prepared_bytes;
    uint64_t cow_bytes;
    uint64_t active_steps;
    uint64_t peak_steps;
};

int32_t ct_flush_dispatch_totals(ct_tree *tree, ct_tree_flush_dispatch_v1 *out);

// Additive fixed-dimension quantities. Cause order: other, flush, cache
// pressure, checkpoint, GC, read, snapshot, allocation. No field resets.
enum { CT_TREE_WORK_COUNTERS_V1 = 55, CT_TREE_WORK_CAUSES_V1 = 8 };

struct ct_tree_work_counters_v1
{
    uint32_t struct_size;
    uint32_t version;
    uint64_t sampled_at_ns;
    uint64_t values[CT_TREE_WORK_CAUSES_V1][CT_TREE_WORK_COUNTERS_V1];
};

int32_t ct_work_counters_totals(ct_tree *tree, ct_tree_work_counters_v1 *out);
int32_t ct_work_metrics_collector_counters(ct_work_metrics_collector *collector, ct_tree_work_counters_v1 *out);
// One collector claim samples all additive schemas and drains exactly one
// window. Invalid headers or concurrent capture leave every output untouched.
int32_t     ct_work_metrics_capture_v2(ct_work_metrics_collector *collector, ct_tree_work_snapshot_v1 *out,
                                       ct_tree_work_ages_v1 *ages, ct_tree_work_counters_v1 *counters);
const char *ct_work_counter_name_v1(uint32_t index);
const char *ct_work_cause_name_v1(uint32_t index);
}
