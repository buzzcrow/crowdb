// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/tree_work_metrics_c_api.h"

extern "C" {
// Nanoseconds. Quantiles use common LatencyHistogram bucket upper bounds;
// overflow quantiles are UINT64_MAX. The exact sum is (sum_high << 64)|sum_low.
struct ct_tree_histogram_v1
{
    uint64_t count;
    uint64_t sum_low;
    uint64_t sum_high;
    uint64_t min_ns;
    uint64_t max_ns;
    uint64_t p50_ns;
    uint64_t p90_ns;
    uint64_t p95_ns;
    uint64_t p99_ns;
    uint64_t p999_ns;
    uint64_t zero_count;
    uint64_t overflow_count;
    uint64_t total_count;
};

struct ct_tree_work_histograms_v1
{
    uint32_t             struct_size;
    uint32_t             version;
    uint64_t             window_sequence;
    uint64_t             sampled_at_ns;
    uint64_t             enabled_stages;
    ct_tree_histogram_v1 stages[CT_TREE_WORK_STAGES_V1];
};

// Stage follows the fixed work-v1 order. Default is disabled. Enable is
// additive/idempotent, charges real bucket storage to this Tree, and can fail
// without changing the selected set or business admission. No completion
// allocates.
int32_t ct_work_histogram_enable_v1(ct_tree *tree, uint32_t stage);
// Non-draining cumulative sample. Disabled slots are zero and absent in mask.
int32_t ct_work_histograms_totals_v1(ct_tree *tree, ct_tree_work_histograms_v1 *out);
// The same exclusive collector claim drains summaries and selected histograms
// exactly once. Fan out the immutable result; old v1/v2 layouts are unchanged.
// Invalid headers or overlapping calls leave all outputs/windows untouched.
int32_t ct_work_metrics_capture_v3(ct_work_metrics_collector *collector, ct_tree_work_snapshot_v1 *out,
                                   ct_tree_work_ages_v1 *ages, ct_tree_work_counters_v1 *counters,
                                   ct_tree_work_histograms_v1 *histograms);
}
