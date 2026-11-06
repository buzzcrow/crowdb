// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#ifdef __cplusplus
#    include <cstdint>
extern "C" {
#else
#    include <stdint.h>
#endif
struct ct_tree;
struct ct_work_metrics_collector;

enum { CT_TREE_WAIT_CLASSES_V1 = 7, CT_TREE_WAIT_OUTCOMES_V1 = 3 };

// Order: handoff, routing, reclaim, admitted writer, gap repair, I/O,
// shared-cache join. Outcomes: granted/ready, error, cancelled.
struct ct_tree_wait_summary_v1
{
    uint64_t attempts;
    uint64_t inflight;
    uint64_t total_count;
    uint64_t total_sum_ns;
    uint64_t total_max_ns;
    uint64_t outcomes[CT_TREE_WAIT_OUTCOMES_V1];
    uint64_t tracked;
    uint64_t untracked;
    uint64_t observed_oldest_age_ns;
    uint64_t complete;
};

// Independent cumulative relaxed sample; never drains a collector window.
// Incomplete age coverage never rejects work. Oldest age covers tracked work
// only unless complete=1. Budget durations cover Request submit to terminal;
// host totals include other accounts and direct non-waiting reservations too.
struct ct_tree_wait_snapshot_v1
{
    uint32_t                       struct_size;
    uint32_t                       version;
    uint64_t                       sampled_at_ns;
    struct ct_tree_wait_summary_v1 classes[CT_TREE_WAIT_CLASSES_V1];
};

int32_t ct_tree_wait_totals(struct ct_tree *tree, struct ct_tree_wait_snapshot_v1 *out);
int32_t ct_work_metrics_collector_wait_totals(struct ct_work_metrics_collector *collector,
                                              struct ct_tree_wait_snapshot_v1  *out);
#ifdef __cplusplus
}
#endif
