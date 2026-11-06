// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#ifdef __cplusplus
#    include <cstdint>
#else
#    include <stdint.h>
#endif
#ifdef __cplusplus
extern "C" {
#endif
struct ct_tree;
struct ct_work_metrics_collector;

// Equal nonzero scopes identify the same backend ledger. Do not sum views of
// one scope. This additive API does not change ct_tree_stats_snapshot_v1.
typedef struct ct_backing_stats_v1
{
    uint32_t struct_size;
    uint32_t version;
    uint64_t scope_id;
    uint64_t allocated_bytes;
} ct_backing_stats_v1;

int32_t ct_get_backing_stats(struct ct_tree *tree, ct_backing_stats_v1 *out);
// The collector retains only the ledger counters, including after Tree close.
int32_t ct_work_metrics_collector_backing_stats(struct ct_work_metrics_collector *collector, ct_backing_stats_v1 *out);
#ifdef __cplusplus
}
#endif
