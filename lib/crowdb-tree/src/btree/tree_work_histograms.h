// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/tree_metrics.h"

namespace crowdb::tree
{
// Fixed result storage only. Bucket arrays exist solely for explicitly enabled
// stages. Quantiles are common LatencyHistogram bucket upper bounds in ns.
struct TreeHistogramSnapshot
{
    uint64_t count          = 0;
    uint64_t sum_low        = 0;
    uint64_t sum_high       = 0;
    uint64_t min_ns         = 0;
    uint64_t max_ns         = 0;
    uint64_t p50_ns         = 0;
    uint64_t p90_ns         = 0;
    uint64_t p95_ns         = 0;
    uint64_t p99_ns         = 0;
    uint64_t p999_ns        = 0;
    uint64_t zero_count     = 0;
    uint64_t overflow_count = 0;
    uint64_t total_count    = 0;
};

struct TreeWorkHistograms
{
    uint64_t                                           enabled_stages = 0;
    std::array<TreeHistogramSnapshot, kTreeStageCount> stages{};
};
} // namespace crowdb::tree
