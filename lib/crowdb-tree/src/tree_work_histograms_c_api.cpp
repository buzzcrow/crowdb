// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/tree_work_histograms_c_api.h"

#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "tree_work_histograms_c_api_internal.h"

namespace crowdb::tree
{
ct_tree_work_histograms_v1 copy_work_histograms(const TreeWorkHistograms &value, uint64_t sequence) noexcept
{
    ct_tree_work_histograms_v1 result{};
    result.struct_size     = sizeof(result);
    result.version         = 1;
    result.window_sequence = sequence;
    result.sampled_at_ns   = tree_metrics_now_ns();
    result.enabled_stages  = value.enabled_stages;
    for (size_t i = 0; i < value.stages.size(); ++i) {
        const auto &stage = value.stages[i];
        result.stages[i]  = {.count          = stage.count,
                             .sum_low        = stage.sum_low,
                             .sum_high       = stage.sum_high,
                             .min_ns         = stage.min_ns,
                             .max_ns         = stage.max_ns,
                             .p50_ns         = stage.p50_ns,
                             .p90_ns         = stage.p90_ns,
                             .p95_ns         = stage.p95_ns,
                             .p99_ns         = stage.p99_ns,
                             .p999_ns        = stage.p999_ns,
                             .zero_count     = stage.zero_count,
                             .overflow_count = stage.overflow_count,
                             .total_count    = stage.total_count};
    }
    return result;
}
} // namespace crowdb::tree

using namespace crowdb::tree;

int32_t ct_work_histogram_enable_v1(ct_tree *tree, uint32_t stage)
{
    if (tree == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    return core == nullptr ? static_cast<int32_t>(Code::kUnavailable)
                           : static_cast<int32_t>(core->enable_work_histogram(stage).code());
}

int32_t ct_work_histograms_totals_v1(ct_tree *tree, ct_tree_work_histograms_v1 *out)
{
    if (tree == nullptr || out == nullptr || out->struct_size != sizeof(*out) || out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    *out = copy_work_histograms(core->publisher().work_metrics()->histogram_totals(), 0);
    return 0;
}
