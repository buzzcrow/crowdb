// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "tree_wait_metrics_c_api_internal.h"

namespace crowdb::tree
{
static_assert(CT_TREE_WAIT_CLASSES_V1 == kTreeWaitClasses);
static_assert(CT_TREE_WAIT_OUTCOMES_V1 == kTreeWaitOutcomes);

int32_t copy_tree_wait_totals(const TreeWorkMetrics *metrics, ct_tree_wait_snapshot_v1 *out) noexcept
{
    if (metrics == nullptr || out == nullptr || out->struct_size != sizeof(*out) || out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    ct_tree_wait_snapshot_v1 result{};
    result.struct_size   = sizeof(result);
    result.version       = 1;
    result.sampled_at_ns = tree_metrics_now_ns();
    const auto values    = metrics->waits.totals(result.sampled_at_ns);
    for (size_t i = 0; i < values.size(); ++i) {
        const auto &in     = values[i];
        auto       &value  = result.classes[i];
        value.attempts     = in.attempts;
        value.inflight     = in.inflight;
        value.total_count  = in.count;
        value.total_sum_ns = in.sum_ns;
        value.total_max_ns = in.max_ns;
        for (size_t outcome = 0; outcome < in.outcomes.size(); ++outcome) {
            value.outcomes[outcome] = in.outcomes[outcome];
        }
        value.tracked                = in.tracked;
        value.untracked              = in.untracked;
        value.observed_oldest_age_ns = in.oldest_age_ns;
        value.complete               = static_cast<uint64_t>(in.complete);
    }
    *out = result;
    return 0;
}
} // namespace crowdb::tree

int32_t ct_tree_wait_totals(ct_tree *tree, ct_tree_wait_snapshot_v1 *out)
{
    if (tree == nullptr) {
        return static_cast<int32_t>(crowdb::tree::Code::kInvalidArgument);
    }
    auto *core = crowdb::tree::TreeCoreAccess::get(*c_api_tree(tree));
    return crowdb::tree::copy_tree_wait_totals(core->publisher().work_metrics().get(), out);
}
