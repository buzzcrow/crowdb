// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/tree_work_metrics_c_api.h"

#include "btree/backing_stats_sampler.h"
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "tree_backing_stats_c_api_internal.h"
#include "tree_wait_metrics_c_api_internal.h"
#include "tree_work_histograms_c_api_internal.h"

using namespace crowdb::tree;

static_assert(CT_TREE_WORK_STAGES_V1 == kTreeStageCount);
static_assert(CT_TREE_WORK_OUTCOMES_V1 == kTreeOutcomeCount);
static_assert(CT_TREE_WORK_COUNTERS_V1 == kTreeWorkCounterCount);
static_assert(CT_TREE_WORK_CAUSES_V1 == kTreeWorkCauseCount);

struct ct_work_metrics_collector
{
    std::shared_ptr<AllocationBudget>     metadata;
    std::shared_ptr<TreeWorkMetrics>      metrics;
    std::shared_ptr<TreeMetricsCollector> owner;
    BackingStatsSampler                   backing;
    std::shared_ptr<TreeResourceMetrics>  resources;
    std::atomic_flag                      capturing = ATOMIC_FLAG_INIT;
    uint64_t                              sequence  = 0;
};

namespace
{
bool valid(const ct_tree_work_snapshot_v1 *out)
{
    return out != nullptr && out->struct_size == sizeof(*out) && out->version == 1;
}

bool valid(const ct_tree_work_ages_v1 *out)
{
    return out != nullptr && out->struct_size == sizeof(*out) && out->version == 1;
}

bool valid(const ct_tree_work_counters_v1 *out)
{
    return out != nullptr && out->struct_size == sizeof(*out) && out->version == 1;
}

ct_tree_work_counters_v1 sample_counters(const TreeWorkMetrics &metrics)
{
    ct_tree_work_counters_v1 result{};
    result.struct_size   = sizeof(result);
    result.version       = 1;
    result.sampled_at_ns = tree_metrics_now_ns();
    const auto values    = metrics.counters.snapshot();
    for (size_t t = 0; t < kTreeWorkCauseCount; ++t) {
        for (size_t c = 0; c < kTreeWorkCounterCount; ++c) {
            result.values[t][c] = values[t][c];
        }
    }
    return result;
}

ct_tree_work_ages_v1 sample_ages(const TreeWorkMetrics &metrics)
{
    ct_tree_work_ages_v1 result{};
    result.struct_size   = sizeof(result);
    result.version       = 1;
    result.sampled_at_ns = tree_metrics_now_ns();
    const auto ages      = metrics.ages(result.sampled_at_ns);
    for (size_t i = 0; i < ages.size(); ++i) {
        result.stages[i] = {.inflight               = ages[i].inflight,
                            .tracked                = ages[i].tracked,
                            .untracked              = ages[i].untracked,
                            .observed_oldest_age_ns = ages[i].oldest_age_ns,
                            .complete               = static_cast<uint64_t>(ages[i].complete)};
    }
    return result;
}

ct_tree_work_snapshot_v1 sample(const TreeWorkMetrics &metrics)
{
    ct_tree_work_snapshot_v1 result{};
    result.struct_size   = sizeof(result);
    result.version       = 1;
    result.sampled_at_ns = tree_metrics_now_ns();
    const auto totals    = metrics.totals();
    for (size_t i = 0; i < totals.size(); ++i) {
        auto &stage        = result.stages[i];
        stage.attempts     = totals[i].attempts;
        stage.inflight     = totals[i].inflight;
        stage.total_count  = totals[i].count;
        stage.total_sum_ns = totals[i].sum_ns;
        stage.total_max_ns = totals[i].max_ns;
        for (size_t j = 0; j < totals[i].outcomes.size(); ++j) {
            stage.outcomes[j] = totals[i].outcomes[j];
        }
    }
    return result;
}
} // namespace

int32_t ct_work_metrics_collector_create(ct_tree *tree, ct_work_metrics_collector **out)
try {
    if (tree == nullptr || out == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out       = nullptr;
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    const auto &budget  = core->binding()->lifecycle_budget();
    const auto &metrics = core->publisher().work_metrics();
    auto        owner   = metrics->claim_collector(budget);
    if (!owner) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    MetadataAllocator<ct_work_metrics_collector> allocator(budget);
    auto                                        *collector = allocator.allocate(1);
    std::construct_at(collector);
    collector->backing   = core->backing_sampler();
    collector->resources = core->binding()->resource_metrics();
    collector->metadata  = budget;
    collector->metrics   = metrics;
    collector->owner     = std::move(owner);
    *out                 = collector;
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}

void ct_work_metrics_collector_free(ct_work_metrics_collector *collector)
{
    if (collector != nullptr) {
        MetadataAllocator<ct_work_metrics_collector> allocator(collector->metadata);
        std::destroy_at(collector);
        allocator.deallocate(collector, 1);
    }
}

int32_t ct_work_metrics_capture(ct_work_metrics_collector *collector, ct_tree_work_snapshot_v1 *out)
{
    if (collector == nullptr || !valid(out)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    if (collector->capturing.test_and_set(std::memory_order_acquire)) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    auto       result = sample(*collector->metrics);
    const auto window = collector->owner->capture_window();
    // Every ABI advances the same collector window, including enabled
    // distributions that this older output layout cannot represent.
    (void)collector->owner->capture_histograms();
    result.window_sequence = ++collector->sequence;
    for (size_t i = 0; i < window.size(); ++i) {
        result.stages[i].window_count  = window[i].count;
        result.stages[i].window_sum_ns = window[i].sum_ns;
        result.stages[i].window_max_ns = window[i].max_ns;
    }
    collector->capturing.clear(std::memory_order_release);
    *out = result;
    return 0;
}

int32_t ct_work_metrics_totals(ct_tree *tree, ct_tree_work_snapshot_v1 *out)
{
    if (tree == nullptr || !valid(out)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    *out = sample(*core->publisher().work_metrics());
    return 0;
}

int32_t ct_flush_dispatch_totals(ct_tree *tree, ct_tree_flush_dispatch_v1 *out)
{
    if (tree == nullptr || out == nullptr || out->struct_size != sizeof(*out) || out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    const auto value = core->publisher().work_metrics()->dispatch.snapshot();
    *out             = {.struct_size      = sizeof(*out),
                        .version          = 1,
                        .sampled_at_ns    = tree_metrics_now_ns(),
                        .inline_probes    = value.inline_probes,
                        .inline_ranges    = value.inline_ranges,
                        .queued_steps     = value.queued_steps,
                        .prepared_runs    = value.prepared_runs,
                        .prepared_entries = value.prepared_entries,
                        .prepared_bytes   = value.prepared_bytes,
                        .cow_bytes        = value.cow_bytes,
                        .active_steps     = value.active_steps,
                        .peak_steps       = value.peak_steps};
    return 0;
}

int32_t ct_work_metrics_ages(ct_tree *tree, ct_tree_work_ages_v1 *out)
{
    if (tree == nullptr || !valid(out)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    *out = sample_ages(*core->publisher().work_metrics());
    return 0;
}

int32_t ct_work_metrics_collector_ages(ct_work_metrics_collector *collector, ct_tree_work_ages_v1 *out)
{
    if (collector == nullptr || !valid(out)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out = sample_ages(*collector->metrics);
    return 0;
}

int32_t ct_work_metrics_collector_backing_stats(ct_work_metrics_collector *collector, ct_backing_stats_v1 *out)
{
    return collector == nullptr ? static_cast<int32_t>(Code::kInvalidArgument)
                                : copy_backing_stats(collector->backing.snapshot(), out);
}

int32_t ct_work_counters_totals(ct_tree *tree, ct_tree_work_counters_v1 *out)
{
    if (tree == nullptr || !valid(out)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    *out = sample_counters(*core->publisher().work_metrics());
    return 0;
}

int32_t ct_work_metrics_collector_counters(ct_work_metrics_collector *collector, ct_tree_work_counters_v1 *out)
{
    if (collector == nullptr || !valid(out)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out = sample_counters(*collector->metrics);
    return 0;
}

static int32_t capture_all(ct_work_metrics_collector *collector, ct_tree_work_snapshot_v1 *out,
                           ct_tree_work_ages_v1 *ages, ct_tree_work_counters_v1 *counters,
                           ct_tree_work_histograms_v1 *histograms)
{
    if (collector == nullptr || !valid(out) || !valid(ages) || !valid(counters)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    if (collector->capturing.test_and_set(std::memory_order_acquire)) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    auto       result           = sample(*collector->metrics);
    const auto age_result       = sample_ages(*collector->metrics);
    const auto counter_result   = sample_counters(*collector->metrics);
    const auto window           = collector->owner->capture_window();
    const auto histogram_window = collector->owner->capture_histograms();
    result.window_sequence      = ++collector->sequence;
    for (size_t i = 0; i < window.size(); ++i) {
        result.stages[i].window_count  = window[i].count;
        result.stages[i].window_sum_ns = window[i].sum_ns;
        result.stages[i].window_max_ns = window[i].max_ns;
    }
    if (histograms != nullptr) {
        *histograms = copy_work_histograms(histogram_window, result.window_sequence);
    }
    *out      = result;
    *ages     = age_result;
    *counters = counter_result;
    collector->capturing.clear(std::memory_order_release);
    return 0;
}

int32_t ct_work_metrics_capture_v2(ct_work_metrics_collector *collector, ct_tree_work_snapshot_v1 *out,
                                   ct_tree_work_ages_v1 *ages, ct_tree_work_counters_v1 *counters)
{
    return capture_all(collector, out, ages, counters, nullptr);
}

int32_t ct_work_metrics_capture_v3(ct_work_metrics_collector *collector, ct_tree_work_snapshot_v1 *out,
                                   ct_tree_work_ages_v1 *ages, ct_tree_work_counters_v1 *counters,
                                   ct_tree_work_histograms_v1 *histograms)
{
    if (histograms == nullptr || histograms->struct_size != sizeof(*histograms) || histograms->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    return capture_all(collector, out, ages, counters, histograms);
}

const char *ct_work_counter_name_v1(uint32_t index)
{
    return index < kTreeWorkCounterCount ? kTreeWorkCounterNames[index].data() : nullptr;
}

const char *ct_work_cause_name_v1(uint32_t index)
{
    return index < kTreeWorkCauseCount ? kTreeWorkCauseNames[index].data() : nullptr;
}

int32_t ct_work_metrics_collector_wait_totals(ct_work_metrics_collector *collector, ct_tree_wait_snapshot_v1 *out)
{
    return copy_tree_wait_totals(collector != nullptr ? collector->metrics.get() : nullptr, out);
}
