// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_work_registry.h"

#include "allocation_reservation.h"

#include <array>
#include <cstdio>

namespace crowdb::tree
{
namespace
{
// Registry serializes flushes and invokes its inline callbacks in registration
// order before sorting output names. Only the first callback samples the age
// slots. These observations never claim or drain the host collector window.
struct WorkSample
{
    explicit WorkSample(std::shared_ptr<TreeWorkMetrics> value) : metrics(std::move(value))
    {
    }

    std::shared_ptr<TreeWorkMetrics>             metrics;
    TreeWorkTotals                               totals{};
    TreeWorkAges                                 ages{};
    TreeCounterTotals                            counters{};
    std::array<TreeWaitTotals, kTreeWaitClasses> waits{};

    uint64_t refresh() noexcept
    {
        const auto now = tree_metrics_now_ns();
        totals         = metrics->totals();
        ages           = metrics->ages(now);
        counters       = metrics->counters.snapshot();
        waits          = metrics->waits.totals(now);
        return now;
    }
};

constexpr std::array<std::string_view, kTreeStageCount>   stages{"flush.batch",
                                                                 "flush.admission_wait",
                                                                 "flush.capture",
                                                                 "flush.writer_drain",
                                                                 "flush.handoff",
                                                                 "flush.range",
                                                                 "publish.queue_wait",
                                                                 "publish.commit",
                                                                 "maintenance.plan",
                                                                 "overlay.coalesce",
                                                                 "page.split",
                                                                 "page.merge",
                                                                 "page.layout",
                                                                 "page.encode",
                                                                 "budget.wait",
                                                                 "page.load_pipeline",
                                                                 "page.writeback_pipeline",
                                                                 "snapshot.capture",
                                                                 "snapshot.commit",
                                                                 "memory.retire_wait",
                                                                 "flush.repair"};
constexpr std::array<std::string_view, kTreeOutcomeCount> outcomes{"success", "error", "cancelled", "retry", "stale"};

template <class F>
void add(crowdb::common::metrics::MetricsRegistry &registry, std::string_view prefix, std::string_view first,
         std::string_view second, F callback)
{
    std::array<char, 192> suffix{};
    const int size = std::snprintf(suffix.data(), suffix.size(), ".work.%.*s.%.*s", static_cast<int>(first.size()),
                                   first.data(), static_cast<int>(second.size()), second.data());
    registry.register_callback_gauge_inline(prefix, std::string_view(suffix.data(), static_cast<size_t>(size)),
                                            callback);
}
} // namespace

std::shared_ptr<crowdb::common::metrics::MetricsRegistry>
create_tree_work_registry(std::shared_ptr<std::pmr::memory_resource> resource, std::string_view prefix,
                          const std::shared_ptr<TreeWorkMetrics>  &metrics,
                          const std::shared_ptr<AllocationBudget> &metadata)
{
    struct Owner
    {
        Owner(std::shared_ptr<std::pmr::memory_resource> resource, std::shared_ptr<TreeWorkMetrics> metrics)
            : sample(std::move(metrics)),
              registry(std::move(resource))
        {
        }

        WorkSample                               sample;
        crowdb::common::metrics::MetricsRegistry registry;
    };

    auto  owner    = std::allocate_shared<Owner>(MetadataAllocator<Owner>(metadata), std::move(resource), metrics);
    auto *sample   = &owner->sample;
    auto &registry = owner->registry;
    registry.register_callback_gauge_inline(prefix, ".work.sampled_at.ns", [sample] { return sample->refresh(); });
    for (size_t i = 0; i < stages.size(); ++i) {
        auto total = [&](std::string_view name, uint64_t TreeStageTotals::*field) {
            add(registry, prefix, stages[i], name, [sample, i, field] { return sample->totals[i].*field; });
        };
        total("attempts.c", &TreeStageTotals::attempts);
        total("inflight.g", &TreeStageTotals::inflight);
        total("latency.count.c", &TreeStageTotals::count);
        total("latency.sum.ns.c", &TreeStageTotals::sum_ns);
        total("latency.max.ns.g", &TreeStageTotals::max_ns);
        for (size_t j = 0; j < outcomes.size(); ++j) {
            add(registry, prefix, stages[i], outcomes[j], [sample, i, j] { return sample->totals[i].outcomes[j]; });
        }
        auto age = [&](std::string_view name, uint64_t TreeStageAge::*field) {
            add(registry, prefix, stages[i], name, [sample, i, field] { return sample->ages[i].*field; });
        };
        age("age.tracked.g", &TreeStageAge::tracked);
        age("age.untracked.g", &TreeStageAge::untracked);
        age("age.oldest.ns.g", &TreeStageAge::oldest_age_ns);
        add(registry, prefix, stages[i], "age.complete.g",
            [sample, i] { return static_cast<uint64_t>(sample->ages[i].complete); });
    }
    constexpr std::array<std::string_view, kTreeWaitClasses> wait_names{
        "wait.handoff",    "wait.routing", "wait.reclaim",     "wait.admitted_writer",
        "wait.gap_repair", "wait.io",      "wait.shared_cache"};
    constexpr std::array<std::string_view, kTreeWaitOutcomes> wait_outcomes{"granted", "error", "cancelled"};
    for (size_t i = 0; i < wait_names.size(); ++i) {
        auto total = [&](std::string_view name, uint64_t TreeWaitTotals::*field) {
            add(registry, prefix, wait_names[i], name, [sample, i, field] { return sample->waits[i].*field; });
        };
        total("attempts.c", &TreeWaitTotals::attempts);
        total("inflight.g", &TreeWaitTotals::inflight);
        total("latency.count.c", &TreeWaitTotals::count);
        total("latency.sum.ns.c", &TreeWaitTotals::sum_ns);
        total("latency.max.ns.g", &TreeWaitTotals::max_ns);
        total("age.tracked.g", &TreeWaitTotals::tracked);
        total("age.untracked.g", &TreeWaitTotals::untracked);
        total("age.oldest.ns.g", &TreeWaitTotals::oldest_age_ns);
        add(registry, prefix, wait_names[i], "age.complete.g",
            [sample, i] { return static_cast<uint64_t>(sample->waits[i].complete); });
        for (size_t j = 0; j < wait_outcomes.size(); ++j) {
            add(registry, prefix, wait_names[i], wait_outcomes[j],
                [sample, i, j] { return sample->waits[i].outcomes[j]; });
        }
    }
    for (size_t counter = 0; counter < kTreeWorkCounterCount; ++counter) {
        std::array<char, 128> suffix{};
        const auto            name = kTreeWorkCounterNames[counter];
        const int             size =
            std::snprintf(suffix.data(), suffix.size(), ".%.*s.c", static_cast<int>(name.size()), name.data());
        registry.register_callback_gauge_inline(prefix, std::string_view(suffix.data(), static_cast<size_t>(size)),
                                                [sample, counter] {
                                                    uint64_t total = 0;
                                                    for (const auto &cause : sample->counters) {
                                                        total += cause[counter];
                                                    }
                                                    return total;
                                                });
    }
    for (const auto [name, counter] : std::array{
             std::pair{std::string_view(".page.split.c"), TreeWorkCounter::SplitOperations},
             std::pair{std::string_view(".page.merge.c"), TreeWorkCounter::MergeOperations}
    }) {
        registry.register_callback_gauge_inline(prefix, name, [sample, counter] {
            uint64_t total = 0;
            for (const auto &cause : sample->counters) {
                total += cause[static_cast<size_t>(counter)];
            }
            return total;
        });
    }
    for (size_t cause = 0; cause < kTreeWorkCauseCount; ++cause) {
        for (size_t counter = 0; counter < kTreeWorkCounterCount; ++counter) {
            add(registry, prefix, kTreeWorkCauseNames[cause], kTreeWorkCounterNames[counter],
                [sample, cause, counter] { return sample->counters[cause][counter]; });
        }
    }
    return {owner, &owner->registry};
}
} // namespace crowdb::tree
