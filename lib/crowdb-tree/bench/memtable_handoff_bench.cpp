// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/log.h"
#include "crowdb-tree/crowdb-tree.h"

#include <benchmark/benchmark.h>
#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <thread>
#include <vector>

namespace
{
using namespace crowdb::tree;
using Clock = std::chrono::steady_clock;

template <class Stats> void record_retention(benchmark::State &state, const Stats &stats)
{
    // The identical source also builds against the earlier engine baseline.
    if constexpr (requires { stats.mt_version_cas_retries; }) {
        auto add = [&](const char *name, uint64_t value) {
            auto &counter = state.counters[name];
            counter.value += static_cast<double>(value);
            counter.flags = benchmark::Counter::kAvgIterations;
        };
        add("overwrites", stats.mt_overwrite_total);
        add("history_keeps", stats.mt_history_keep_total);
        add("history_merges", stats.mt_history_merge_total);
        add("version_cas_retries", stats.mt_version_cas_retries);
        add("resident_bytes", stats.mt_resident_bytes);
    }
}

void bm_memtable_handoff(benchmark::State &state)
{
    crowdb::common::init_logging("/tmp/crowdb-handoff-benchmark", "error");
    crowdb::common::add_log_stderr("error");
    const int workers    = static_cast<int>(state.range(0));
    const int batch_size = static_cast<int>(state.range(1));
    const int mode       = static_cast<int>(state.range(2));
    const int batches    = mode == 2 ? 128 : 2048;
    double    p50        = 0;
    double    p99        = 0;
    for (auto iteration : state) {
        (void)iteration;
        state.PauseTiming();
        Config options;
        options.buffer_pool_bytes                = 8 * 1024 * 1024;
        options.memtable_flush_entries           = 1U << 30;
        options.memtable_flush_bytes             = 1ULL << 40;
        auto                               owner = std::make_unique<Crowdbtree>(options);
        auto                              &tree  = *owner;
        std::vector<std::vector<Batch>>    input(workers);
        std::vector<std::vector<uint64_t>> latency(workers);
        for (int worker = 0; worker < workers; ++worker) {
            for (int i = 0; i < batches; ++i) {
                Batch batch;
                for (int j = 0; j < batch_size; ++j) {
                    auto key = mode == 1 || mode == 2
                                 ? "hot"
                                 : "key" + std::to_string(worker) + ":" + std::to_string(((i * batch_size) + j) % 128);
                    batch.ops.push_back({.key = key, .kind = OpKind::kPut, .value = std::string(64, 'v')});
                }
                input[worker].push_back(std::move(batch));
            }
            latency[worker].resize(batches);
        }
        std::atomic<uint64_t>    slots{mode == 2 ? 1U : 0U};
        std::atomic<bool>        done{false};
        std::atomic<bool>        failed{false};
        std::barrier             start(workers + 1);
        std::vector<std::thread> writers;
        writers.reserve(workers);
        state.ResumeTiming();
        for (int worker = 0; worker < workers; ++worker) {
            writers.emplace_back([&, worker] {
                start.arrive_and_wait();
                for (int i = 0; i < batches; ++i) {
                    const auto slot  = slots.fetch_add(1) + 1;
                    const auto begin = Clock::now();
                    if (!tree.apply(slot, input[worker][i]).ok()) {
                        failed.store(true);
                    }
                    latency[worker][i] = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin).count());
                }
            });
        }
        std::thread maintenance;
        if (mode == 3) {
            maintenance = std::thread([&] {
                while (!done.load()) {
                    std::vector<scan_entry> rows;
                    bool                    truncated = false;
                    if (!tree.scan({}, {}, {}, 64, 0, false, 0, &rows, &truncated).ok() || !tree.flush().ok()) {
                        failed.store(true);
                    }
                    std::this_thread::yield();
                }
            });
        }
        start.arrive_and_wait();
        for (auto &writer : writers) {
            writer.join();
        }
        done.store(true);
        if (maintenance.joinable()) {
            maintenance.join();
        }
        state.PauseTiming();
        if (mode == 2 && !tree.apply(1, {}).ok()) {
            failed.store(true);
        }
        if (tree.contiguous_slot() != slots.load()) {
            failed.store(true);
        }
        std::vector<uint64_t> samples;
        for (const auto &values : latency) {
            samples.insert(samples.end(), values.begin(), values.end());
        }
        std::sort(samples.begin(), samples.end());
        p50 += static_cast<double>(samples[samples.size() / 2]);
        p99 += static_cast<double>(samples[(samples.size() * 99) / 100]);
        if (failed.load()) {
            state.SkipWithError("apply/maintenance failed");
        }
        record_retention(state, tree.stats());
        owner.reset();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations() * workers * batches * batch_size);
    state.counters["batch_p50_ns"] = p50 / static_cast<double>(state.iterations());
    state.counters["batch_p99_ns"] = p99 / static_cast<double>(state.iterations());
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    state.counters["process_peak_rss_kib"] = static_cast<double>(usage.ru_maxrss);
}

BENCHMARK(bm_memtable_handoff)
    ->Args({1, 1, 0})
    ->Args({4, 1, 0})
    ->Args({8, 1, 0})
    ->Args({1, 16, 0})
    ->Args({4, 16, 0})
    ->Args({1, 1, 1})
    ->Args({4, 1, 1})
    ->Args({4, 1, 2})
    ->Args({4, 1, 3})
    ->UseRealTime()
    ->MeasureProcessCPUTime();
} // namespace
