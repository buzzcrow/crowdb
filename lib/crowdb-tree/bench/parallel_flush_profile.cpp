// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// One identical checked workload for the pre-parallel engine and current runtime.
#include "crowdb-tree/c_api.h"
#if __has_include("crowdb-tree/tree_work_histograms_c_api.h")
#    include "crowdb-tree/runtime_c_api.h"
#    include "crowdb-tree/tree_stats_c_api.h"
#    include "crowdb-tree/tree_work_histograms_c_api.h"
#    include "crowdb-tree/tuning_c_api.h"
#    define CROWDB_PROFILE_RUNTIME 1
#endif

#include <sys/resource.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
using Clock = std::chrono::steady_clock;
using Tree  = std::unique_ptr<ct_tree, decltype(&ct_close)>;

void check(int32_t status, const char *operation)
{
    if (status != 0) {
        throw std::runtime_error(std::string(operation) + ": " + std::to_string(status));
    }
}

uint64_t elapsed(Clock::time_point start)
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
}

struct Options
{
    size_t      workers, entries, value_bytes, batch, cold, trees, histogram;
    std::string pattern;
    size_t      source_entries = 4096;
    size_t      cache_bytes    = 64ULL * 1024 * 1024;
    size_t      cache_pages    = 256;
};

#ifdef CROWDB_PROFILE_RUNTIME
ct_tree_runtime_options tree_resources()
{
    ct_tree_runtime_options resources{};
    ct_tree_runtime_options_default(&resources);
    // Leave headroom for retained owners and the next admitted work window.
    constexpr size_t mib = 1024 * 1024;
    // Fixed cache/queue descriptors must fit even in the ample-cache profile.
    resources.lifecycle_bytes      = 8 * mib;
    resources.limits.handoff_bytes = 256 * mib;
    resources.limits.routing_bytes = 512 * mib;
    resources.limits.reclaim_bytes = 256 * mib;
    resources.limits.writer_bytes  = 512 * mib;
    return resources;
}
#endif

std::string key(size_t id, const Options &options)
{
    auto result = std::string(20 - std::to_string(id).size(), '0') + std::to_string(id);
    if (options.pattern == "long-key") {
        result += std::string(1024, 'k');
    }
    return result;
}

std::vector<Tree> open_trees(const Options &options)
{
#ifdef CROWDB_PROFILE_RUNTIME
    ct_runtime_options settings{};
    ct_runtime_options_default(&settings);
    settings.workers              = options.workers;
    const auto resources          = tree_resources();
    settings.limits.handoff_bytes = resources.limits.handoff_bytes * options.trees;
    settings.limits.routing_bytes = resources.limits.routing_bytes * options.trees;
    settings.limits.reclaim_bytes = resources.limits.reclaim_bytes * options.trees;
    settings.limits.writer_bytes  = resources.limits.writer_bytes * options.trees;
    ct_runtime *raw               = nullptr;
    check(ct_runtime_create(&settings, &raw), "runtime create");
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(raw, ct_runtime_free);
#endif
    std::vector<Tree> trees;
    trees.reserve(options.trees);
    for (size_t index = 0; index < options.trees; ++index) {
        ct_page_store *store = nullptr;
        check(ct_page_store_open_mem(1, &store), "mem store");
        std::unique_ptr<ct_page_store, decltype(&ct_page_store_free)> owner(store, ct_page_store_free);
        ct_options                                                    config{};
        config.page_store        = store;
        config.frame_bytes       = 65536;
        config.buffer_pool_bytes = options.cache_bytes;
        ct_tree *tree            = nullptr;
#ifdef CROWDB_PROFILE_RUNTIME
        ct_tree_tuning tuning{};
        ct_tree_tuning_default(&tuning);
        tuning.source_max_entries = options.source_entries;
        tuning.cache_max_pages    = options.cache_pages;
        config.tuning             = &tuning;
        check(ct_open_with_runtime(&config, raw, &resources, &tree), "open");
#else
        check(ct_open(&config, &tree), "open");
#endif
        trees.emplace_back(tree, ct_close);
#ifdef CROWDB_PROFILE_RUNTIME
        if (options.histogram != 0) {
            for (uint32_t stage = 0; stage < CT_TREE_WORK_STAGES_V1; ++stage) {
                check(ct_work_histogram_enable_v1(tree, stage), "enable histogram");
            }
        }
#endif
    }
    return trees;
}

void write_pass(ct_tree *tree, const Options &options, size_t pass, std::vector<uint64_t> *latencies)
{
    std::vector<std::string> keys(options.batch);
    std::vector<ct_kv_ref>   operations(options.batch);
    const std::string        value(options.value_bytes, pass == 0 ? 'v' : 'w');
    for (size_t begin = 0; begin < options.entries; begin += options.batch) {
        const auto count = std::min(options.batch, options.entries - begin);
        for (size_t item = 0; item < count; ++item) {
            auto id = begin + item;
            if (pass != 0 && options.pattern == "random") {
                id = ((id * 2654435761ULL) + 31) % options.entries;
            }
            if (pass != 0 && options.pattern == "hot") {
                id %= 64;
            }
            keys[item]       = key(id, options);
            const bool erase = pass != 0 && options.pattern == "delete" && id % 2 == 0;
            operations[item] = {.key       = reinterpret_cast<const uint8_t *>(keys[item].data()),
                                .key_len   = keys[item].size(),
                                .value     = reinterpret_cast<const uint8_t *>(value.data()),
                                .value_len = erase ? 0 : value.size(),
                                .kind      = static_cast<uint8_t>(erase ? 1 : 0)};
        }
        const auto start   = Clock::now();
        const auto batches = (options.entries + options.batch - 1) / options.batch;
        check(ct_apply_batch_slices(tree, (pass * batches) + (begin / options.batch) + 1, operations.data(), count),
              "apply");
        latencies->push_back(elapsed(start));
    }
}

void verify(ct_tree *tree, const Options &options, size_t pass, std::vector<uint64_t> *latencies)
{
    for (size_t id = 0; id < options.entries; ++id) {
        const bool        replaced = pass != 0 && (options.pattern != "hot" || id < 64);
        const std::string expected(options.value_bytes, replaced ? 'w' : 'v');
        const auto        name = key(id, options);
        ct_buf            value{};
        int32_t           found = 0;
        uint64_t          slot  = 0;
        const auto        start = Clock::now();
        check(ct_get(tree, reinterpret_cast<const uint8_t *>(name.data()), name.size(), &found, &slot, &value), "get");
        latencies->push_back(elapsed(start));
        const bool present = pass == 0 || options.pattern != "delete" || id % 2 != 0;
        const bool valid   = (found != 0) == present &&
                             (!present || std::string(reinterpret_cast<char *>(value.data), value.len) == expected);
        ct_free_buf(&value);
        if (!valid) {
            throw std::runtime_error("point value mismatch at " + std::to_string(id));
        }
    }
}

void scan(ct_tree *tree, const Options &options, size_t pass)
{
    // Cover the whole keyspace through bounded responses on both engines.
    // A single 64MiB result plus its read window exceeds the runtime budget.
    for (size_t first = 0; first < options.entries; first += 64) {
        const auto last  = std::min(first + 64, options.entries);
        const auto lower = key(first, options);
        const auto upper = key(last, options);
        ct_buf     packed{};
        uint64_t   count     = 0;
        int32_t    truncated = 0;
        check(ct_scan(tree, nullptr, 0, reinterpret_cast<const uint8_t *>(lower.data()), lower.size(), 1, 1,
                      reinterpret_cast<const uint8_t *>(upper.data()), upper.size(), 65, 0, 0, 0, 0, &packed, &count,
                      &truncated),
              "scan");
        ct_free_buf(&packed);
        const auto expected = pass != 0 && options.pattern == "delete" ? (last - first) / 2 : last - first;
        if (count != expected || truncated != 0) {
            throw std::runtime_error("scan count/truncation mismatch");
        }
    }
}

void metrics(ct_tree *tree)
{
#ifdef CROWDB_PROFILE_RUNTIME
    ct_tree_stats_snapshot_v1 state{};
    state.struct_size = sizeof(state);
    state.version     = 1;
    check(ct_get_tree_state_stats(tree, &state), "tree stats");
    ct_tree_flush_dispatch_v1 dispatch{};
    dispatch.struct_size = sizeof(dispatch);
    dispatch.version     = 1;
    check(ct_flush_dispatch_totals(tree, &dispatch), "flush stats");
    std::cout << ",\"allocated_bytes\":" << state.resources.allocated_bytes
              << ",\"retired_bytes\":" << state.resources.retired_bytes << ",\"cow_bytes\":" << dispatch.cow_bytes
              << ",\"prepared_bytes\":" << dispatch.prepared_bytes << ",\"work_counters\":{";
    ct_tree_work_counters_v1 counters{};
    counters.struct_size = sizeof(counters);
    counters.version     = 1;
    check(ct_work_counters_totals(tree, &counters), "work counters");
    for (uint32_t counter = 0; counter < CT_TREE_WORK_COUNTERS_V1; ++counter) {
        uint64_t sum = 0;
        for (auto &value : counters.values) {
            sum += value[counter];
        }
        std::cout << (counter == 0 ? "" : ",") << '"' << ct_work_counter_name_v1(counter) << "\":" << sum;
    }
    std::cout << '}';
    ct_tree_work_snapshot_v1 work{};
    work.struct_size = sizeof(work);
    work.version     = 1;
    check(ct_work_metrics_totals(tree, &work), "work stages");
    std::cout << ",\"stage_totals\":[";
    for (uint32_t stage = 0; stage < CT_TREE_WORK_STAGES_V1; ++stage) {
        const auto &value = work.stages[stage];
        std::cout << (stage == 0 ? "" : ",") << "{\"count\":" << value.total_count
                  << ",\"sum_ns\":" << value.total_sum_ns << ",\"max_ns\":" << value.total_max_ns << '}';
    }
    std::cout << ']';
    const auto begin = Clock::now();
    for (size_t index = 0; index < 10000; ++index) {
        check(ct_get_tree_state_stats(tree, &state), "collect stats");
    }
    std::cout << ",\"stats_10000_ns\":" << elapsed(begin);
    ct_resource_usage_snapshot_v1 resources{};
    resources.struct_size     = sizeof(resources);
    resources.version         = 1;
    const auto resource_begin = Clock::now();
    for (size_t index = 0; index < 10000; ++index) {
        check(ct_tree_resource_usage(tree, &resources), "collect resource usage");
    }
    std::cout << ",\"resource_usage_10000_ns\":" << elapsed(resource_begin);
#else
    (void)tree;
#endif
}
} // namespace

int main(int argc, char **argv)
try {
    if (argc != 9) {
        throw std::runtime_error("args: workers entries value-bytes batch pattern cold trees histogram");
    }
    Options options{.workers     = std::stoull(argv[1]),
                    .entries     = std::stoull(argv[2]),
                    .value_bytes = std::stoull(argv[3]),
                    .batch       = std::stoull(argv[4]),
                    .cold        = std::stoull(argv[6]),
                    .trees       = std::stoull(argv[7]),
                    .histogram   = std::stoull(argv[8]),
                    .pattern     = argv[5]};
    // Diagnostic sweep of the existing source window; this is not a new server setting.
    if (const auto *source_entries = std::getenv("CROWDB_PROFILE_SOURCE_ENTRIES")) {
        options.source_entries = std::stoull(source_entries);
    }
    if (const auto *cache_bytes = std::getenv("CROWDB_PROFILE_CACHE_BYTES")) {
        options.cache_bytes = std::stoull(cache_bytes);
    }
    if (const auto *cache_pages = std::getenv("CROWDB_PROFILE_CACHE_PAGES")) {
        options.cache_pages = std::stoull(cache_pages);
    }
    if (options.entries < 64 || (options.entries & (options.entries - 1)) != 0 || options.batch == 0 ||
        options.batch > 64 || options.trees == 0 || options.workers == 0 || options.source_entries < options.batch ||
        options.cache_bytes < 65536 || options.cache_pages == 0) {
        throw std::runtime_error("entries must be a power of two >=64, batch 1..64, positive trees/workers");
    }
    ct_init_logging("/tmp/crowdb-parallel-flush-profile", "error", 1, 1, "profile");
    auto                  trees = open_trees(options);
    std::vector<uint64_t> apply_latencies;
    std::vector<uint64_t> read_latencies;
    std::vector<uint64_t> scan_latencies;
    uint64_t              apply_ns    = 0;
    uint64_t              flush_ns    = 0;
    uint64_t              snapshot_ns = 0;
    uint64_t              get_ns      = 0;
    uint64_t              scan_ns     = 0;
    for (size_t pass = 0; pass < 2; ++pass) {
        for (auto &tree : trees) {
            auto start = Clock::now();
            write_pass(tree.get(), options, pass, &apply_latencies);
            apply_ns += elapsed(start);
            start = Clock::now();
            check(ct_flush(tree.get()), "flush");
            flush_ns += elapsed(start);
            start                 = Clock::now();
            uint64_t durable_slot = 0;
            check(ct_snapshot(tree.get(), &durable_slot), "snapshot");
            ct_set_gc_watermark(tree.get(), durable_slot, durable_slot);
            snapshot_ns += elapsed(start);
            if (options.cold != 0) {
                ct_evict_clean_leaves(tree.get(), 0);
                ct_evict_clean_inner(tree.get(), 0);
            }
            start = Clock::now();
            verify(tree.get(), options, pass, &read_latencies);
            get_ns += elapsed(start);
            for (size_t repeat = 0; repeat < 32; ++repeat) {
                if (options.cold != 0) {
                    ct_evict_clean_leaves(tree.get(), 0);
                    ct_evict_clean_inner(tree.get(), 0);
                }
                start = Clock::now();
                scan(tree.get(), options, pass);
                const auto duration = elapsed(start);
                scan_ns += duration;
                scan_latencies.push_back(duration);
            }
        }
    }
    std::sort(apply_latencies.begin(), apply_latencies.end());
    std::sort(read_latencies.begin(), read_latencies.end());
    std::sort(scan_latencies.begin(), scan_latencies.end());
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cout << R"({"passed":true,"workers":)" << options.workers << ",\"entries_per_tree\":" << options.entries
              << ",\"value_bytes\":" << options.value_bytes << ",\"batch\":" << options.batch << R"(,"pattern":")"
              << options.pattern << R"(","cold":)" << options.cold << ",\"trees\":" << options.trees
              << ",\"histogram\":" << options.histogram << ",\"source_entries\":" << options.source_entries
              << ",\"cache_bytes\":" << options.cache_bytes << ",\"cache_pages\":" << options.cache_pages
              << ",\"apply_ns\":" << apply_ns << ",\"flush_ns\":" << flush_ns << ",\"snapshot_ns\":" << snapshot_ns
              << ",\"get_ns\":" << get_ns << ",\"scan_ns\":" << scan_ns
              << ",\"batch_p50_ns\":" << apply_latencies[apply_latencies.size() / 2]
              << ",\"batch_p99_ns\":" << apply_latencies[apply_latencies.size() * 99 / 100]
              << ",\"get_p50_ns\":" << read_latencies[read_latencies.size() / 2]
              << ",\"get_p99_ns\":" << read_latencies[read_latencies.size() * 99 / 100]
              << ",\"scan_p50_ns\":" << scan_latencies[scan_latencies.size() / 2]
              << ",\"scan_p99_ns\":" << scan_latencies[scan_latencies.size() * 99 / 100]
              << ",\"peak_rss_kib\":" << usage.ru_maxrss;
    metrics(trees.front().get());
    const auto closing = Clock::now();
    trees.clear();
    std::cout << ",\"close_ns\":" << elapsed(closing) << "}\n";
    ct_shutdown_logging();
    return 0;
}
catch (const std::exception &error) {
    ct_shutdown_logging();
    std::cerr << "parallel flush profile failed: " << error.what() << '\n';
    return 1;
}
