// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "cstate_metrics.h"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <iterator>
#include <set>
#include <string>

namespace crowdb::common::metrics::detail
{

static std::optional<int> parse_cpu(std::string_view text)
{
    int  cpu    = 0;
    auto result = std::from_chars(text.data(), text.data() + text.size(), cpu);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() || cpu < 0) {
        return std::nullopt;
    }
    return cpu;
}

std::optional<std::vector<int>> parse_cpu_list(std::string_view text)
{
    auto last = text.find_last_not_of(" \t\r\n");
    text      = last == std::string_view::npos ? std::string_view{} : text.substr(0, last + 1);
    std::set<int> cpus;
    while (!text.empty()) {
        auto comma = text.find(',');
        auto part  = text.substr(0, comma);
        auto dash  = part.find('-');
        auto first = parse_cpu(part.substr(0, dash));
        auto end   = dash == std::string_view::npos ? first : parse_cpu(part.substr(dash + 1));
        if (!first || !end || *end < *first) {
            return std::nullopt;
        }
        for (int cpu = *first;; ++cpu) {
            cpus.insert(cpu);
            if (cpu == *end) {
                break;
            }
        }
        if (comma == std::string_view::npos) {
            return std::vector<int>(cpus.begin(), cpus.end());
        }
        text.remove_prefix(comma + 1);
    }
    return std::nullopt;
}

#ifdef __linux__
std::optional<double> C6Window::sample(std::optional<PerfSample> c6, std::optional<PerfSample> tsc)
{
    auto baseline = std::exchange(previous, std::nullopt);
    if (!c6 || !tsc) {
        return std::nullopt;
    }
    previous = std::pair{*c6, *tsc};
    if (!baseline) {
        return std::nullopt;
    }
    auto c6_delta  = scaled_delta(*c6, baseline->first);
    auto tsc_delta = scaled_delta(*tsc, baseline->second);
    if (!c6_delta || !tsc_delta || *tsc_delta <= 0 || *c6_delta > *tsc_delta * 1.01) {
        return std::nullopt;
    }
    // Separately read PMUs can have a small boundary skew near 100%.
    return std::min(100.0, 100.0 * *c6_delta / *tsc_delta);
}

C6Residency C6Pair::collect()
{
    auto c6_sample  = c6.read();
    auto tsc_sample = tsc.read();
    return {.cpu = cpu, .pct = window.sample(c6_sample, tsc_sample)};
}

static std::optional<std::string> read_pmu_file(std::string_view pmu, std::string_view file)
{
    std::ifstream input("/sys/bus/event_source/devices/" + std::string(pmu) + "/" + std::string(file));
    std::string   text;
    if (!std::getline(input, text)) {
        return std::nullopt;
    }
    return text;
}

static std::optional<uint64_t> read_pmu_number(std::string_view pmu, std::string_view file, bool event)
{
    auto text = read_pmu_file(pmu, file);
    if (!text) {
        return std::nullopt;
    }
    std::string_view number(*text);
    if (event) {
        if (!number.starts_with("event=0x")) {
            return std::nullopt;
        }
        number.remove_prefix(8);
    }
    uint64_t value  = 0;
    auto     result = std::from_chars(number.data(), number.data() + number.size(), value, event ? 16 : 10);
    if (result.ec != std::errc{} || result.ptr != number.data() + number.size()) {
        return std::nullopt;
    }
    return value;
}

std::vector<C6Pair> C6Counters::open_scope(std::string_view pmu)
{
    auto type       = read_pmu_number(pmu, "type", false);
    auto config     = read_pmu_number(pmu, "events/c6-residency", true);
    auto tsc_type   = read_pmu_number("msr", "type", false);
    auto tsc_config = read_pmu_number("msr", "events/tsc", true);
    auto mask       = read_pmu_file(pmu, "cpumask");
    auto cpus       = mask ? parse_cpu_list(*mask) : std::nullopt;
    if (!type || !config || !tsc_type || !tsc_config || !cpus) {
        return {};
    }
    std::vector<C6Pair> pairs;
    for (int cpu : *cpus) {
        int c6_fd = PerfCounter::open_fd(static_cast<uint32_t>(*type), *config, cpu);
        if (c6_fd < 0) {
            return {};
        }
        PerfCounter c6(c6_fd);
        int         tsc_fd = PerfCounter::open_fd(static_cast<uint32_t>(*tsc_type), *tsc_config, cpu);
        if (tsc_fd < 0) {
            return {};
        }
        C6Pair pair{.cpu = cpu, .c6 = std::move(c6), .tsc = PerfCounter(tsc_fd), .window = {}};
        pair.collect(); // Establish both baselines before the first flush.
        pairs.push_back(std::move(pair));
    }
    return pairs;
}

C6Counters::C6Counters() : cores_(open_scope("cstate_core")), packages_(open_scope("cstate_pkg"))
{
}
#else
C6Counters::C6Counters() = default;
#endif

std::pair<std::vector<C6Residency>, std::vector<C6Residency>> C6Counters::collect()
{
    std::vector<C6Residency> cores;
    std::vector<C6Residency> packages;
#ifdef __linux__
    std::transform(cores_.begin(), cores_.end(), std::back_inserter(cores),
                   [](C6Pair &pair) { return pair.collect(); });
    std::transform(packages_.begin(), packages_.end(), std::back_inserter(packages),
                   [](C6Pair &pair) { return pair.collect(); });
#endif
    return {std::move(cores), std::move(packages)};
}

} // namespace crowdb::common::metrics::detail
