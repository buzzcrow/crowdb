// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-common/metrics/system_metrics.h"
#include "perf_counter.h"

#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace crowdb::common::metrics::detail
{

std::optional<std::vector<int>> parse_cpu_list(std::string_view text);

#ifdef __linux__
struct C6Window
{
    std::optional<std::pair<PerfSample, PerfSample>> previous;
    std::optional<double> sample(std::optional<PerfSample> c6, std::optional<PerfSample> tsc);
};

struct C6Pair
{
    int         cpu;
    PerfCounter c6;
    PerfCounter tsc;
    C6Window    window;
    C6Residency collect();
};
#endif

class C6Counters
{
  public:
    C6Counters();
    std::pair<std::vector<C6Residency>, std::vector<C6Residency>> collect();

  private:
#ifdef __linux__
    static std::vector<C6Pair> open_scope(std::string_view pmu);
    std::vector<C6Pair>        cores_;
    std::vector<C6Pair>        packages_;
#endif
};

} // namespace crowdb::common::metrics::detail
