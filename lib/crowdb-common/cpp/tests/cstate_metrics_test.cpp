// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/metrics/cstate_metrics.h"

#include <gtest/gtest.h>

using crowdb::common::metrics::detail::parse_cpu_list;

TEST(C6MetricsTest, PmuRepresentativesAreUnique)
{
    EXPECT_EQ(parse_cpu_list("0-3,2,8,10-11\n"), (std::vector<int>{0, 1, 2, 3, 8, 10, 11}));
    for (const char *invalid : {"", "-1", "3-1", "0-", "a", "0,,2", "0,"}) {
        EXPECT_FALSE(parse_cpu_list(invalid)) << invalid;
    }
}

#ifdef __linux__
using crowdb::common::metrics::detail::C6Window;
using crowdb::common::metrics::detail::PerfSample;

static PerfSample sample(uint64_t value, uint64_t enabled, uint64_t running)
{
    return {.value = value, .time_enabled = enabled, .time_running = running};
}

TEST(C6MetricsTest, ZeroIsDistinctFromMissingBaseline)
{
    C6Window window;
    EXPECT_FALSE(window.sample(sample(100, 100, 100), sample(1000, 100, 100)));
    EXPECT_EQ(window.sample(sample(100, 200, 200), sample(2000, 200, 200)), 0.0);
    EXPECT_EQ(window.sample(sample(600, 300, 300), sample(3000, 300, 300)), 50.0);
}

TEST(C6MetricsTest, UsesTscAndCorrectsMultiplexing)
{
    C6Window window;
    window.sample(PerfSample{}, PerfSample{});
    EXPECT_EQ(window.sample(sample(250, 100, 50), sample(1000, 100, 100)), 50.0);
}

TEST(C6MetricsTest, FailedReadRequiresFreshBaselines)
{
    C6Window window;
    window.sample(PerfSample{}, PerfSample{});
    EXPECT_FALSE(window.sample(std::nullopt, sample(1000, 100, 100)));
    EXPECT_FALSE(window.sample(sample(1000, 200, 200), sample(2000, 200, 200)));
    EXPECT_EQ(window.sample(sample(1250, 300, 300), sample(3000, 300, 300)), 25.0);
}

TEST(C6MetricsTest, CounterResetRecovers)
{
    C6Window window;
    window.sample(sample(1000, 100, 100), sample(2000, 100, 100));
    EXPECT_FALSE(window.sample(PerfSample{}, PerfSample{}));
    EXPECT_EQ(window.sample(sample(750, 100, 100), sample(1000, 100, 100)), 75.0);
}

TEST(C6MetricsTest, InvalidIntervalsAreUnavailable)
{
    for (const auto &[c6, tsc] : {
             std::pair{sample(1,    100, 0),   sample(1000, 100, 100)},
             std::pair{sample(1,    50,  100), sample(1000, 100, 100)},
             std::pair{sample(1,    100, 100), sample(0,    100, 100)},
             std::pair{sample(2000, 100, 100), sample(1000, 100, 100)}
    }) {
        C6Window window;
        window.sample(PerfSample{}, PerfSample{});
        EXPECT_FALSE(window.sample(c6, tsc));
    }
}
#endif
