// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/latency_histogram.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

using crowdb::common::metrics::LatencyHistogram;

TEST(LatencyHistogram, SharedNanosecondGoldenVectors)
{
    const auto path =
        std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() / "testdata/latency_histogram.csv";
    std::ifstream input(path);
    ASSERT_TRUE(input.is_open()) << path;
    std::string line;
    size_t      cases = 0;
    while (std::getline(input, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        const auto comma = line.find(',');
        ASSERT_NE(comma, std::string::npos);
        const uint64_t   sample = std::stoull(line.substr(0, comma));
        const uint64_t   upper  = std::stoull(line.substr(comma + 1));
        LatencyHistogram h("test.latency");
        h.observe(sample);
        h.observe(UINT64_MAX);
        const auto s = h.snapshot_total();
        EXPECT_EQ(s.p50, upper) << sample;
        EXPECT_EQ(s.p90, UINT64_MAX);
        EXPECT_EQ(s.min, sample);
        EXPECT_EQ(s.max, UINT64_MAX);
        EXPECT_EQ(s.sum_low, sample - 1);
        EXPECT_EQ(s.sum_high, sample == 0 ? 0 : 1);
        EXPECT_EQ(s.zero_count, sample == 0 ? 1 : 0);
        EXPECT_EQ(s.overflow_count, 1 + (sample >= (uint64_t{1} << 40) ? 1 : 0));
        ++cases;
    }
    EXPECT_EQ(cases, 18);
}

TEST(LatencyHistogram, NearestRankAndExactExtrema)
{
    LatencyHistogram::Data h;
    for (uint64_t ns = 1; ns <= 1000; ++ns) {
        h.observe(ns);
    }
    const auto s = h.snapshot();
    EXPECT_EQ(s.count, 1000);
    EXPECT_EQ(s.sum_low, 500500);
    EXPECT_EQ(s.min, 1);
    EXPECT_EQ(s.max, 1000);
    EXPECT_EQ(s.p50, 501);
    EXPECT_EQ(s.p90, 903);
    EXPECT_EQ(s.p95, 951);
    EXPECT_EQ(s.p99, 991);
    EXPECT_EQ(s.p999, 999);
    LatencyHistogram::Data one;
    one.observe(123);
    EXPECT_EQ(one.snapshot().p50, 123);
}

TEST(LatencyHistogram, WindowResetAndShardMerge)
{
    LatencyHistogram h("test.latency");
    EXPECT_EQ(h.snapshot_total().min, 0);
    h.observe(100);
    EXPECT_EQ(h.flush().sum_low, 100);
    EXPECT_EQ(h.flush().count, 0);
    h.observe(900);
    const auto s = h.snapshot_total();
    EXPECT_EQ(s.count, 2);
    EXPECT_EQ(s.sum_low, 1000);
    EXPECT_EQ(s.min, 100);
    EXPECT_EQ(s.max, 900);
    LatencyHistogram::Data a;
    LatencyHistogram::Data b;
    a.observe(100);
    b.observe(900);
    a.merge(b);
    EXPECT_EQ(a.snapshot().p50, s.p50);
    EXPECT_EQ(a.snapshot().sum_low, s.sum_low);
}

TEST(LatencyHistogram, ConcurrentFinalSnapshot)
{
    LatencyHistogram         h("test.latency");
    std::vector<std::thread> threads;
    threads.reserve(4);
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&h] {
            for (uint64_t ns = 0; ns < 10000; ++ns) {
                h.observe(ns);
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    const auto s = h.snapshot_total();
    EXPECT_EQ(s.count, 40000);
    EXPECT_EQ(s.sum_low, 199980000);
    EXPECT_EQ(s.min, 0);
    EXPECT_EQ(s.max, 9999);
    EXPECT_EQ(s.zero_count, 4);
}
