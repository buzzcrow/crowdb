// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace crowdb::tree
{
// Metrics core moved to crowdb-common::metrics (R12); bring the moved types
// into `crowdb-tree` so the test's unqualified `Counter`/`Gauge`/... references
// resolve. `Crowdbtree`/`Options`/`Batch`/`MemPageStore` stay in `crowdb-tree`.
using namespace crowdb::common::metrics;

namespace
{

TEST(MetricsCounter, WindowResetAndTotalAccumulate)
{
    Counter c("test.c");
    c.inc();
    c.inc();
    auto snap = c.flush();
    EXPECT_EQ(snap.count, 2u);
    EXPECT_EQ(snap.total, 2u);

    c.inc();
    snap = c.flush();
    EXPECT_EQ(snap.count, 1u);
    EXPECT_EQ(snap.total, 3u);

    snap = c.flush();
    EXPECT_EQ(snap.count, 0u);
    EXPECT_EQ(snap.total, 3u);
}

TEST(MetricsGauge, ReportsLastValue)
{
    Gauge g("test.g");
    g.set(42);
    EXPECT_EQ(g.get(), 42u);
    g.set(0);
    EXPECT_EQ(g.get(), 0u);
}

TEST(MetricsBandwidth, BasicFlush)
{
    Bandwidth bw("test.bw");
    for (int i = 0; i < 10; ++i) {
        bw.observe(100);
    }
    auto snap = bw.flush();
    EXPECT_EQ(snap.count, 10u);
    EXPECT_EQ(snap.sum, 1000u);
    EXPECT_EQ(snap.total_bytes, 1000u);

    snap = bw.flush();
    EXPECT_EQ(snap.count, 0u);
    EXPECT_EQ(snap.total_bytes, 1000u);
}

TEST(MetricsHistogram, P50P99WithKnownDistribution)
{
    LatencyHistogram h("test.lh");
    for (int i = 0; i < 100; ++i) {
        h.observe(500'000); // 500us
    }
    auto snap = h.flush();
    EXPECT_EQ(snap.count, 100u);
    // HDR bucket upper bound for 500us is 501'760ns (≤0.78% error).
    EXPECT_EQ(snap.p50, 501'760u);
    EXPECT_EQ(snap.p99, 501'760u);
    EXPECT_EQ(snap.max, 501'760u);
    // avg is exact (f64).
    EXPECT_DOUBLE_EQ(snap.avg, 500'000.0);
}

TEST(MetricsHistogram, MixedDistribution)
{
    LatencyHistogram h("test.lh");
    // 80 fast (200us), 20 slow (10ms) — both above LVD (65.5us).
    for (int i = 0; i < 80; ++i) {
        h.observe(200'000);
    }
    for (int i = 0; i < 20; ++i) {
        h.observe(10'000'000);
    }
    auto snap = h.flush();
    EXPECT_EQ(snap.count, 100u);
    // p50 falls in the 200us bucket (upper bound 200'704ns).
    EXPECT_EQ(snap.p50, 200'704u);
    // p99 falls in the 10ms bucket (upper bound 10'027'008ns).
    EXPECT_EQ(snap.p99, 10'027'008u);
    EXPECT_EQ(snap.max, 10'027'008u);
}

TEST(MetricsHistogram, UnderflowAndOverflow)
{
    LatencyHistogram h("test.lh");
    // Below LVD (65.5us) → underflow bucket.
    h.observe(1'000);
    h.observe(65'535);
    // In-range values (majority so p50 falls in-range).
    h.observe(100'000);
    h.observe(100'000);
    h.observe(100'000);
    h.observe(500'000);
    auto snap = h.flush();
    EXPECT_EQ(snap.count, 6u);
    // p50 (target=3) falls in the 100us bucket (upper bound 100'352ns).
    EXPECT_EQ(snap.p50, 100'352u);
    // max falls in the 500us bucket (upper bound 501'760ns).
    EXPECT_EQ(snap.max, 501'760u);
}

TEST(MetricsHistogram, WindowResetsAfterFlush)
{
    LatencyHistogram h("test.lh");
    h.observe(100'000);
    h.observe(200'000);
    auto s1 = h.flush();
    EXPECT_EQ(s1.count, 2u);
    EXPECT_EQ(s1.total_count, 2u);

    auto s2 = h.flush();
    EXPECT_EQ(s2.count, 0u);
    EXPECT_EQ(s2.p50, 0u);
    EXPECT_EQ(s2.total_count, 2u); // total accumulates
}

TEST(MetricsSummary, AvgAndMax)
{
    LatencySummary s("test.ls");
    s.observe(100);
    s.observe(200);
    s.observe(300);
    auto snap = s.flush();
    EXPECT_EQ(snap.count, 3u);
    EXPECT_EQ(snap.sum, 600u);
    EXPECT_EQ(snap.max, 300u);
    EXPECT_EQ(snap.total_count, 3u);

    uint64_t avg = snap.sum / snap.count;
    EXPECT_EQ(avg, 200u);
}

TEST(MetricsSummary, MaxResetsAfterFlush)
{
    LatencySummary s("test.ls2");
    s.observe(500);
    auto snap = s.flush();
    EXPECT_EQ(snap.max, 500u);

    snap = s.flush();
    EXPECT_EQ(snap.max, 0u);
}

TEST(MetricsRegistry, RegisterReturnsUsableHandle)
{
    MetricsRegistry reg;
    auto           *c = reg.register_counter("s.1.test.c");
    ASSERT_NE(c, nullptr);
    c->inc();
    c->inc();

    auto *g = reg.register_gauge("s.1.test.g");
    ASSERT_NE(g, nullptr);
    g->set(99);

    auto *bw = reg.register_bandwidth("s.1.test.bw");
    ASSERT_NE(bw, nullptr);
    bw->observe(42);

    auto *h = reg.register_histogram("s.1.test.lh");
    ASSERT_NE(h, nullptr);
    h->observe(1'000'000);

    auto *s = reg.register_summary("s.1.test.ls");
    ASSERT_NE(s, nullptr);
    s->observe(1'000'000);
}

TEST(MetricsRegistry, FlushFormat)
{
    MetricsRegistry reg;
    auto           *c = reg.register_counter("s.1.kv.delete.c");
    c->inc();
    c->inc();

    auto *g = reg.register_gauge("s.1.g.0.buf.resident.g");
    g->set(512);

    auto *s = reg.register_summary("s.1.kv.scan.l");
    s->observe(1'200'000);
    s->observe(800'000);

    std::string tmp = "/tmp/crowtree_metrics_test_XXXXXX";
    FILE       *fp  = tmpfile();
    ASSERT_NE(fp, nullptr);
    reg.flush_to(fp, 5.0, "2026-07-15T16:30:05.123Z");
    std::fflush(fp);

    // Read back via rewind + fread
    std::rewind(fp);
    char   buf[4096];
    size_t n = std::fread(buf, 1, sizeof(buf) - 1, fp);
    buf[n]   = '\0';
    std::fclose(fp);

    std::string output(buf);
    EXPECT_NE(output.find("metrics\n"), std::string::npos);
    EXPECT_NE(output.find("s.1.kv.delete.c"), std::string::npos);
    EXPECT_NE(output.find("s.1.g.0.buf.resident.g"), std::string::npos);
    EXPECT_NE(output.find("s.1.kv.scan.l"), std::string::npos);
    EXPECT_NE(output.find("512"), std::string::npos);
    // Counter header should appear (counter was inc'd)
    EXPECT_NE(output.find("count"), std::string::npos);
    EXPECT_NE(output.find("tps(/s)"), std::string::npos);
    EXPECT_NE(output.find("total"), std::string::npos);
    // Bandwidth header should be suppressed (no bandwidth registered)
    EXPECT_EQ(output.find("avg_size(KB)"), std::string::npos);
}

TEST(MetricsRegistry, FlushMetricsStrFormat)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;
    Crowdbtree t(opt);
    t.init_metrics("s.0.g.0", "mem");

    // Trigger a snapshot to populate some metrics.
    ASSERT_TRUE(t.apply(1, Batch{{{.key = "k", .kind = OpKind::kPut, .value = "v"}}}).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    std::string out = t.flush_metrics_str(5.0, "2026-07-15T16:30:05.123Z", 0);
    ASSERT_FALSE(out.empty());
    EXPECT_NE(out.find("cpp-tree\n"), std::string::npos);
    // Latency section should use us units.
    EXPECT_NE(out.find("us"), std::string::npos);
    // Bandwidth section should use MB.
    EXPECT_NE(out.find("MB"), std::string::npos);
    // tps column should be present.
    EXPECT_NE(out.find("tps"), std::string::npos);
    // max_name_len should be non-zero after init_metrics.
    EXPECT_GT(t.max_name_len(), 0U);
}

} // namespace
} // namespace crowdb::tree
