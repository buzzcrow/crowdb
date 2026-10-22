// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/latency_histogram.h"

#include <algorithm>
#include <bit>
#include <numeric>
#include <utility>

namespace crowdb::common::metrics
{

size_t LatencyHistogram::bucket_index(uint64_t ns)
{
    if (ns < 128) {
        return static_cast<size_t>(ns);
    }
    const auto magnitude = static_cast<uint32_t>(std::bit_width(ns) - 1);
    if (magnitude >= 40) {
        return NUM_BUCKETS - 1;
    }
    return 128 + (static_cast<size_t>(magnitude - 7) * 128) + static_cast<size_t>((ns >> (magnitude - 7)) & 127);
}

static uint64_t upper_bound(size_t index)
{
    if (index < 128) {
        return index;
    }
    if (index == LatencyHistogram::NUM_BUCKETS - 1) {
        return UINT64_MAX;
    }
    const size_t offset    = index - 128;
    const auto   magnitude = 7 + static_cast<uint32_t>(offset / 128);
    return (uint64_t{1} << magnitude) + (((offset % 128) + 1) * (uint64_t{1} << (magnitude - 7))) - 1;
}

static uint64_t percentile(const LatencyHistogram::Data &data, uint64_t count, uint64_t permille)
{
    if (count == 0) {
        return 0;
    }
    const uint64_t target     = (count / 1000 * permille) + (((count % 1000 * permille) + 999) / 1000);
    uint64_t       cumulative = 0;
    for (size_t i = 0; i < data.buckets.size(); ++i) {
        cumulative += data.buckets[i];
        if (cumulative >= target) {
            return i == LatencyHistogram::NUM_BUCKETS - 1 ? UINT64_MAX : std::min(upper_bound(i), data.max);
        }
    }
    return 0;
}

void LatencyHistogram::Data::observe(uint64_t ns)
{
    ++buckets[bucket_index(ns)];
    const uint64_t previous = sum_low;
    sum_low += ns;
    sum_high += sum_low < previous ? 1 : 0;
    min = std::min(min, ns);
    max = std::max(max, ns);
}

void LatencyHistogram::Data::merge(const Data &other)
{
    for (size_t i = 0; i < buckets.size(); ++i) {
        buckets[i] += other.buckets[i];
    }
    const uint64_t previous = sum_low;
    sum_low += other.sum_low;
    sum_high += other.sum_high + (sum_low < previous ? 1 : 0);
    min = std::min(min, other.min);
    max = std::max(max, other.max);
}

LatencyHistogram::Snapshot LatencyHistogram::Data::snapshot(uint64_t total_count) const
{
    Snapshot result;
    result.count      = std::accumulate(buckets.begin(), buckets.end(), uint64_t{0});
    result.sum_low    = sum_low;
    result.sum_high   = sum_high;
    result.avg        = result.count == 0
                          ? 0.0
                          : ((static_cast<double>(sum_high) * 18446744073709551616.0) + static_cast<double>(sum_low)) /
                                static_cast<double>(result.count);
    result.min        = result.count == 0 ? 0 : min;
    result.max        = max;
    result.p50        = percentile(*this, result.count, 500);
    result.p90        = percentile(*this, result.count, 900);
    result.p95        = percentile(*this, result.count, 950);
    result.p99        = percentile(*this, result.count, 990);
    result.p999       = percentile(*this, result.count, 999);
    result.zero_count = buckets[0];
    result.overflow_count = buckets.back();
    result.total_count    = std::max(result.count, total_count);
    return result;
}

LatencyHistogram::LatencyHistogram(std::string name) : name_(std::move(name))
{
}

void LatencyHistogram::AtomicData::observe(uint64_t ns)
{
    buckets[bucket_index(ns)].fetch_add(1, std::memory_order_relaxed);
    const uint64_t previous = sum_low.fetch_add(ns, std::memory_order_relaxed);
    if (previous > UINT64_MAX - ns) {
        sum_high.fetch_add(1, std::memory_order_relaxed);
    }
    uint64_t bound = min.load(std::memory_order_relaxed);
    while (ns < bound && !min.compare_exchange_weak(bound, ns, std::memory_order_relaxed)) {
    }
    bound = max.load(std::memory_order_relaxed);
    while (ns > bound && !max.compare_exchange_weak(bound, ns, std::memory_order_relaxed)) {
    }
}

LatencyHistogram::Data LatencyHistogram::AtomicData::capture(bool reset)
{
    if (!reset) {
        return std::as_const(*this).capture();
    }
    Data result;
    for (size_t i = 0; i < buckets.size(); ++i) {
        result.buckets[i] = buckets[i].exchange(0, std::memory_order_relaxed);
    }
    result.sum_low  = sum_low.exchange(0, std::memory_order_relaxed);
    result.sum_high = sum_high.exchange(0, std::memory_order_relaxed);
    result.min      = min.exchange(UINT64_MAX, std::memory_order_relaxed);
    result.max      = max.exchange(0, std::memory_order_relaxed);
    return result;
}

LatencyHistogram::Data LatencyHistogram::AtomicData::capture() const
{
    Data result;
    for (size_t i = 0; i < buckets.size(); ++i) {
        result.buckets[i] = buckets[i].load(std::memory_order_relaxed);
    }
    result.sum_low  = sum_low.load(std::memory_order_relaxed);
    result.sum_high = sum_high.load(std::memory_order_relaxed);
    result.min      = min.load(std::memory_order_relaxed);
    result.max      = max.load(std::memory_order_relaxed);
    return result;
}

void LatencyHistogram::observe(uint64_t ns)
{
    window_.observe(ns);
    total_.observe(ns);
}

LatencyHistogram::Snapshot LatencyHistogram::flush()
{
    const auto cumulative = total_.capture().snapshot().count;
    return window_.capture(true).snapshot(cumulative);
}

LatencyHistogram::Snapshot LatencyHistogram::snapshot() const
{
    return window_.capture().snapshot(total_.capture().snapshot().count);
}

LatencyHistogram::Snapshot LatencyHistogram::snapshot_total() const
{
    return total_.capture().snapshot();
}

LatencyHistogram::Data LatencyHistogram::capture_total() const
{
    return total_.capture();
}

} // namespace crowdb::common::metrics
