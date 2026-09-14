// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/latency_histogram.h"

#include <algorithm>
#include <cstdint>
#include <utility>

namespace crowdb::common::metrics
{

// ── HDR bucket parameters ────────────────────────────────────────
//
// HDR-style logarithmic buckets: each power-of-2 magnitude is divided
// into SUB_BUCKET_COUNT equal sub-buckets, giving a bounded relative
// error of <= 1 / SUB_BUCKET_COUNT ≈ 0.78% everywhere.
//
// Configuration tuned for the crowdb latency range:
//   - LVD (lowest discernible value) = 2^UNIT_MAGNITUDE ≈ 65.5us.
//     Values below this go into a single underflow bucket (index 0).
//   - Max magnitude covers 2^(UNIT_MAGNITUDE + NUM_MAGNITUDES) = 2^34
//     ≈ 17.2s. Values above go into the overflow bucket (last index).
//   - 2 significant figures → SUB_BUCKET_COUNT = 128 (2^7).

static constexpr uint32_t UNIT_MAGNITUDE   = 16;  // LVD ≈ 65.5us
static constexpr size_t   SUB_BUCKET_COUNT = 128; // 2^7 → ≤0.78% error
static constexpr uint32_t SUB_BUCKET_BITS  = 7;   // log2(SUB_BUCKET_COUNT)
static constexpr uint64_t SUB_BUCKET_MASK  = 127; // SUB_BUCKET_COUNT - 1
static constexpr size_t   NUM_MAGNITUDES   = 18;  // magnitudes 0..17 → up to 2^34
// 1 underflow + NUM_MAGNITUDES * SUB_BUCKET_COUNT regular + 1 overflow.
static constexpr size_t NUM_BUCKETS = 1 + (NUM_MAGNITUDES * SUB_BUCKET_COUNT) + 1;
static constexpr size_t UNDERFLOW   = 0;
static constexpr size_t OVERFLOW    = NUM_BUCKETS - 1;

// ── HDR index / boundary math ─────────────────────────────────────

// Map a value to its HDR bucket index via O(1) bit manipulation.
// Values < LVD go to the underflow bucket (0); values > max go to
// the overflow bucket (NUM_BUCKETS - 1).
static size_t bucket_index(uint64_t v)
{
    if (v < (1ULL << UNIT_MAGNITUDE)) {
        return UNDERFLOW;
    }
    uint32_t highest_bit = 63 - static_cast<uint32_t>(__builtin_clzll(v));
    uint32_t magnitude   = highest_bit - UNIT_MAGNITUDE;
    if (magnitude >= NUM_MAGNITUDES) {
        return OVERFLOW;
    }
    uint64_t sub_bucket = (v >> (highest_bit - SUB_BUCKET_BITS)) & SUB_BUCKET_MASK;
    return 1 + (static_cast<size_t>(magnitude) * SUB_BUCKET_COUNT) + static_cast<size_t>(sub_bucket);
}

// Upper bound (exclusive) of the bucket at `index`, in nanoseconds.
// The underflow bucket returns the LVD; the overflow bucket returns
// the max trackable value (2^(UNIT_MAGNITUDE + NUM_MAGNITUDES)).
static uint64_t bucket_upper_bound(size_t index)
{
    if (index == UNDERFLOW) {
        return 1ULL << UNIT_MAGNITUDE;
    }
    if (index == OVERFLOW) {
        return 1ULL << (UNIT_MAGNITUDE + static_cast<uint32_t>(NUM_MAGNITUDES));
    }
    size_t   linear           = index - 1;
    size_t   magnitude        = linear / SUB_BUCKET_COUNT;
    size_t   sub_bucket       = linear % SUB_BUCKET_COUNT;
    uint64_t magnitude_base   = 1ULL << (UNIT_MAGNITUDE + static_cast<uint32_t>(magnitude));
    uint64_t sub_bucket_width = 1ULL << (UNIT_MAGNITUDE + static_cast<uint32_t>(magnitude) - SUB_BUCKET_BITS);
    return magnitude_base + ((static_cast<uint64_t>(sub_bucket) + 1) * sub_bucket_width);
}

// Compute the p-th percentile from bucket counts. Returns the upper
// bound of the bucket that contains the p-th percentile value. The
// overflow bucket is capped at the last regular bucket's upper bound
// to avoid reporting the max-trackable sentinel.
static uint64_t percentile(const std::vector<uint64_t> &bucket_counts, uint64_t count, uint64_t p)
{
    if (count == 0) {
        return 0;
    }
    uint64_t target     = count * p / 100;
    uint64_t cumulative = 0;
    for (size_t i = 0; i < bucket_counts.size(); ++i) {
        cumulative += bucket_counts[i];
        if (cumulative >= target) {
            size_t capped = std::min(i, OVERFLOW - 1);
            return bucket_upper_bound(capped);
        }
    }
    return bucket_upper_bound(OVERFLOW - 1);
}

// Find the highest non-empty bucket's upper bound. The overflow
// bucket is capped at the last regular bucket's upper bound.
static uint64_t max_latency(const std::vector<uint64_t> &bucket_counts)
{
    for (size_t i = bucket_counts.size(); i-- > 0;) {
        if (bucket_counts[i] > 0) {
            size_t capped = std::min(i, OVERFLOW - 1);
            return bucket_upper_bound(capped);
        }
    }
    return 0;
}

// ── LatencyHistogram ─────────────────────────────────────────────

LatencyHistogram::LatencyHistogram(std::string name) : name_(std::move(name)), count_(0), sum_(0), total_count_(0)
{
    buckets_.reserve(NUM_BUCKETS);
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        buckets_.push_back(std::make_unique<std::atomic<uint64_t>>(0));
    }
}

void LatencyHistogram::observe(uint64_t ns)
{
    size_t idx = bucket_index(ns);
    buckets_[idx]->fetch_add(1, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(ns, std::memory_order_relaxed);
    total_count_.fetch_add(1, std::memory_order_relaxed);
}

LatencyHistogram::Snapshot LatencyHistogram::flush()
{
    std::vector<uint64_t> bucket_counts(NUM_BUCKETS);
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        bucket_counts[i] = buckets_[i]->exchange(0, std::memory_order_relaxed);
    }
    uint64_t count = count_.exchange(0, std::memory_order_relaxed);
    uint64_t sum   = sum_.exchange(0, std::memory_order_relaxed);
    uint64_t total = total_count_.load(std::memory_order_relaxed);

    Snapshot snap;
    snap.count       = count;
    snap.avg         = count > 0 ? static_cast<double>(sum) / static_cast<double>(count) : 0.0;
    snap.p50         = percentile(bucket_counts, count, 50);
    snap.p99         = percentile(bucket_counts, count, 99);
    snap.max         = max_latency(bucket_counts);
    snap.total_count = total;
    return snap;
}

} // namespace crowdb::common::metrics
