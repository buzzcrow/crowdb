// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// HDR-style latency histogram with logarithmic buckets.
//
// Each power-of-2 magnitude is divided into 128 sub-buckets, giving a
// bounded relative error of <= 0.78% everywhere. Values below the
// lowest discernible value (2^16 ≈ 65.5us) go into a single underflow
// bucket; values above the max trackable value (2^34 ≈ 17.2s) go into
// a single overflow bucket. Observation is lock-free and
// allocation-free on the hot path (O(1) bit manipulation + relaxed
// atomic fetch_add).
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace crowdb::common::metrics
{

class LatencyHistogram
{
  public:
    explicit LatencyHistogram(std::string name);

    void observe(uint64_t ns);

    struct Snapshot
    {
        uint64_t count;
        double   avg; // f64 average (ns), exact sum/count
        uint64_t p50; // ns
        uint64_t p99; // ns
        uint64_t max; // ns
        uint64_t total_count;
    };

    Snapshot flush();

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string                                         name_;
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> buckets_;
    std::atomic<uint64_t>                               count_;
    std::atomic<uint64_t>                               sum_;
    std::atomic<uint64_t>                               total_count_;
};

} // namespace crowdb::common::metrics
