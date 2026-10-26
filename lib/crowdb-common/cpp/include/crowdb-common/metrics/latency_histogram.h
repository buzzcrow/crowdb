// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string>

namespace crowdb::common::metrics
{

// Shared with Rust: exact 0..255 ns, then 128 buckets per power of two
// below 2^40 ns. Overflow quantiles are UINT64_MAX, with an explicit count.
class LatencyHistogram
{
  public:
    struct Snapshot
    {
        uint64_t count          = 0;
        double   avg            = 0;
        uint64_t p50            = 0;
        uint64_t p99            = 0;
        uint64_t max            = 0;
        uint64_t total_count    = 0;
        uint64_t min            = 0;
        uint64_t p90            = 0;
        uint64_t p95            = 0;
        uint64_t p999           = 0;
        uint64_t zero_count     = 0;
        uint64_t overflow_count = 0;
        uint64_t sum_low        = 0;
        uint64_t sum_high       = 0;
    };

    static constexpr size_t NUM_BUCKETS = 128 + (33 * 128) + 1;

    // One polling owner's shard. Observe does not allocate or use atomics.
    struct Data
    {
        std::array<uint64_t, NUM_BUCKETS> buckets{};
        uint64_t                          sum_low  = 0;
        uint64_t                          sum_high = 0;
        uint64_t                          min      = UINT64_MAX;
        uint64_t                          max      = 0;

        void                   observe(uint64_t ns);
        void                   merge(const Data &other);
        [[nodiscard]] Snapshot snapshot(uint64_t total_count = 0) const;
    };

    explicit LatencyHistogram(std::string name);
    void observe(uint64_t ns);

    // Concurrent captures are approximate; join writers before the exact
    // final cumulative snapshot. Window flush never resets cumulative data.
    Snapshot               flush();
    [[nodiscard]] Snapshot snapshot() const;
    [[nodiscard]] Snapshot snapshot_total() const;
    [[nodiscard]] Data     capture_total() const;

    [[nodiscard]] const std::string &name() const
    {
        return name_;
    }

  private:
    struct AtomicData
    {
        std::array<std::atomic<uint64_t>, NUM_BUCKETS> buckets{};
        std::atomic<uint64_t>                          sum_low{0};
        std::atomic<uint64_t>                          sum_high{0};
        std::atomic<uint64_t>                          min{UINT64_MAX};
        std::atomic<uint64_t>                          max{0};

        void               observe(uint64_t ns);
        Data               capture(bool reset);
        [[nodiscard]] Data capture() const;
    };

    static size_t bucket_index(uint64_t ns);
    std::string   name_;
    AtomicData    window_;
    AtomicData    total_;
};

} // namespace crowdb::common::metrics
