// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <atomic>
#include <cstdint>

namespace crowdb::tree
{
struct FlushDispatchSnapshot
{
    uint64_t inline_probes    = 0;
    uint64_t inline_ranges    = 0;
    uint64_t queued_steps     = 0;
    uint64_t prepared_runs    = 0;
    uint64_t prepared_entries = 0;
    uint64_t prepared_bytes   = 0;
    uint64_t cow_bytes        = 0;
    uint64_t active_steps     = 0;
    uint64_t peak_steps       = 0;
};

// Fixed dimensions, updated once per work step, run, or committed window.
class FlushDispatchMetrics
{
  public:
    void inline_probe() noexcept
    {
        inline_probes_.fetch_add(1, std::memory_order_relaxed);
    }

    void inline_range() noexcept
    {
        inline_ranges_.fetch_add(1, std::memory_order_relaxed);
    }

    void queued_step() noexcept
    {
        queued_steps_.fetch_add(1, std::memory_order_relaxed);
    }

    void run(uint64_t entries, uint64_t bytes) noexcept
    {
        prepared_runs_.fetch_add(1, std::memory_order_relaxed);
        prepared_entries_.fetch_add(entries, std::memory_order_relaxed);
        prepared_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void installed(uint64_t bytes) noexcept
    {
        cow_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void begin() noexcept
    {
        const auto active = active_steps_.fetch_add(1, std::memory_order_relaxed) + 1;
        auto       peak   = peak_steps_.load(std::memory_order_relaxed);
        while (active > peak && !peak_steps_.compare_exchange_weak(peak, active, std::memory_order_relaxed)) {
        }
    }

    void finish() noexcept
    {
        active_steps_.fetch_sub(1, std::memory_order_relaxed);
    }

    [[nodiscard]] FlushDispatchSnapshot snapshot() const noexcept
    {
        return {.inline_probes    = inline_probes_.load(std::memory_order_relaxed),
                .inline_ranges    = inline_ranges_.load(std::memory_order_relaxed),
                .queued_steps     = queued_steps_.load(std::memory_order_relaxed),
                .prepared_runs    = prepared_runs_.load(std::memory_order_relaxed),
                .prepared_entries = prepared_entries_.load(std::memory_order_relaxed),
                .prepared_bytes   = prepared_bytes_.load(std::memory_order_relaxed),
                .cow_bytes        = cow_bytes_.load(std::memory_order_relaxed),
                .active_steps     = active_steps_.load(std::memory_order_relaxed),
                .peak_steps       = peak_steps_.load(std::memory_order_relaxed)};
    }

  private:
    std::atomic<uint64_t> inline_probes_{0}, inline_ranges_{0}, queued_steps_{0};
    std::atomic<uint64_t> prepared_runs_{0}, prepared_entries_{0}, prepared_bytes_{0}, cow_bytes_{0};
    std::atomic<uint64_t> active_steps_{0}, peak_steps_{0};
};
} // namespace crowdb::tree
