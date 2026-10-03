// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <atomic>
#include <cstdint>
#include <utility>

namespace crowdb::tree
{
// Replacement fence, independent from per-table rotation. Ordinary admission
// uses atomic RMWs; replacement waits without holding source-selection locks.
class GenerationGate
{
  public:
    class Guard
    {
      public:
        explicit Guard(const GenerationGate *gate = nullptr) : gate_(gate)
        {
        }

        Guard(Guard &&other) noexcept : gate_(std::exchange(other.gate_, nullptr))
        {
        }

        Guard(const Guard &)            = delete;
        Guard &operator=(const Guard &) = delete;

        ~Guard()
        {
            if (gate_ != nullptr) {
                const auto previous = gate_->state_.fetch_sub(1, std::memory_order_acq_rel);
                if (previous == kClosed + 1 || previous == kClosed - 1) {
                    gate_->state_.notify_all();
                }
            }
        }

      private:
        const GenerationGate *gate_;
    };

    [[nodiscard]] Guard enter() const
    {
        auto state = state_.load(std::memory_order_acquire);
        for (;;) {
            if ((state & kClosed) != 0 || state == kClosed - 1) {
                state_.wait(state, std::memory_order_acquire);
                state = state_.load(std::memory_order_acquire);
            }
            else if (state_.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel)) {
                return Guard(this);
            }
        }
    }

    void close()
    {
        auto state = state_.fetch_or(kClosed, std::memory_order_acq_rel) | kClosed;
        while (state != kClosed) {
            state_.wait(state, std::memory_order_acquire);
            state = state_.load(std::memory_order_acquire);
        }
    }

    void open()
    {
        state_.store(0, std::memory_order_release);
        state_.notify_all();
    }
#ifdef CROWDB_TREE_TEST_UTIL
    [[nodiscard]] bool closed_for_tests() const
    {
        return (state_.load() & kClosed) != 0;
    }
#endif
    class Replacement
    {
      public:
        explicit Replacement(GenerationGate &gate) : gate_(gate)
        {
            gate_.close();
        }

        ~Replacement()
        {
            gate_.open();
        }

        Replacement(const Replacement &)            = delete;
        Replacement &operator=(const Replacement &) = delete;

      private:
        GenerationGate &gate_;
    };

  private:
    static constexpr uint64_t     kClosed = uint64_t{1} << 63;
    mutable std::atomic<uint64_t> state_{0};
};
} // namespace crowdb::tree
