// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <atomic>
#include <thread>

namespace crowdb::tree::detail
{
// A one-shot bridge for a host stack waiting on an asynchronous callback.
// Publishing readiness can wake the host before notify_one returns. Keep the
// stack storage alive until the notifier's final access has completed too.
class HostCompletion
{
  public:
    void signal() noexcept
    {
        signal_impl([] {});
    }

    void wait() const noexcept
    {
        ready_.wait(false, std::memory_order_acquire);
        // Only the notification tail remains; it must not access this object
        // after publishing released_. No operation work runs in this interval.
        while (!released_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

#ifdef CROWDB_TREE_TEST_UTIL
    [[nodiscard]] bool completed_for_tests() const noexcept
    {
        return released_.load(std::memory_order_acquire);
    }

    void signal_for_tests(void (*before_notify)(void *), void *context) noexcept
    {
        signal_impl([&] { before_notify(context); });
    }
#endif

  private:
    template <class BeforeNotify> void signal_impl(BeforeNotify before_notify) noexcept
    {
        ready_.store(true, std::memory_order_release);
        before_notify();
        ready_.notify_one();
        released_.store(true, std::memory_order_release);
    }

    std::atomic<bool> ready_{false};
    std::atomic<bool> released_{false};
};
} // namespace crowdb::tree::detail
