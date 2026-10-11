// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <atomic>
#include <utility>

namespace crowdb::rpc
{
// Publish immutable callbacks while connection workers may invoke an older
// version. Retain versions until the connection and its workers release them.
template <typename Callback> class ConnectionCallback
{
  public:
    ConnectionCallback()                                      = default;
    ConnectionCallback(const ConnectionCallback &)            = delete;
    ConnectionCallback &operator=(const ConnectionCallback &) = delete;

    ~ConnectionCallback()
    {
        auto *node = current_.load(std::memory_order_relaxed);
        while (node != nullptr) {
            auto *previous = node->previous;
            delete node;
            node = previous;
        }
    }

    void set(Callback callback)
    {
        auto *node = new Version{std::move(callback), current_.load(std::memory_order_acquire)};
        while (!current_.compare_exchange_weak(node->previous, node, std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
        }
    }

    [[nodiscard]] const Callback *get() const
    {
        const auto *node = current_.load(std::memory_order_acquire);
        return node == nullptr ? nullptr : &node->callback;
    }

  private:
    struct Version
    {
        Callback callback;
        Version *previous;
    };

    static_assert(std::atomic<Version *>::is_always_lock_free);
    std::atomic<Version *> current_{nullptr};
};
} // namespace crowdb::rpc
