// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <atomic>
#include <memory>
#include <utility>

namespace crowdb::common
{

#if defined(__cpp_lib_atomic_shared_ptr) && __cpp_lib_atomic_shared_ptr >= 201711L
template <typename T> using AtomicSharedPtr = std::atomic<std::shared_ptr<T>>;
#else
// Older libc++ releases lack the C++20 specialization. Keep their atomic
// snapshot publication through the shared_ptr operations available there.
template <typename T> class AtomicSharedPtr
{
  public:
    AtomicSharedPtr() = default;

    explicit AtomicSharedPtr(std::shared_ptr<T> value) : value_(std::move(value))
    {
    }

    std::shared_ptr<T> load(std::memory_order order = std::memory_order_seq_cst) const
    {
        return std::atomic_load_explicit(&value_, order);
    }

    void store(std::shared_ptr<T> value, std::memory_order order = std::memory_order_seq_cst)
    {
        std::atomic_store_explicit(&value_, std::move(value), order);
    }

    bool compare_exchange_weak(std::shared_ptr<T> &expected, std::shared_ptr<T> desired, std::memory_order success,
                               std::memory_order failure)
    {
        return std::atomic_compare_exchange_weak_explicit(&value_, &expected, std::move(desired), success, failure);
    }

  private:
    std::shared_ptr<T> value_;
};
#endif

} // namespace crowdb::common
