// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-rpc/control_allocator_c_api.h"

#include <utility>

namespace crowdb::rpc
{
// Borrowed immutable callbacks. Every object using them retains the payload
// owner until its own storage and admission have both been returned.
struct ControlAllocator
{
    crowdb_rpc_control_allocator_v1 callbacks{};

    template <typename T, typename... Args> [[nodiscard]] T *create(Args &&...args) const noexcept
    {
        if (callbacks.claim != nullptr && callbacks.claim(callbacks.context, sizeof(T), alignof(T)) == 0) {
            return nullptr;
        }
        T *object = nullptr;
        try {
            object = new T(std::forward<Args>(args)...);
        }
        catch (...) {
            release<T>();
            return nullptr;
        }
        record<T>(true);
        return object;
    }

    template <typename T> void destroy(T *object) const noexcept
    {
        if (object == nullptr) {
            return;
        }
        delete object;
        record<T>(false);
        release<T>();
    }

  private:
    template <typename T> void record(bool acquired) const noexcept
    {
        if (callbacks.record != nullptr) {
            callbacks.record(callbacks.context, sizeof(T), alignof(T), acquired ? 1 : 0);
        }
    }

    template <typename T> void release() const noexcept
    {
        if (callbacks.release != nullptr) {
            callbacks.release(callbacks.context, sizeof(T), alignof(T));
        }
    }
};
} // namespace crowdb::rpc
