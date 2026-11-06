// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/backing_identity.h"

#include <atomic>

namespace crowdb::tree::detail
{
uint64_t allocate_backend_identity() noexcept
{
    static std::atomic<uint64_t> next{1};
    auto                         value = next.load(std::memory_order_relaxed);
    do {
        if (value == UINT64_MAX) {
            return 0;
        }
    } while (!next.compare_exchange_weak(value, value + 1, std::memory_order_relaxed));
    return value;
}
} // namespace crowdb::tree::detail
