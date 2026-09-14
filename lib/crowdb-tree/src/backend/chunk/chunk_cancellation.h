// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <atomic>
#include <cstdint>

namespace crowdb::tree::detail
{

struct ChunkCancellation
{
    const std::atomic<uint64_t> *cancelled_id = nullptr;
    uint64_t                     operation_id = 0;

    [[nodiscard]] bool cancelled() const
    {
        return cancelled_id != nullptr && cancelled_id->load(std::memory_order_acquire) == operation_id;
    }
};

} // namespace crowdb::tree::detail
