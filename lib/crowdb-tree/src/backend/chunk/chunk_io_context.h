// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <chrono>
#include <cstdint>

namespace crowdb::tree::detail
{
// Captured before authority/admission and reused through route lookup, all
// strips, mirrors and retries. Queueing must never create a new request age.
struct ChunkIoContext
{
    uint64_t created_ms = 0;

    static ChunkIoContext capture() noexcept
    {
        const auto now =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        return {.created_ms = now > 0 ? static_cast<uint64_t>(now) : 0};
    }
};
} // namespace crowdb::tree::detail
