// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <utility>

namespace crowdb::diskio
{
inline std::optional<std::chrono::steady_clock::time_point> request_deadline(uint64_t created_ms, uint64_t max_age_ms,
                                                                             uint64_t skew_ms)
{
    const auto steady_now = std::chrono::steady_clock::now();
    const auto now_ms     = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    if (created_ms == 0 || max_age_ms == 0 || (created_ms > now_ms && created_ms - now_ms > skew_ms) ||
        max_age_ms > UINT64_MAX - skew_ms) {
        return std::nullopt;
    }
    const auto allowance = max_age_ms + skew_ms;
    if (created_ms > UINT64_MAX - allowance || created_ms + allowance <= now_ms) {
        return std::nullopt;
    }
    const auto remaining_ms = created_ms + allowance - now_ms;
    const auto max_wait_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::time_point::max() - steady_now)
            .count();
    if (std::cmp_greater(remaining_ms, max_wait_ms)) {
        return std::nullopt;
    }
    return steady_now + std::chrono::milliseconds(static_cast<int64_t>(remaining_ms));
}
} // namespace crowdb::diskio
