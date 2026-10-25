// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>

namespace crowdb::protocol
{

// Shared write-admission ceilings used by DiskIO and tail recovery.
// A server may enforce shorter windows but cannot extend these bounds.
inline constexpr uint64_t kMaxWriteRequestAgeMs     = 30'000;
inline constexpr uint64_t kMaxWriteClockSkewMs      = 1'000;
inline constexpr uint64_t kDefaultSelfFenceMarginMs = 1'000;

} // namespace crowdb::protocol
