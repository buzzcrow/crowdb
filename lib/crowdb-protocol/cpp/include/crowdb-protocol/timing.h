// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>

namespace crowdb::protocol
{

// System-wide stale-write and owner self-fence policy. Deployments tune these
// values as one policy shared by DiskIO and ChunkDB.
inline constexpr uint64_t kDefaultMaxWriteRequestAgeMs = 30'000;
inline constexpr uint64_t kDefaultMaxClockSkewMs       = 1'000;
inline constexpr uint64_t kDefaultSelfFenceMarginMs    = 1'000;

} // namespace crowdb::protocol
