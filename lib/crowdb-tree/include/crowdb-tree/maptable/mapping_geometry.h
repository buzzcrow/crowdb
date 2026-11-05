// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>

namespace crowdb::tree
{
// Durable local segment geometry. These values are part of the existing
// anchor/segment format and do not depend on an in-memory mapping
// implementation.
inline constexpr uint64_t kMappingSegmentSize = 1024;
inline constexpr uint64_t kMaxMappingSegments = 1U << 16;
} // namespace crowdb::tree
