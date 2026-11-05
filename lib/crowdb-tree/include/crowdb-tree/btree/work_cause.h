// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <cstdint>

namespace crowdb::tree
{
// Fixed attribution carried across asynchronous I/O. Other is an explicit
// unclassified caller, never inferred from a value or registered dynamically.
enum class TreeWorkCause : uint8_t {
    Other,
    Flush,
    CachePressure,
    Checkpoint,
    GarbageCollection,
    Read,
    Snapshot,
    Allocation,
    Count
};
} // namespace crowdb::tree
