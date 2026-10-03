// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>

namespace crowdb::tree
{

// Values match the concrete ChunkDB wire purposes.
enum class PagePurpose : uint8_t { kInvalid = 0, kBtreePage = 2, kPageIndex = 3 };

} // namespace crowdb::tree
