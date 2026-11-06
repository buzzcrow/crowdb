// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <cstdint>

namespace crowdb::tree::detail
{
// Zero reports permanent exhaustion; identities never wrap into a live domain.
uint64_t allocate_backend_identity() noexcept;
} // namespace crowdb::tree::detail
