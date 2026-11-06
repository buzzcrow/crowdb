// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/backing_stats_c_api.h"
#include "crowdb-tree/btree/diagnostics.h"

namespace crowdb::tree
{
int32_t copy_backing_stats(BackingStatsSnapshot value, ct_backing_stats_v1 *out) noexcept;
}
