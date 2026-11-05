// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/tree_wait_metrics_c_api.h"

namespace crowdb::tree
{
class TreeWorkMetrics;
int32_t copy_tree_wait_totals(const TreeWorkMetrics *metrics, ct_tree_wait_snapshot_v1 *out) noexcept;
} // namespace crowdb::tree
