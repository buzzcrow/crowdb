// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/tree_work_histograms.h"
#include "crowdb-tree/tree_work_histograms_c_api.h"

namespace crowdb::tree
{
ct_tree_work_histograms_v1 copy_work_histograms(const TreeWorkHistograms &value, uint64_t sequence) noexcept;
} // namespace crowdb::tree
