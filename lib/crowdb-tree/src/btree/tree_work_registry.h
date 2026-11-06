// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-common/metrics/metrics.h"
#include "tree_metrics.h"

#include <string_view>

namespace crowdb::tree
{
[[nodiscard]] std::shared_ptr<crowdb::common::metrics::MetricsRegistry>
create_tree_work_registry(std::shared_ptr<std::pmr::memory_resource> resource, std::string_view prefix,
                          const std::shared_ptr<TreeWorkMetrics>  &metrics,
                          const std::shared_ptr<AllocationBudget> &metadata);
}
