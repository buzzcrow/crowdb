// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/config.h"
#include "crowdb-tree/runtime_c_api.h"

namespace crowdb::tree
{
class AllocationBudget;
// Borrow fixed host lifecycle credit without retaining its CPU executors.
[[nodiscard]] Status runtime_host_allocation_budget(ct_runtime *, std::shared_ptr<AllocationBudget> *) noexcept;

// Copies host ownership (the process host when null) and validates options;
// the core creates one binding.
[[nodiscard]] Status configure_tree_runtime(ct_runtime *runtime, const ct_tree_runtime_options *options,
                                            Config *out) noexcept;
} // namespace crowdb::tree
