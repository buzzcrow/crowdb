// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/config.h"
#include "crowdb-tree/tuning_c_api.h"

namespace crowdb::tree
{
Status configure_tree_tuning(const ct_tree_tuning *options, Config *out) noexcept;
Status validate_tree_execution(const TreeExecutionOptions &options) noexcept;
} // namespace crowdb::tree
