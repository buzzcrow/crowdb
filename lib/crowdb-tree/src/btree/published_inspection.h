// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/tree_core.h"

namespace crowdb::tree
{
Status inspect_published_page(TreeCore &core, const std::vector<uint32_t> &path, uint64_t expected_version,
                              NativeFrame *out, uint64_t *version, uint64_t *root, uint32_t *deltas);
} // namespace crowdb::tree
