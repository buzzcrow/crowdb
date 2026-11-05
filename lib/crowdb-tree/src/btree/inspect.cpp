// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_inspection.h"
#include "btree/tree_core.h"

namespace crowdb::tree
{
Status Crowdbtree::inspect_page(const std::vector<uint32_t> &path, uint64_t expected_version, NativeFrame *out,
                                uint64_t *out_version, uint64_t *out_root, uint32_t *out_deltas) const
{
    return inspect_published_page(*core_, path, expected_version, out, out_version, out_root, out_deltas);
}
} // namespace crowdb::tree
