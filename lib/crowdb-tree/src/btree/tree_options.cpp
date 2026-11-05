// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_options.h"

#include "btree/tree_core_geometry.h"
#include "runtime/default_runtime.h"

namespace crowdb::tree
{
Status configure_tree_options(Config *options)
{
    const Config defaults;
    if (options->leaf_split_bytes == defaults.leaf_split_bytes) {
        options->leaf_split_bytes = leaf_record_capacity(options->frame_bytes);
    }
    return configure_default_runtime(options);
}
} // namespace crowdb::tree
