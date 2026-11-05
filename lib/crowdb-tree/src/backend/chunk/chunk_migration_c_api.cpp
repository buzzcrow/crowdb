// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/chunk_migration_c_api.h"

#include "c_api_internal.h"
#include "crowdb-tree/btree/tree.h"

ct_status ct_migrate_legacy_chunk_with_runtime(const ct_options *options, ct_runtime *runtime,
                                               const ct_tree_runtime_options *binding, uint64_t generation,
                                               ct_tree **out)
{
    return ct_migrate_configured(options, runtime, binding, generation, &crowdb::tree::Crowdbtree::migrate_legacy_chunk,
                                 out);
}
