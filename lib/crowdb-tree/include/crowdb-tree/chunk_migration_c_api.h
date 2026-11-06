// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/runtime_c_api.h"

#ifdef __cplusplus
extern "C" {
#endif
// Blocking host maintenance entry. Stop and drain all old-format writers in
// this catalog namespace first. The supplied backend must use the runtime
// Chunk factory. Unknown commits retain the original root pin and are resolved
// by retrying the same expected_generation with a fresh catalog writer owner.
ct_status ct_migrate_legacy_chunk_with_runtime(const ct_options *, ct_runtime *, const ct_tree_runtime_options *,
                                               uint64_t expected_generation, ct_tree **);
#ifdef __cplusplus
}
#endif
