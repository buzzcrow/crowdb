// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/runtime_c_api.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ct_chunk_runtime_options
{
    uint32_t version;
    uint32_t struct_bytes;
    uint64_t tree_id;
    uint64_t owner_epoch;
    uint64_t open_generation;
    uint32_t iu_size;
    uint32_t mirror_copies;
    uint64_t max_chunk_bytes;
};

// No storage operation runs here. The paid handle is injected into a Tree
// using the same host runtime. Catalog and transport require staged protection,
// exact fixed-pack I/O and a bound host pin-release service. One catalog owner
// represents one writer incarnation; reopening requires a fresh catalog claim.
ct_status ct_chunk_runtime_page_store_open(ct_runtime *, const ct_chunk_runtime_options *, ct_root_catalog *,
                                           ct_chunk_transport *, ct_page_store **);

struct ct_chunk_collection_result_v1
{
    uint32_t struct_size;
    uint32_t version;
    int32_t  terminal_status;
    uint32_t more;
    uint64_t candidates_examined;
    uint64_t reference_windows;
    uint64_t stages_examined;
    uint64_t pins_examined;
    uint64_t pins_released;
    uint64_t stages_removed;
    uint64_t logical_capacity_bytes;
};

// A finite pass does not advance root retention or WAL durability. A failed
// pass retains partial progress and the exact operation identities for retry.
ct_status ct_chunk_collect_async(ct_tree *, ct_future **out);
ct_status ct_future_poll_chunk_collection(ct_future *, int32_t *done, ct_chunk_collection_result_v1 *out);

#ifdef __cplusplus
}
#endif
