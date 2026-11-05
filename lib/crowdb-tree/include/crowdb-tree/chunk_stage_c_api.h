// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/c_api.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ct_chunk_stage_id
{
    uint64_t owner_epoch;
    uint64_t operation_high;
    uint64_t operation_low;
};

// Independent staging is enabled only when catalog, transport, and reclamation
// all support it. This bit advertises the persistent record schema alone.
enum { CT_CHUNK_STAGE_SCHEMA_V1 = 1 };

struct ct_root_catalog_staging_callbacks
{
    uint32_t version;
    uint32_t struct_bytes;
    uint64_t capabilities;
    ct_status (*load)(void *, uint64_t, ct_chunk_stage_id, const uint8_t **, size_t *);
    ct_status (*compare)(void *, uint64_t, uint64_t, const uint8_t *, size_t, const uint8_t *, size_t);
    ct_status (*list)(void *, uint64_t, const ct_chunk_stage_id *, uint32_t, const uint8_t **, size_t *);
};

ct_status ct_callback_root_catalog_open_v2(const ct_root_catalog_callbacks *, const ct_root_catalog_staging_callbacks *,
                                           void *context, ct_root_catalog **out);
uint64_t  ct_root_catalog_stage_capabilities(const ct_root_catalog *);
ct_status ct_root_catalog_stage_load(ct_root_catalog *, uint64_t, ct_chunk_stage_id, ct_buf *);
ct_status ct_root_catalog_stage_compare(ct_root_catalog *, uint64_t, uint64_t, const uint8_t *, size_t, const uint8_t *,
                                        size_t);
ct_status ct_root_catalog_stage_list(ct_root_catalog *, uint64_t, const ct_chunk_stage_id *, uint32_t, ct_buf *);

#ifdef __cplusplus
}
#endif
