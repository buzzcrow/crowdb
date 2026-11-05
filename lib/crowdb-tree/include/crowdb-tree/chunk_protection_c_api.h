// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/chunk_stage_c_api.h"

#ifdef __cplusplus
extern "C" {
#endif

enum { CT_CHUNK_PROTECTION_SCHEMA_V1 = 1 };

enum ct_chunk_candidate_action {
    CT_CHUNK_CANDIDATE_COMMIT  = 1,
    CT_CHUNK_CANDIDATE_ABORT   = 2,
    CT_CHUNK_CANDIDATE_RECOVER = 3
};

struct ct_chunk_candidate_status
{
    ct_chunk_stage_id id;
    uint64_t          generation;
    uint64_t          reference_count;
    uint64_t          prepared_references;
    uint64_t          prepared_windows;
    uint64_t          released_windows;
    uint32_t          state;
};

struct ct_chunk_pin_progress
{
    ct_chunk_stage_id after;
    uint32_t          examined;
    uint32_t          released;
    uint8_t           has_after;
    uint8_t           more;
};

// Calls may block on metadata and run only on the host backend lane. The
// original catalog owns context/free_blob; this table never owns it twice.
struct ct_root_catalog_protection_callbacks
{
    uint32_t version;
    uint32_t struct_bytes;
    uint64_t capabilities;
    ct_status (*check_reader)(void *);
    ct_status (*writer_quiesced)(void *, uint64_t, uint64_t);
    ct_status (*begin)(void *, uint64_t, ct_chunk_stage_id, uint64_t, uint64_t, uint32_t, const uint8_t *, size_t);
    ct_status (*load)(void *, uint64_t, ct_chunk_stage_id, ct_chunk_candidate_status *);
    ct_status (*list)(void *, uint64_t, const ct_chunk_stage_id *, uint32_t, ct_chunk_candidate_status *, size_t *);
    ct_status (*prepare)(void *, uint64_t, ct_chunk_stage_id, uint64_t, const ct_chunk_stage_id *, size_t);
    ct_status (*finish)(void *, uint64_t, ct_chunk_stage_id, uint32_t);
    ct_status (*release)(void *, uint64_t, ct_chunk_stage_id, uint64_t);
    ct_status (*pin)(void *, uint64_t, ct_chunk_stage_id, ct_chunk_stage_id, uint8_t);
    ct_status (*prune)(void *, uint64_t, ct_chunk_stage_id, const ct_chunk_stage_id *, uint32_t,
                       ct_chunk_pin_progress *);
};

ct_status ct_callback_root_catalog_open_v3(const ct_root_catalog_callbacks *, const ct_root_catalog_staging_callbacks *,
                                           const ct_root_catalog_protection_callbacks *, void *, ct_root_catalog **);
uint64_t  ct_root_catalog_protection_capabilities(const ct_root_catalog *);
#ifdef __cplusplus
}
#endif
