// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/c_api.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ct_prepared_split_overlay ct_prepared_split_overlay;

// Both v1 structures require version == 1 and struct_size exactly equal to
// sizeof(the v1 structure). Rejected calls leave caller outputs unchanged.
typedef struct ct_split_overlay_limits_v1
{
    uint32_t version;
    uint32_t struct_size;
    uint64_t max_records;
    uint64_t max_bytes;
} ct_split_overlay_limits_v1;

typedef struct ct_installed_split_overlay_v1
{
    uint32_t version;
    uint32_t struct_size;
    uint64_t generation;
    uint64_t frontier;
} ct_installed_split_overlay_v1;

// Background-only preparation. The destination's durable checkpoint is not
// changed. The returned capability exclusively owns its next installation.
ct_status ct_arm_split_overlay(ct_tree *source, ct_tree *destination, const ct_split_overlay_limits_v1 *limits,
                               ct_prepared_split_overlay **out, uint64_t *start_frontier);
// A is the pre-registration write high-watermark, including out-of-order
// slots. Replay the private child to a fixed P >= A without
// changing its durable checkpoint, then complete this background preparation.
ct_status ct_prepared_split_overlay_prepare(ct_prepared_split_overlay *prepared, uint64_t replayed_prefix);

// No allocation, I/O, memtable drain, or executor wait. Repeating the committed
// frontier is idempotent; a different frontier never extends an installed
// token.
ct_status ct_prepared_split_overlay_try_install(ct_prepared_split_overlay *prepared, uint64_t frontier,
                                                ct_installed_split_overlay_v1 *out);

// A nonzero return means this token has committed; out then receives its fixed
// frontier. This query neither advances the token nor pins additional storage.
int ct_prepared_split_overlay_committed_frontier(const ct_prepared_split_overlay *prepared, uint64_t *out);

// Host-only destruction releases old controls and unused admission credit.
// Cancels an uninstalled capability. Installed children retain independent
// owners; releasing this handle does not remove their inherited prefix.
void ct_prepared_split_overlay_destroy(ct_prepared_split_overlay *prepared);

#ifdef __cplusplus
}
#endif
