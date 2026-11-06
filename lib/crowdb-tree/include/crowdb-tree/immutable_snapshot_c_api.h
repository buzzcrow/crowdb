// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/c_api.h"

extern "C" {
struct ct_immutable_snapshot;

// Host-thread operations. Capture shares one immutable L1 mapping and exact
// backing owners; it neither opens a writer nor materializes all entries.
// expected_sequence=0 permits an in-memory fixed coverage without a checkpoint.
ct_status ct_immutable_snapshot_capture(ct_tree *tree, uint64_t expected_sequence, uint64_t expected_coverage,
                                        ct_immutable_snapshot **out);
void      ct_immutable_snapshot_free(ct_immutable_snapshot *snapshot);

// Synchronous reads return existing owned result handles. The get future is
// already terminal and follows ct_future_poll/free's kGet ownership contract.
ct_status ct_immutable_snapshot_get(ct_immutable_snapshot *snapshot, const uint8_t *key, size_t size, ct_future **out);
ct_status ct_immutable_snapshot_scan(ct_immutable_snapshot *snapshot, const ct_scan_options_v2 *options,
                                     ct_scan_result **out);

// Rebuilds the destination's configured range from this fixed view and writes
// its own checkpoint. Destination is a separately opened runtime Tree/account.
ct_status ct_immutable_snapshot_rebuild(ct_immutable_snapshot *snapshot, ct_tree *destination,
                                        ct_range_rebuild_stats *stats);
}
