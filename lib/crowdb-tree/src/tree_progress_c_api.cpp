// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_internal.h"
#include "crowdb-tree/btree/tree.h"
#include "crowdb-tree/tree_stats_c_api.h"

using namespace crowdb::tree;

int32_t ct_get_tree_progress_stats(ct_tree *tree, ct_tree_progress_snapshot_v1 *out)
try {
    if (tree == nullptr || out == nullptr || out->struct_size != sizeof(*out) || out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    const auto snapshot = c_api_tree(tree)->tree_state_stats();
    if (snapshot.instance_generation == 0) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    *out = {.struct_size                      = sizeof(*out),
            .version                          = 1,
            .instance_generation              = snapshot.instance_generation,
            .commit_seq                       = snapshot.commit_seq,
            .sampled_at_ns                    = snapshot.sampled_at_ns,
            .candidate_oldest_age_ns          = snapshot.progress.candidate_oldest_age_ns,
            .checkpoint_oldest_pending_age_ns = snapshot.progress.checkpoint_oldest_pending_age_ns,
            .maintenance_oldest_age_ns        = snapshot.progress.maintenance_oldest_age_ns};
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}
